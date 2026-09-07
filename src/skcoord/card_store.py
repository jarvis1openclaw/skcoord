"""Event-sourced Card store (Phase 4): the unified storage substrate.

One work item = one directory ``cards/<id>/`` with an immutable ``core.json``
(birth facts, write-once via O_EXCL) plus append-only per-writer event logs
``events/<agent>@<host>.jsonl``. Current state is folded on read, never stored.
This is the same conflict-free pattern proven in ``itil.py`` (the July-13
refactor), generalized with a ``kind`` discriminator so tasks, epics, and ITIL
tickets share one engine.

Phase 4 ships flag-gated (``SKCOORD_CARD_STORE``); see
docs/superpowers/plans/2026-07-16-cards-storage-cutover-phase4-SHELVED.md.
"""

from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import socket
import stat
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field

from .card import Card, Column, Kind
from .coordination import validate_shared_home

logger = logging.getLogger(__name__)

_TASK_VIEW_PAGE_LIMIT = 200
_TASK_VIEW_SCOPE_MAX_CHARACTERS = 256
_TASK_VIEW_SCOPE_MAX_BYTES = 1024
# Maximum unpadded base64url bytes for the canonical ASCII-escaped cursor with
# every bounded text field filled by worst-case Unicode, plus its HMAC-SHA256.
_TASK_VIEW_CURSOR_MAX_ENCODED_BYTES = 10382
# Process-local by design: restart invalidates every outstanding cursor.
_TASK_VIEW_CURSOR_SECRET = secrets.token_bytes(32)
_HELD_CARD_LOCKS: ContextVar[frozenset[tuple[str, str]]] = ContextVar(
    "skcoord_held_card_locks", default=frozenset()
)
_GOVERNED_CARD_CLASS = re.compile(r"\[(REVIEW|REREVIEW|REPAIR)\]", re.IGNORECASE)
_CARD_PARENT_LABEL_PREFIX = "parent-"
HUMAN_CARD_CREATION_OVERRIDE_LABEL = "human-override"


def _card_lock_key(home: Path, card_id: str) -> tuple[str, str]:
    """Return a process-local reentrancy key for one resolved card namespace."""
    return str(Path(home).expanduser().resolve(strict=False)), card_id


@contextmanager
def _mark_card_lock_held(home: Path, card_id: str):
    """Mark a real acquired lock so nested store appends do not reacquire it."""
    key = _card_lock_key(home, card_id)
    token = _HELD_CARD_LOCKS.set(_HELD_CARD_LOCKS.get() | {key})
    try:
        yield
    finally:
        _HELD_CARD_LOCKS.reset(token)


def validate_card_lock_identifier(card_id: str) -> str:
    """Return a portable card identifier before any lock path is opened."""
    if (
        not isinstance(card_id, str)
        or not card_id
        or card_id in {".", ".."}
        or ".." in card_id
        or "/" in card_id
        or "\\" in card_id
        or "\x00" in card_id
        or any(ord(char) < 32 or ord(char) == 127 for char in card_id)
        or len(card_id) > 128
    ):
        raise ValueError("card lock identifier must be a non-path identifier")
    return card_id


def _open_coordination_child_directory(home: Path, child: str) -> int:
    """Open a coordination child directory without following symlinks.

    The returned descriptor pins the validated directory while a caller opens
    a lock, recovery log, or agent projection below it. Locks coordinate only
    one local filesystem. Cross-host state converges through append-only event
    ordering and mutation preconditions, never through ``flock``.
    """
    if child not in {"agents", "locks", "recovery"}:
        raise ValueError("unsupported coordination child directory")
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise RuntimeError("safe coordination paths require O_NOFOLLOW support")
    root = Path(home).expanduser()
    if root.is_symlink():
        raise ValueError("coordination home must not be a symlink")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | no_follow
    try:
        root_fd = os.open(root, directory_flags)
    except OSError as exc:
        raise ValueError("coordination home is unsafe") from exc

    def open_child(parent_fd: int, name: str) -> int:
        try:
            existing = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            try:
                os.mkdir(name, 0o700, dir_fd=parent_fd)
            except FileExistsError:
                pass
        else:
            if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
                raise ValueError("coordination lock directory must not be a symlink")
        try:
            descriptor = os.open(name, directory_flags, dir_fd=parent_fd)
        except OSError as exc:
            raise ValueError("coordination lock directory is unsafe") from exc
        descriptor_stat = os.fstat(descriptor)
        if not stat.S_ISDIR(descriptor_stat.st_mode):
            os.close(descriptor)
            raise ValueError("coordination lock directory is unsafe")
        return descriptor

    try:
        root_stat = os.fstat(root_fd)
        if not stat.S_ISDIR(root_stat.st_mode):
            raise ValueError("coordination home is unsafe")
        coordination_fd = open_child(root_fd, "coordination")
        try:
            return open_child(coordination_fd, child)
        finally:
            os.close(coordination_fd)
    finally:
        os.close(root_fd)


def _open_lockfile(home: Path, filename: str, label: str):
    """Open one regular, single-link advisory lock without symlink races."""
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        raise ValueError("lock filename must be a non-path identifier")
    locks_fd = _open_coordination_child_directory(home, "locks")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    try:
        try:
            existing = os.stat(filename, dir_fd=locks_fd, follow_symlinks=False)
        except FileNotFoundError:
            existing = None
        if existing is not None and (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
        ):
            raise ValueError(f"{label} lock path must be a regular single-link file")
        try:
            descriptor = os.open(
                filename,
                os.O_CREAT | os.O_RDWR | no_follow,
                0o600,
                dir_fd=locks_fd,
            )
        except OSError as exc:
            raise ValueError(f"{label} lock path is unsafe") from exc
    finally:
        os.close(locks_fd)
    lock_stat = os.fstat(descriptor)
    if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_nlink != 1:
        os.close(descriptor)
        raise ValueError(f"{label} lock path must be a regular single-link file")
    return os.fdopen(descriptor, "a+", encoding="utf-8")


def _open_home_ancestor_lock(home: Path, card_id: str, *, create: bool = True):
    """Open a card lock beside the replaceable CardStore home.

    Artifact-neutral callers pass ``create=False`` so rejected calls cannot
    manufacture a lock path. New cards receive this anchor during creation.
    """
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise RuntimeError("safe coordination paths require O_NOFOLLOW support")
    supplied_root = Path(home).expanduser()
    if supplied_root.is_symlink():
        raise ValueError("CardStore home must not be a symlink")
    root = supplied_root.resolve(strict=False)
    home_digest = hashlib.sha256(os.fsencode(root)).hexdigest()
    card_digest = hashlib.sha256(card_id.encode("utf-8")).hexdigest()
    filename = f".skcoord-{home_digest}-{card_digest}.lock"
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | no_follow
    try:
        parent_fd = os.open(root.parent, directory_flags)
    except OSError as exc:
        raise ValueError("CardStore home ancestor is unsafe") from exc
    descriptor = -1
    try:
        try:
            existing = os.stat(filename, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            existing = None
        if existing is not None and (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
        ):
            raise ValueError("CardStore home ancestor lock must be a regular single-link file")
        open_flags = os.O_RDWR | no_follow
        if create:
            open_flags |= os.O_CREAT
        try:
            descriptor = os.open(filename, open_flags, 0o600, dir_fd=parent_fd)
        except FileNotFoundError:
            if not create:
                return None
            raise
        except OSError as exc:
            raise ValueError("CardStore home ancestor lock is unsafe") from exc
    finally:
        os.close(parent_fd)
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
        os.close(descriptor)
        raise ValueError("CardStore home ancestor lock must be a regular single-link file")
    return os.fdopen(descriptor, "a+", encoding="utf-8")


@contextlib.contextmanager
def _forced_legacy_read():
    """Force KanbanBoard to serve the LEGACY projection inside this block.

    Post Phase-4e the store is the default, so simply unsetting the flag would
    now mean "on". migrate/parity must compare against real legacy, so this
    pins ``SKCOORD_CARD_STORE=0`` for the duration and restores the prior value.
    """
    saved = os.environ.get("SKCOORD_CARD_STORE")
    os.environ["SKCOORD_CARD_STORE"] = "0"
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop("SKCOORD_CARD_STORE", None)
        else:
            os.environ["SKCOORD_CARD_STORE"] = saved


_HOSTNAME = socket.gethostname()

# Column reached by a claim/complete convenience event, to mirror coord.
# coord's claim_task sets current_task, so a claim = in_progress = doing (not ready).
_CLAIM_COLUMN = Column.DOING
_COMPLETE_COLUMN = Column.DONE


def _now_iso() -> str:
    """UTC now as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


class CardCore(BaseModel):
    """Immutable birth-facts of a card (written once to core.json)."""

    id: str
    kind: str = Kind.TASK.value
    title: str
    description: str = ""
    created_by: str = ""
    created_at: str = Field(default_factory=_now_iso)
    acceptance_criteria: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    initial_priority: str = "medium"
    initial_swimlane: str = "feature"
    initial_labels: list[str] = Field(default_factory=list)
    initial_owner: str | None = None
    initial_claim_revision: str | None = None
    meta: dict = Field(default_factory=dict)


# Sanctioned legacy overlay actions (coordination/card_events/*.jsonl) mapped
# onto the store fold's action vocabulary. Anything unmapped is ignored.
_OVERLAY_TO_STORE_ACTION = {
    "move": "move",
    "set_priority": "priority",
    "set_swimlane": "swimlane",
    "add_label": "add_label",
    "remove_label": "remove_label",
    "link": "link",
    "assign": "assign",
    "unassign": "unassign",
    "describe": "describe",
}

_OVERLAY_PAYLOAD_KEYS = (
    "column",
    "order",
    "priority",
    "swimlane",
    "label",
    "link_key",
    "link_value",
    "owner",
    "title",
    "description",
)


def load_legacy_mutations(home: Path) -> dict[str, list[dict]]:
    """Synthesize fold events from the sanctioned legacy append-only paths.

    Two legacy write paths remain live post-cutover (as the hot backup) and can
    carry mutations the store's own logs never saw (flag unset in that process,
    e.g. cron sweeps, or a best-effort mirror failure):

    - ``coordination/archive/<host>.jsonl`` (``Board.archive_task``) becomes an
      ``archive`` event stamped with its ``archived_at`` timestamp.
    - ``coordination/card_events/*.jsonl`` (the kanban overlay) becomes the
      equivalent store action per ``_OVERLAY_TO_STORE_ACTION``.

    Both are per-writer append-only, so merging them into the fold keeps the
    conflict-free invariant: no file is ever rewritten, ordering stays
    ``(ts, writer, seq)``, and a mutation mirrored into BOTH sides simply
    applies twice idempotently.

    Returns:
        dict: card_id -> list of synthetic event dicts (fold-shaped).
    """
    out: dict[str, list[dict]] = {}

    archive_dir = Path(home).expanduser() / "coordination" / "archive"
    if archive_dir.exists():
        for f in sorted(archive_dir.glob("*.jsonl")):
            try:
                lines = f.read_text(encoding="utf-8").splitlines()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping unreadable archive index %s: %s", f.name, exc)
                continue
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
                tid = entry.get("id")
                if not tid:
                    continue
                out.setdefault(tid, []).append(
                    {
                        "ts": entry.get("archived_at", ""),
                        "writer": entry.get("archived_by") or "archive",
                        "seq": 0,
                        "action": "archive",
                        "origin": "legacy-archive",
                    }
                )

    from .card import CardEventLog

    for e in CardEventLog(home).read_all():
        action = _OVERLAY_TO_STORE_ACTION.get(e.action)
        if action is None:
            continue
        ev: dict = {
            "ts": e.ts,
            "writer": e.writer,
            "seq": e.seq,
            "action": action,
            "origin": "legacy-overlay",
        }
        for k in _OVERLAY_PAYLOAD_KEYS:
            v = getattr(e, k, None)
            if v is not None:
                ev[k] = v
        out.setdefault(e.card_id, []).append(ev)
    return out


class CardStore:
    """Event-sourced store for unified work-item cards.

    Args:
        home: Shared skcapstone root (``~/.skcapstone``).
    """

    def __init__(self, home: Path) -> None:
        self.home = validate_shared_home(home)
        self.cards_dir = self.home / "cards"
        # Per-instance cache of legacy mutations (archive index + overlay).
        # Instances are short-lived (one per CLI/MCP call), so a single load
        # keeps list_cards() O(files) instead of rescanning per card.
        self._legacy_cache: Optional[dict[str, list[dict]]] = None

    def ensure_dirs(self) -> None:
        descriptor = self._open_cards_directory()
        os.close(descriptor)

    @staticmethod
    def _open_or_create_directory(parent_fd: int, name: str, label: str) -> int:
        """Open one direct child safely, creating only a real directory."""
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise RuntimeError("safe CardStore paths require O_NOFOLLOW support")
        try:
            existing = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            try:
                os.mkdir(name, 0o700, dir_fd=parent_fd)
            except FileExistsError:
                pass
        else:
            if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
                raise ValueError(f"{label} must be a directory, not a symlink")
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | no_follow,
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise ValueError(f"{label} is unsafe") from exc
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise ValueError(f"{label} is unsafe")
        return descriptor

    def _open_cards_directory(self) -> int:
        """Open the CardStore root and cards directory without link traversal."""
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise RuntimeError("safe CardStore paths require O_NOFOLLOW support")
        self.home.mkdir(parents=True, exist_ok=True)
        if self.home.is_symlink():
            raise ValueError("CardStore home must not be a symlink")
        try:
            home_fd = os.open(
                self.home,
                os.O_RDONLY | os.O_DIRECTORY | no_follow,
            )
        except OSError as exc:
            raise ValueError("CardStore home is unsafe") from exc
        try:
            return self._open_or_create_directory(home_fd, "cards", "CardStore cards directory")
        finally:
            os.close(home_fd)

    def _open_card_directory(self, card_id: str) -> int:
        """Open one validated card directory without following a raced symlink."""
        validate_card_lock_identifier(card_id)
        cards_fd = self._open_cards_directory()
        try:
            return self._open_or_create_directory(cards_fd, card_id, "CardStore card directory")
        finally:
            os.close(cards_fd)

    @staticmethod
    def _open_existing_directory(parent_fd: int, name: str, label: str) -> int | None:
        """Open one existing direct child without link traversal or creation."""
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise RuntimeError("safe CardStore paths require O_NOFOLLOW support")
        try:
            existing = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
            raise ValueError(f"{label} is unsafe")
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | no_follow,
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise ValueError(f"{label} is unsafe") from exc
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise ValueError(f"{label} is unsafe")
        return descriptor

    def _open_existing_cards_directory(self) -> int | None:
        """Open the existing cards root without creating or following it."""
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise RuntimeError("safe CardStore paths require O_NOFOLLOW support")
        if self.home.is_symlink():
            raise ValueError("CardStore home is unsafe")
        try:
            home_fd = os.open(
                self.home,
                os.O_RDONLY | os.O_DIRECTORY | no_follow,
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ValueError("CardStore home is unsafe") from exc
        try:
            return self._open_existing_directory(home_fd, "cards", "CardStore cards directory")
        finally:
            os.close(home_fd)

    def _open_existing_card_directory(self, card_id: str) -> int | None:
        """Open one existing validated card directory without creation."""
        validate_card_lock_identifier(card_id)
        cards_fd = self._open_existing_cards_directory()
        if cards_fd is None:
            return None
        try:
            return self._open_existing_directory(cards_fd, card_id, "CardStore card directory")
        finally:
            os.close(cards_fd)

    @staticmethod
    def _read_regular_file_bytes(parent_fd: int, name: str, label: str) -> bytes | None:
        """Read one existing regular single-link file through a pinned parent."""
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise RuntimeError("safe CardStore paths require O_NOFOLLOW support")
        if not name or "/" in name or "\\" in name or ".." in name:
            raise ValueError(f"{label} is unsafe")
        try:
            existing = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
        ):
            raise ValueError(f"{label} is unsafe")
        try:
            descriptor = os.open(name, os.O_RDONLY | no_follow, dir_fd=parent_fd)
        except OSError as exc:
            raise ValueError(f"{label} is unsafe") from exc
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise ValueError(f"{label} is unsafe")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)
        finally:
            os.close(descriptor)

    def _writer_id(self, agent: str) -> str:
        safe = (agent or "unknown").replace("/", "-").replace("@", "-")
        return f"{safe}@{_HOSTNAME}"

    # ── writes ────────────────────────────────────────────────────────────

    def _ensure_card_lock_anchor(self, card_id: str) -> None:
        """Create the persistent card-ID anchor only for a validated card."""
        core = self._load_core(card_id)
        if core is None or core.get("id") != card_id:
            raise ValueError(f"CardStore card {card_id} has no foldable core")
        filename = f"{hashlib.sha256(card_id.encode('utf-8')).hexdigest()}.lock"
        with _open_lockfile(self.home, filename, "card"):
            pass
        with _open_home_ancestor_lock(self.home, card_id):
            pass

    @staticmethod
    def _creation_class(core: CardCore | dict) -> str | None:
        """Return the governed title class, independent of caller or labels."""
        title = core.get("title", "") if isinstance(core, dict) else core.title
        match = _GOVERNED_CARD_CLASS.search(title if isinstance(title, str) else "")
        creation_class = match.group(1).lower() if match else None
        return "review" if creation_class == "rereview" else creation_class

    @staticmethod
    def _creation_parent(labels: list[str], card_id: str) -> str:
        parents = {
            label[len(_CARD_PARENT_LABEL_PREFIX) :]
            for label in labels
            if isinstance(label, str)
            and label.lower().startswith(_CARD_PARENT_LABEL_PREFIX)
            and label[len(_CARD_PARENT_LABEL_PREFIX) :]
        }
        if len(parents) != 1:
            raise ValueError(
                f"Governed card {card_id} requires exactly one parent-<card_id> label"
            )
        return parents.pop()

    def _is_terminal_card(self, card: Card) -> bool:
        """Use structural CardStore state only, never evidence annotations."""
        if card.status == Column.DONE or card.archived:
            return True
        return any(event.get("action") == "void" for event in self._read_events(card.id))

    def _govern_create(self, core: CardCore) -> None:
        """Fail closed on duplicate or over-depth review and repair creation."""
        creation_class = self._creation_class(core)
        if creation_class is None:
            return
        labels = list(core.initial_labels)
        if HUMAN_CARD_CREATION_OVERRIDE_LABEL in {label.lower() for label in labels}:
            return
        parent_id = self._creation_parent(labels, core.id)

        cards = self.list_cards(include_archived=True)
        cards_by_id = {card.id: card for card in cards}
        if parent_id not in cards_by_id:
            raise ValueError(f"Governed card {core.id} names unknown parent {parent_id}")

        for existing in cards:
            existing_core = self._load_core(existing.id) or {}
            if self._creation_class(existing_core) != creation_class:
                continue
            try:
                existing_parent = self._creation_parent(
                    list(existing_core.get("initial_labels", [])), existing.id
                )
            except ValueError:
                continue
            if existing_parent == parent_id and not self._is_terminal_card(existing):
                raise ValueError(
                    f"Refusing live {creation_class} duplicate for parent {parent_id}; "
                    f"existing card {existing.id} is non-terminal"
                )

        if creation_class != "review":
            return
        review_depth = 0
        root_id = parent_id
        visited = {core.id}
        while True:
            if root_id in visited:
                raise ValueError(f"Review ancestry for {core.id} contains a cycle at {root_id}")
            visited.add(root_id)
            ancestor = cards_by_id.get(root_id)
            if ancestor is None:
                raise ValueError(f"Review ancestry for {core.id} names unknown card {root_id}")
            ancestor_core = self._load_core(root_id) or {}
            if self._creation_class(ancestor_core) != "review":
                break
            review_depth += 1
            root_id = self._creation_parent(
                list(ancestor_core.get("initial_labels", [])), ancestor.id
            )

        if review_depth >= 2:
            raise ValueError(
                f"Refusing third review level for root {root_id}; human escalation is required"
            )

    def create(self, core: CardCore) -> str:
        """Govern and write ``cards/<id>/core.json`` exactly once.

        All callers, including coordination CLI and MCP adapters, converge here.
        The creation lock makes the scan and O_EXCL write one local transaction;
        the immutable core remains the final collision boundary for identical IDs.
        """
        validate_card_lock_identifier(core.id)
        if self._load_core(core.id) is not None:
            return core.id
        with _open_lockfile(self.home, "card-creation-governor.lock", "card creation") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                if self._load_core(core.id) is not None:
                    return core.id
                self._govern_create(core)
                rec_fd = self._open_card_directory(core.id)
                payload = (core.model_dump_json(indent=2) + "\n").encode("utf-8")
                fd = -1
                try:
                    try:
                        fd = os.open(
                            "core.json",
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                            0o644,
                            dir_fd=rec_fd,
                        )
                    except FileExistsError:
                        existing = os.stat("core.json", dir_fd=rec_fd, follow_symlinks=False)
                        if (
                            stat.S_ISLNK(existing.st_mode)
                            or not stat.S_ISREG(existing.st_mode)
                            or existing.st_nlink != 1
                        ):
                            raise ValueError("CardStore core destination is unsafe")
                        return core.id
                    offset = 0
                    while offset < len(payload):
                        offset += os.write(fd, payload[offset:])
                    os.fsync(fd)
                finally:
                    if fd >= 0:
                        os.close(fd)
                    os.fsync(rec_fd)
                    os.close(rec_fd)
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        self._ensure_card_lock_anchor(core.id)
        return core.id

    def append_event(self, card_id: str, action: str, agent: str, **payload: Any) -> dict:
        """Append one event line under the common same-card lock protocol.

        ``transition_id`` is an optional deterministic caller token. Repeating
        it returns the already-durable event instead of appending another line,
        which lets a caller safely classify a write-then-error as success.
        A lock already held by this context is recognized internally to avoid
        recursively reacquiring ``flock`` through an ordinary higher-level
        mutation protocol.
        """
        validate_card_lock_identifier(card_id)
        if _card_lock_key(self.home, card_id) not in _HELD_CARD_LOCKS.get():
            with card_mutation_lock(self.home, card_id):
                return self.append_event(card_id, action, agent, **payload)
        self._require_foldable_core(card_id)
        if action in {
            "move",
            "reopen",
            "assign",
            "unassign",
            "claim",
            "release_claim",
            "complete",
        } and any(event.get("action") == "void" for event in self._read_events(card_id)):
            raise ValueError(f"CardStore card {card_id} is voided; void is a terminal decision")
        writer_filename = f"{self._writer_id(agent)}.jsonl"
        rec_fd = self._open_card_directory(card_id)
        try:
            events_fd = self._open_or_create_directory(
                rec_fd, "events", "CardStore event directory"
            )
        finally:
            os.close(rec_fd)
        descriptor = -1
        try:
            try:
                existing = os.stat(writer_filename, dir_fd=events_fd, follow_symlinks=False)
            except FileNotFoundError:
                existing = None
            if existing is not None and (
                stat.S_ISLNK(existing.st_mode)
                or not stat.S_ISREG(existing.st_mode)
                or existing.st_nlink != 1
            ):
                raise ValueError("CardStore event destination is unsafe")
            try:
                descriptor = os.open(
                    writer_filename,
                    os.O_APPEND | os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=events_fd,
                )
            except OSError as exc:
                raise ValueError("CardStore event destination is unsafe") from exc
            event_stat = os.fstat(descriptor)
            if not stat.S_ISREG(event_stat.st_mode) or event_stat.st_nlink != 1:
                raise ValueError("CardStore event destination is unsafe")
            with os.fdopen(descriptor, "a+", encoding="utf-8") as fh:
                descriptor = -1
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                try:
                    fh.seek(0)
                    lines = list(fh)
                    transition_id = payload.get("transition_id")
                    if isinstance(transition_id, str) and transition_id:
                        for line in lines:
                            try:
                                existing_event = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if existing_event.get("transition_id") == transition_id:
                                return existing_event
                    seq = len(lines)
                    prev_hash = ""
                    if lines:
                        prev_hash = hashlib.sha256(lines[-1].strip().encode("utf-8")).hexdigest()
                    event = {
                        "event_id": uuid.uuid4().hex,
                        "ts": _now_iso(),
                        "writer": agent,
                        "node": _HOSTNAME,
                        "seq": seq,
                        "action": action,
                        "prev_hash": prev_hash,
                    }
                    event.update(payload)
                    fh.seek(0, os.SEEK_END)
                    fh.write(json.dumps(event, default=str) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
                    return event
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(events_fd)

    def _require_foldable_core(self, card_id: str) -> None:
        """Reject an event target before creating an orphan directory."""
        card = self.fold(card_id)
        if card is None or card.id != card_id:
            raise ValueError(f"CardStore card {card_id} has no foldable core")

    def has_transition(self, card_id: str, transition_id: str) -> bool:
        """Return whether an exact intended CardStore event is durable."""
        return any(
            event.get("transition_id") == transition_id for event in self._read_events(card_id)
        )

    # ── reads ─────────────────────────────────────────────────────────────

    def _load_core(self, card_id: str) -> Optional[dict]:
        card_fd = self._open_existing_card_directory(card_id)
        if card_fd is None:
            return None
        try:
            try:
                raw = self._read_regular_file_bytes(card_fd, "core.json", "CardStore core")
            except ValueError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"CardStore core for {card_id} is unreadable") from exc
            if raw is None:
                return None
            try:
                core = json.loads(raw.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"CardStore core for {card_id} is malformed") from exc
            if not isinstance(core, dict):
                raise ValueError(f"CardStore core for {card_id} must be a JSON object")
            return core
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, ValueError):
                raise
            raise ValueError(f"CardStore core for {card_id} is unreadable") from exc
        finally:
            os.close(card_fd)

    def _read_events(self, card_id: str) -> list[dict]:
        out: list[dict] = []
        card_fd = self._open_existing_card_directory(card_id)
        if card_fd is None:
            return out
        try:
            events_fd = self._open_existing_directory(
                card_fd, "events", "CardStore event directory"
            )
        finally:
            os.close(card_fd)
        if events_fd is None:
            return out
        try:
            for name in sorted(os.listdir(events_fd)):
                if not name.endswith(".jsonl"):
                    continue
                try:
                    raw = self._read_regular_file_bytes(events_fd, name, "CardStore event source")
                except ValueError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise ValueError(
                        f"CardStore event source for {card_id} is unreadable"
                    ) from exc
                if raw is None:
                    continue
                try:
                    lines = raw.decode("utf-8").splitlines()
                except UnicodeError as exc:
                    raise ValueError(f"CardStore event source for {card_id} is malformed") from exc
                prev_line_hash = ""
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"CardStore event source for {card_id} is malformed"
                        ) from exc
                    if not isinstance(event, dict):
                        raise ValueError(
                            f"CardStore event source for {card_id} must contain JSON objects"
                        )
                    # Hash-chain verification: each chained event must link
                    # to the hash of the preceding line in this writer file.
                    # Legacy events without prev_hash pass (chain starts at
                    # the first chained event).
                    event_prev = event.get("prev_hash")
                    if isinstance(event_prev, str) and event_prev:
                        if event_prev != prev_line_hash:
                            raise ValueError(
                                f"CardStore event chain broken in {card_id}/{name}: "
                                f"event {event.get('event_id', '?')} prev_hash mismatch"
                            )
                    out.append(event)
                    prev_line_hash = hashlib.sha256(line.encode("utf-8")).hexdigest()
        finally:
            os.close(events_fd)
        # Deterministic order: ts, then writer, then per-writer seq.
        out.sort(key=lambda e: (e.get("ts", ""), e.get("writer", ""), e.get("seq", 0)))
        return out

    def _legacy_events(self, card_id: str) -> list[dict]:
        """Legacy mutations (archive index + overlay) for one card, cached."""
        if self._legacy_cache is None:
            try:
                self._legacy_cache = load_legacy_mutations(self.home)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Legacy mutation load failed: %s", exc)
                self._legacy_cache = {}
        return self._legacy_cache.get(card_id, [])

    def fold(self, card_id: str) -> Optional[Card]:
        """Fold core + events into the current ``Card`` state.

        The event stream is the union of this card's own store logs AND the
        sanctioned legacy paths (archive index + card_events overlay), merged
        in ``(ts, writer, seq)`` order. That is the fold-drift fix (card
        ba4af853): a mutation that only reached a legacy file (mirror off or
        failed) still folds into the served state, so ``coord status`` cannot
        overcount open cards post-cutover.
        """
        core = self._load_core(card_id)
        if core is None:
            return None
        try:
            kind = Kind(core.get("kind", "task"))
        except ValueError:
            kind = Kind.TASK
        card = Card(
            id=core["id"],
            kind=kind,
            title=core.get("title", ""),
            description=core.get("description", ""),
            status=Column.BACKLOG,
            swimlane=core.get("initial_swimlane", "feature"),
            priority=core.get("initial_priority", "medium"),
            originator=core.get("created_by", ""),
            owner=core.get("initial_owner"),
            labels=list(core.get("initial_labels", [])),
            acceptance_criteria=list(core.get("acceptance_criteria", []) or []),
            dependencies=list(core.get("dependencies", [])),
            meta=dict(core.get("meta", {})),
            created_at=core.get("created_at", ""),
            source="cards",
        )
        initial_revision = core.get("initial_claim_revision")
        if card.owner is not None:
            if not isinstance(initial_revision, str) or not initial_revision:
                raise ValueError(f"CardStore card {card_id} has an owner without a claim revision")
            card.status = _CLAIM_COLUMN
            card.meta["_claim_revision"] = initial_revision
        events = self._read_events(card_id)
        legacy_events = self._legacy_events(card_id)
        if legacy_events:
            events = events + legacy_events
            events.sort(key=lambda e: (e.get("ts", ""), e.get("writer", ""), e.get("seq", 0)))
        voided = False
        void_terminal_actions = {
            "move",
            "reopen",
            "assign",
            "unassign",
            "claim",
            "release_claim",
            "complete",
        }
        for e in events:
            action = e.get("action")
            if voided and action in void_terminal_actions:
                card.updated_at = e.get("ts", card.updated_at)
                continue
            if action == "void":
                voided = True
                card.archived = True
                card.owner = None
                card.meta.pop("_claim_revision", None)
                card.meta["voided"] = True
                card.meta["voided_at"] = e.get("ts")
                card.meta["voided_by"] = e.get("writer")
            elif action == "move":
                col = e.get("column")
                if col in {c.value for c in Column}:
                    card.status = Column(col)
                if e.get("order") is not None:
                    card.order = e["order"]
            elif action == "assign":
                card.owner = e.get("owner")
                card.meta.pop("_claim_revision", None)
            elif action == "unassign":
                card.owner = None
                card.meta.pop("_claim_revision", None)
            elif action == "release_claim":
                released_owner = e.get("released_owner")
                expected_revision = e.get("expected_claim_revision")
                actual_revision = card.meta.get("_claim_revision")
                valid_release = (
                    isinstance(released_owner, str)
                    and bool(released_owner)
                    and isinstance(expected_revision, str)
                    and bool(expected_revision)
                )
                releases_current = (
                    valid_release
                    and card.owner == released_owner
                    and actual_revision == expected_revision
                )
                conflicts = card.meta.get("claim_conflicts", [])
                matching_conflicts = [
                    conflict
                    for conflict in conflicts
                    if isinstance(conflict, dict)
                    and conflict.get("owner") == released_owner
                    and conflict.get("claim_revision") == expected_revision
                ]
                releases_conflict = (
                    valid_release
                    and len(matching_conflicts) == 1
                    and isinstance(actual_revision, str)
                    and bool(actual_revision)
                    and card.owner == matching_conflicts[0].get("existing_owner")
                    and actual_revision == matching_conflicts[0].get("existing_claim_revision")
                )
                if releases_current:
                    card.owner = None
                    card.status = Column.BACKLOG
                    card.meta.pop("_claim_revision", None)
                elif releases_conflict:
                    remaining = [
                        conflict for conflict in conflicts if conflict is not matching_conflicts[0]
                    ]
                    if remaining:
                        card.meta["claim_conflicts"] = remaining
                    else:
                        card.meta.pop("claim_conflicts", None)
                else:
                    card.meta.setdefault("release_conflicts", []).append(
                        {
                            "event_id": e.get("event_id"),
                            "reason": "claim precondition did not match",
                            "released_owner": released_owner,
                            "expected_claim_revision": expected_revision,
                            "actual_owner": card.owner,
                            "actual_claim_revision": actual_revision,
                        }
                    )
            elif action == "claim":
                owner = e.get("owner")
                revision = e.get("claim_revision") or e.get("event_id")
                was_unowned_backlog = card.owner is None and card.status == Column.BACKLOG
                if (
                    isinstance(owner, str)
                    and owner
                    and card.owner
                    and card.owner != owner
                    and card.status in {Column.READY, Column.DOING, Column.REVIEW}
                ):
                    card.meta.setdefault("claim_conflicts", []).append(
                        {
                            "event_id": e.get("event_id"),
                            "owner": owner,
                            "claim_revision": e.get("claim_revision"),
                            "existing_owner": card.owner,
                            "existing_claim_revision": card.meta.get("_claim_revision"),
                            "reason": "concurrent claim requires explicit release or completion",
                        }
                    )
                else:
                    card.owner = owner
                    card.status = _CLAIM_COLUMN
                    if isinstance(revision, str) and revision:
                        card.meta["_claim_revision"] = revision
                    else:
                        card.meta.pop("_claim_revision", None)
                    if was_unowned_backlog and owner:
                        card.meta.pop("claim_conflicts", None)
            elif action == "complete":
                card.status = _COMPLETE_COLUMN
                # coord drops a completed task from claimed_tasks, so its derived
                # claimed_by is None. Match that so parity holds on done cards.
                card.owner = None
                card.meta.pop("_claim_revision", None)
            elif action == "priority" and e.get("priority"):
                card.priority = e["priority"]
            elif action == "swimlane" and e.get("swimlane"):
                card.swimlane = e["swimlane"]
            elif action == "add_label" and e.get("label") and e["label"] not in card.labels:
                card.labels.append(e["label"])
            elif action == "remove_label" and e.get("label") in card.labels:
                card.labels.remove(e["label"])
            elif action == "link" and e.get("link_key") is not None:
                card.links[e["link_key"]] = e.get("link_value")
            elif action == "describe":
                # SPE P3.1: title/description are folded, not frozen. Only the
                # keys actually present are applied, so an empty string is a
                # deliberate clear while an omitted key leaves the field alone.
                if e.get("title") is not None:
                    card.title = e["title"]
                if e.get("description") is not None:
                    card.description = e["description"]
            elif action == "amend_criteria":
                criteria = e.get("criteria")
                if (
                    not isinstance(criteria, list)
                    or not criteria
                    or any(not isinstance(value, str) or not value.strip() for value in criteria)
                ):
                    raise ValueError(f"CardStore criteria amendment for {card_id} is malformed")
                card.acceptance_criteria = list(criteria)
            elif action == "add_dependency" and isinstance(e.get("dependency"), str):
                dependency = e["dependency"]
                if dependency and dependency not in card.dependencies:
                    card.dependencies.append(dependency)
            elif action == "remove_dependency" and isinstance(e.get("dependency"), str):
                dependency = e["dependency"]
                if dependency in card.dependencies:
                    card.dependencies.remove(dependency)
            elif action == "note" and e.get("text"):
                card.meta.setdefault("comments", []).append(
                    {"ts": e.get("ts"), "writer": e.get("writer"), "text": e["text"]}
                )
            elif action == "agent_run_request":
                card.meta["agent_run"] = {
                    "run_id": e.get("run_id"),
                    "state": "queued",
                    "instruction": e.get("instruction", ""),
                    "agent": e.get("run_agent"),
                    "mode": e.get("mode", "propose"),
                    "kind": e.get("kind"),
                    "requester": e.get("writer"),
                    "created_at": e.get("ts"),
                    "activity": [],
                    "attempts": 0,
                    "links": {},
                }
            elif action == "agent_run_claim":
                r = card.meta.get("agent_run")
                if r and r.get("run_id") == e.get("run_id"):
                    r["state"] = "running"
                    r["worker"] = e.get("worker")
                    r["lease_expires"] = e.get("lease_expires")
                    r["attempts"] = r.get("attempts", 0) + 1
            elif action == "agent_run_activity":
                r = card.meta.get("agent_run")
                if r and r.get("run_id") == e.get("run_id"):
                    r.setdefault("activity", []).append(
                        {
                            "ts": e.get("ts"),
                            "atype": e.get("atype"),
                            "text": e.get("text"),
                            "writer": e.get("writer"),
                        }
                    )
            elif action == "agent_run_state":
                r = card.meta.get("agent_run")
                if r and r.get("run_id") == e.get("run_id"):
                    r["state"] = e.get("state", r.get("state"))
                    if e.get("last_error"):
                        r["last_error"] = e.get("last_error")
                    for k in ("pr", "commit", "branch", "transcript"):
                        if e.get(k):
                            r.setdefault("links", {})[k] = e.get(k)
            elif action == "archive":
                card.archived = True
                card.meta["archived_at"] = e.get("ts")
                card.meta["archived_by"] = e.get("writer")
            elif action == "reopen":
                card.archived = False
                col = e.get("column")
                if col in {c.value for c in Column}:
                    card.status = Column(col)
            card.updated_at = e.get("ts", card.updated_at)
        return card

    def list_card_ids(self) -> list[str]:
        cards_fd = self._open_existing_cards_directory()
        if cards_fd is None:
            return []
        card_ids: list[str] = []
        try:
            for name in sorted(os.listdir(cards_fd)):
                try:
                    entry = os.stat(name, dir_fd=cards_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(entry.st_mode):
                    raise ValueError("CardStore card entry is unsafe")
                if not stat.S_ISDIR(entry.st_mode):
                    continue
                validate_card_lock_identifier(name)
                card_fd = self._open_existing_directory(cards_fd, name, "CardStore card directory")
                if card_fd is None:
                    continue
                try:
                    if (
                        self._read_regular_file_bytes(card_fd, "core.json", "CardStore core")
                        is not None
                    ):
                        card_ids.append(name)
                finally:
                    os.close(card_fd)
        finally:
            os.close(cards_fd)
        return card_ids

    @staticmethod
    def _unreadable_card(card_id: str, error: Exception) -> Card:
        """Project one failed fold without weakening that card's read boundary.

        The underlying fold still fails closed. Only the multi-card aggregation
        boundary converts that failure into an explicit record, so one corrupt
        stream cannot erase its card or prevent healthy cards from being read.
        """
        source = f"cards/{card_id}"
        reason = str(error).strip() or error.__class__.__name__
        return Card(
            id=card_id,
            kind=Kind.TASK,
            title=f"UNREADABLE [source: {source}; reason: {reason}]",
            description=f"Source: {source}\nReason: {reason}",
            status=Column.BACKLOG,
            swimlane="bug",
            priority="critical",
            labels=["unreadable"],
            meta={"unreadable": True, "source": source, "reason": reason},
            source="cards",
        )

    def list_cards(
        self, include_archived: bool = False, *, degrade_unreadable: bool = False
    ) -> list[Card]:
        """Fold every card, optionally degrading failed folds explicitly.

        ``fold`` and ``_read_events`` remain strict and raise on malformed
        input. Human-facing multi-card board callers opt into the one-card
        fault boundary. Governance callers such as parity and export retain
        strict all-or-nothing reads by leaving ``degrade_unreadable`` false.
        """
        out: list[Card] = []
        for cid in self.list_card_ids():
            try:
                card = self.fold(cid)
            except Exception as exc:  # noqa: BLE001 - one-card fault boundary
                if not degrade_unreadable:
                    raise
                logger.error("CardStore card %s is unreadable: %s", cid, exc)
                card = self._unreadable_card(cid, exc)
            if card is None:
                continue
            if card.archived and not include_archived:
                continue
            out.append(card)
        return out


# ---------------------------------------------------------------------------
# Importer + parity (Phase 4b / 4c)
# ---------------------------------------------------------------------------


def import_from_legacy(home: Path, dry_run: bool = False) -> dict:
    """Import the live legacy board (coord + ITIL + overlay) into the CardStore.

    Idempotent: a card whose ``core.json`` already exists is skipped, so a
    re-run is a no-op. Reproduces each card's column, owner, and archived state
    by emitting create + move + assign + archive events.

    Returns:
        dict: ``{"imported": n, "skipped": m, "total": t}``.
    """
    from .card import KanbanBoard

    store = CardStore(home)
    # Force the LEGACY projection even post-cutover, otherwise KanbanBoard would
    # serve the store back to us and every legacy-only card would look "already
    # present" (i.e. migrate could never import it).
    with _forced_legacy_read():
        legacy = KanbanBoard(home).cards(include_archived=True)
    imported = 0
    skipped = 0
    for c in legacy:
        if store._load_core(c.id) is not None:
            skipped += 1
            continue
        if dry_run:
            imported += 1
            continue
        store.create(
            CardCore(
                id=c.id,
                kind=c.kind.value,
                title=c.title,
                description=c.description,
                created_by=c.originator,
                created_at=c.created_at or _now_iso(),
                acceptance_criteria=list(c.acceptance_criteria),
                dependencies=list(c.dependencies),
                initial_priority=c.priority,
                initial_swimlane=c.swimlane,
                initial_labels=list(c.labels),
                meta=dict(c.meta),
            )
        )
        writer = c.originator or "import"
        store.append_event(c.id, "move", writer, column=c.status.value, order=c.order)
        if c.owner:
            store.append_event(c.id, "assign", writer, owner=c.owner)
        if c.archived:
            store.append_event(c.id, "archive", writer)
        imported += 1
    return {"imported": imported, "skipped": skipped, "total": len(legacy)}


# Phase 4e: the CardStore is the DEFAULT store. Only an explicit disable token
# turns it back off (the instant rollback escape hatch). ``dual`` keeps writing
# both stores while still serving reads from legacy (used during a bake).
_CARD_STORE_DISABLED = {"0", "off", "false", "no"}


def _card_store_flag() -> str:
    return (os.environ.get("SKCOORD_CARD_STORE") or "").strip().lower()


def card_store_write_enabled() -> bool:
    """True when coord writes should mirror into the CardStore.

    Phase 4e default-ON: writes mirror into the store unless explicitly disabled
    with ``SKCOORD_CARD_STORE`` in {0, off, false, no}.
    """
    return _card_store_flag() not in _CARD_STORE_DISABLED


def card_store_read_enabled() -> bool:
    """True when reads should be served from the CardStore.

    Phase 4e default-ON: served from the store unless explicitly disabled, or
    unless in ``dual`` mode (write-both, read-legacy) used during a bake.
    """
    flag = _card_store_flag()
    return flag not in _CARD_STORE_DISABLED and flag != "dual"


# Reverse of card._STATUS_TO_COLUMN, to reconstruct coord TaskViews from cards.
_COLUMN_TO_STATUS = {
    "backlog": "open",
    "ready": "claimed",
    "doing": "in_progress",
    "review": "review",
    "done": "done",
}


def _task_view_from_card(card):
    """Reconstruct one coord ``TaskView`` from a folded CardStore card."""
    from .coordination import Task, TaskPriority, TaskStatus, TaskView

    try:
        priority = TaskPriority(card.priority)
    except ValueError:
        priority = TaskPriority.MEDIUM
    meta = dict(card.meta)
    # ponytail: fold updated_at into meta so archive-done can age by completion
    # without widening the Task schema; created_at remains the birth fact.
    if card.updated_at:
        meta.setdefault("_board_updated_at", card.updated_at)
    task = Task(
        id=card.id,
        title=card.title,
        description=card.description,
        priority=priority,
        tags=list(card.labels),
        created_by=card.originator,
        created_at=card.created_at,
        acceptance_criteria=list(card.acceptance_criteria),
        dependencies=list(card.dependencies),
        meta=meta,
    )
    status = TaskStatus(_COLUMN_TO_STATUS.get(card.status.value, "open"))
    return TaskView(task=task, status=status, claimed_by=card.owner)


def _task_view_cursor(payload: dict) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    signature = hmac.digest(_TASK_VIEW_CURSOR_SECRET, body, "sha256")
    cursor = base64.urlsafe_b64encode(body + signature).decode().rstrip("=")
    if len(cursor) > _TASK_VIEW_CURSOR_MAX_ENCODED_BYTES:
        raise ValueError("task-view cursor exceeds its encoded-size contract")
    return cursor


def _task_view_cursor_position(
    cursor: str | None,
    *,
    scope: str,
    limit: int,
    include_archived: bool,
) -> tuple[str | None, str | None]:
    if cursor is None:
        return None, None
    if (
        not isinstance(cursor, str)
        or not cursor
        or len(cursor) > _TASK_VIEW_CURSOR_MAX_ENCODED_BYTES
        or not cursor.isascii()
    ):
        raise ValueError("task-view cursor is malformed")
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        body, signature = raw[:-32], raw[-32:]
        expected = hmac.digest(_TASK_VIEW_CURSOR_SECRET, body, "sha256")
        if len(signature) != 32 or not hmac.compare_digest(signature, expected):
            raise ValueError
        payload = json.loads(body)
        if (
            not isinstance(payload, dict)
            or set(payload) != {"after", "archived", "limit", "population", "scope", "v"}
            or payload["v"] != 2
            or payload["scope"] != scope
            or payload["limit"] != limit
            or payload["archived"] is not include_archived
            or not isinstance(payload["after"], str)
            or not payload["after"]
            or len(payload["after"]) > 128
            or not isinstance(payload["population"], str)
            or not payload["population"]
            or len(payload["population"]) > 256
        ):
            raise ValueError
        validate_card_lock_identifier(payload["after"])
        return payload["after"], payload["population"]
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("task-view cursor is malformed, stale, or out of scope") from exc


def task_view_page_from_store(
    home: Path,
    scope,
    *,
    limit: int,
    cursor: str | None = None,
    include_archived: bool = False,
):
    """Fold only one authorized ``limit + 1`` task-view page.

    The owner provides a bounded keyset result and its exact population-state
    identity. A missing, non-task, archived, mismatched, reordered, or changed
    result fails closed rather than skipping forward and creating a gap.
    """
    from .coordination import TaskViewPage, TaskViewReadBatch, TaskViewReadScope

    if not isinstance(scope, TaskViewReadScope):
        raise TypeError("task-view scope must be owner-authorized before paging")
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= _TASK_VIEW_PAGE_LIMIT
    ):
        raise ValueError(f"task-view limit must be between 1 and {_TASK_VIEW_PAGE_LIMIT}")
    after, expected_population = _task_view_cursor_position(
        cursor,
        scope=scope.authorization_scope,
        limit=limit,
        include_archived=include_archived,
    )
    try:
        batch = scope.read_page(after, limit + 1)
    except Exception as exc:  # noqa: BLE001 - authorization owner failures fail closed
        raise ValueError("task-view authorization population is unavailable") from exc
    if not isinstance(batch, TaskViewReadBatch):
        raise TypeError("task-view owner reader returned an invalid batch")
    selected_ids = batch.card_ids
    if not isinstance(selected_ids, (tuple, list)) or len(selected_ids) > limit + 1:
        raise ValueError("task-view owner reader exceeded the bounded request")
    if (
        not isinstance(batch.population_state, str)
        or not batch.population_state
        or len(batch.population_state) > 256
        or (expected_population is not None and batch.population_state != expected_population)
    ):
        raise ValueError("task-view cursor population is stale")

    store = CardStore(home)
    cards = []
    previous = after
    for card_id in selected_ids:
        validate_card_lock_identifier(card_id)
        if previous is not None and card_id <= previous:
            raise ValueError("task-view owner reader returned an unstable order")
        try:
            card = store.fold(card_id)
        except Exception as exc:  # noqa: BLE001 - one-card fault boundary
            logger.error("CardStore card %s is unreadable: %s", card_id, exc)
            card = store._unreadable_card(card_id, exc)
        if (
            card is None
            or card.id != card_id
            or card.kind.value not in ("task", "epic")
            or (card.archived and not include_archived)
        ):
            raise ValueError("task-view cursor population is stale")
        cards.append(card)
        previous = card_id
    has_more = len(cards) > limit
    items = tuple(_task_view_from_card(card) for card in cards[:limit])
    next_cursor = None
    if has_more:
        next_cursor = _task_view_cursor(
            {
                "after": cards[limit - 1].id,
                "archived": include_archived,
                "limit": limit,
                "population": batch.population_state,
                "scope": scope.authorization_scope,
                "v": 2,
            }
        )
    return TaskViewPage(
        items=items,
        population_state=batch.population_state,
        next_cursor=next_cursor,
        has_more=has_more,
        eligible_records_touched=len(selected_ids),
    )


def task_views_from_store(home: Path, include_archived: bool = False) -> list:
    """Reconstruct coord ``TaskView`` objects from the CardStore.

    Used by ``Board.get_task_views`` when reads are cut over
    (``SKCOORD_CARD_STORE=1``), so the dashboard, ``coord status``, and claim
    validation all serve from the event-sourced store while legacy keeps being
    written as a hot backup.
    """
    store = CardStore(home)
    return [
        _task_view_from_card(card)
        for card in store.list_cards(include_archived=include_archived, degrade_unreadable=True)
        # get_task_views is the COORD task board: coord-origin kinds only.
        # ITIL cards (incident/problem/change) live in the kanban view, not here.
        if card.kind.value in ("task", "epic")
    ]


def _card_exists(home: Path, card_id: str) -> bool:
    """Return whether a card is known to either compatible board projection."""
    store = CardStore(home)
    if store._load_core(card_id) is not None:
        return True
    from .coordination import Board

    return any(task.id == card_id for task in Board(home).load_tasks(include_archived=True))


def current_dependencies(
    home: Path, card_id: str, birth_dependencies: Optional[list[str]] = None
) -> list[str]:
    """Return dependencies after applying the append-only card-event fold.

    Args:
        home: Shared SKCapstone root.
        card_id: Card whose effective dependencies are requested.
        birth_dependencies: Immutable task-file dependencies used when a legacy
            card has not yet been mirrored into the CardStore.

    Returns:
        The ordered, de-duplicated effective dependency identifiers.
    """
    card = CardStore(home).fold(card_id)
    if card is not None:
        return list(card.dependencies)
    return list(dict.fromkeys(birth_dependencies or []))


def current_acceptance_criteria(
    home: Path,
    card_id: str,
    birth_criteria: Optional[list[str]] = None,
    store: Optional[CardStore] = None,
) -> list[str]:
    """Return acceptance criteria after the append-only CardStore fold.

    A task with no CardStore directory is a legitimate legacy-only task and
    retains its immutable birth criteria. A known CardStore directory without
    a readable core is indeterminate and fails closed rather than presenting
    stale governance requirements.

    Args:
        home: Shared SKCapstone root.
        card_id: Card whose effective acceptance criteria are requested.
        birth_criteria: Immutable task-file criteria for a legacy-only task.
        store: Short-lived shared fold reader for one board projection.

    Returns:
        The latest valid folded criteria, or legacy birth criteria when the
        task has never been mirrored into CardStore.

    Raises:
        ValueError: If a known CardStore card has missing or invalid state.
    """
    reader = store or CardStore(home)
    card = reader.fold(card_id)
    if card is not None:
        return list(card.acceptance_criteria)
    card_fd = reader._open_existing_card_directory(card_id)
    if card_fd is None:
        return list(birth_criteria or [])
    os.close(card_fd)
    raise ValueError(f"CardStore core for {card_id} is missing")


def amend_dependency(
    home: Path,
    card_id: str,
    dependency_id: str,
    action: str,
    agent: str = "",
    reason: str = "",
) -> bool:
    """Append one idempotent dependency amendment to a known card.

    ``add_dependency`` and ``remove_dependency`` modify only the folded card
    projection. The original task and ``core.json`` remain untouched. Repeating
    an already-effective operation appends no event, making normal retries a
    no-op while the retained event provides provenance and rollback context.

    Args:
        home: Shared SKCapstone root.
        card_id: Existing downstream card to amend.
        dependency_id: Existing gate card to add or remove.
        action: ``add_dependency`` or ``remove_dependency``.
        agent: Attributed writer.
        reason: Non-empty governance reason retained in the event.

    Returns:
        ``True`` if an event was appended, otherwise ``False`` for an
        idempotent no-op.

    Raises:
        ValueError: If identifiers are invalid, unknown, self-referential, or
            the reason/action is invalid.
    """
    home = Path(home).expanduser()
    if action not in {"add_dependency", "remove_dependency"}:
        raise ValueError("action must be add_dependency or remove_dependency")
    if not card_id or not dependency_id:
        raise ValueError("card and dependency identifiers are required")
    if card_id == dependency_id:
        raise ValueError("a card cannot depend on itself")
    if not reason.strip():
        raise ValueError("a dependency amendment reason is required")
    if not _card_exists(home, card_id):
        raise ValueError(f"card {card_id} not found")
    if not _card_exists(home, dependency_id):
        raise ValueError(f"dependency {dependency_id} not found")

    with card_mutation_lock(home, card_id):
        dependencies = current_dependencies(home, card_id)
        present = dependency_id in dependencies
        if (action == "add_dependency" and present) or (
            action == "remove_dependency" and not present
        ):
            return False
        CardStore(home).append_event(
            card_id,
            action,
            agent or "coord",
            dependency=dependency_id,
            reason=reason.strip(),
        )
        return True


def _open_existing_coordination_lock(home: Path, filename: str, label: str):
    """Open an existing persistent lock without creating any path component."""
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise RuntimeError("safe coordination paths require O_NOFOLLOW support")
    root = Path(home).expanduser()
    if root.is_symlink():
        raise ValueError("coordination home must not be a symlink")
    flags = os.O_RDONLY | os.O_DIRECTORY | no_follow
    try:
        root_fd = os.open(root, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ValueError("coordination home is unsafe") from exc
    coordination_fd = locks_fd = descriptor = -1
    try:
        coordination_fd = CardStore._open_existing_directory(
            root_fd, "coordination", "coordination lock directory"
        )
        if coordination_fd is None:
            return None
        locks_fd = CardStore._open_existing_directory(
            coordination_fd, "locks", "coordination lock directory"
        )
        if locks_fd is None:
            return None
        try:
            existing = os.stat(filename, dir_fd=locks_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if (
            stat.S_ISLNK(existing.st_mode)
            or not stat.S_ISREG(existing.st_mode)
            or existing.st_nlink != 1
        ):
            raise ValueError(f"{label} lock path must be a regular single-link file")
        try:
            descriptor = os.open(filename, os.O_RDWR | no_follow, dir_fd=locks_fd)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ValueError(f"{label} lock path is unsafe") from exc
    finally:
        if locks_fd is not None and locks_fd >= 0:
            os.close(locks_fd)
        if coordination_fd is not None and coordination_fd >= 0:
            os.close(coordination_fd)
        os.close(root_fd)
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or (opened.st_dev, opened.st_ino) != (existing.st_dev, existing.st_ino)
    ):
        os.close(descriptor)
        raise ValueError(f"{label} lock path must be a regular single-link file")
    return os.fdopen(descriptor, "a+", encoding="utf-8")


def _open_existing_card_lock(home: Path, card_id: str):
    """Open a validated card directory as its artifact-neutral lock anchor.

    A directory descriptor survives atomic replacement of ``core.json`` and is
    therefore common to every same-card mutation while the card namespace is
    stable.  The core is validated through that descriptor before it is
    returned; malformed, missing, symlinked, hardlinked, or ID-mismatched cores
    fail without creating any filesystem node.
    """
    store = CardStore(home)
    card_fd = store._open_existing_card_directory(card_id)
    if card_fd is None:
        raise ValueError(f"CardStore card {card_id} has no foldable core")
    try:
        raw = store._read_regular_file_bytes(card_fd, "core.json", "CardStore core")
        if raw is None:
            raise ValueError(f"CardStore card {card_id} has no foldable core")
        try:
            core = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"CardStore core for {card_id} is malformed") from exc
        if not isinstance(core, dict) or core.get("id") != card_id:
            raise ValueError(f"CardStore core for {card_id} does not match its card")
        return card_fd
    except Exception:
        os.close(card_fd)
        raise


def _acquire_card_lock(descriptor: int, card_id: str, deadline: float) -> None:
    """Acquire one already-open card lock before a shared deadline."""
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out acquiring card lock for {card_id}")
            time.sleep(0.01)


@contextmanager
def card_mutation_lock(
    home: Path,
    card_id: str,
    timeout_seconds: float = 5.0,
    *,
    artifact_neutral: bool = False,
):
    """Acquire the common bounded lock for every existing-card mutation.

    The validated card directory, not replaceable ``core.json``, is the common
    card-ID anchor.  It is opened without creation, locked first, and validated
    again after acquisition.  Ordinary callers may additionally retain the
    persistent hashed lock for compatibility.  Artifact-neutral callers never
    create it or any parent, preserving the complete tree on invalid input.
    """
    card_id = validate_card_lock_identifier(card_id)
    key = _card_lock_key(home, card_id)
    if key in _HELD_CARD_LOCKS.get():
        yield
        return
    deadline = time.monotonic() + timeout_seconds
    try:
        anchor_handle = _open_existing_card_lock(home, card_id)
    except ValueError:
        if artifact_neutral or card_store_write_enabled():
            raise
        # Explicit legacy-only rollback mode has no CardStore card directory.
        filename = f"{hashlib.sha256(card_id.encode('utf-8')).hexdigest()}.lock"
        with _open_lockfile(home, filename, "card") as handle:
            _acquire_card_lock(handle.fileno(), card_id, deadline)
            try:
                with _mark_card_lock_held(home, card_id):
                    yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return

    try:
        ancestor_handle = _open_home_ancestor_lock(home, card_id, create=not artifact_neutral)
        if ancestor_handle is None:
            raise ValueError(f"CardStore card {card_id} has no stable home lock anchor")
        with ancestor_handle:
            _acquire_card_lock(ancestor_handle.fileno(), card_id, deadline)
            try:
                _acquire_card_lock(anchor_handle, card_id, deadline)
                try:
                    current_handle = _open_existing_card_lock(home, card_id)
                    try:
                        anchor = os.fstat(anchor_handle)
                        current = os.fstat(current_handle)
                        if (anchor.st_dev, anchor.st_ino) != (
                            current.st_dev,
                            current.st_ino,
                        ):
                            raise ValueError(
                                "CardStore card lock anchor changed while acquiring it"
                            )
                    finally:
                        os.close(current_handle)

                    filename = f"{hashlib.sha256(card_id.encode('utf-8')).hexdigest()}.lock"
                    if artifact_neutral:
                        lock_handle = _open_existing_coordination_lock(home, filename, "card")
                        if lock_handle is None:
                            raise ValueError(f"CardStore card {card_id} has no stable lock anchor")
                    else:
                        lock_handle = _open_lockfile(home, filename, "card")
                    with lock_handle:
                        _acquire_card_lock(lock_handle.fileno(), card_id, deadline)
                        try:
                            with _mark_card_lock_held(home, card_id):
                                yield
                        finally:
                            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                finally:
                    fcntl.flock(anchor_handle, fcntl.LOCK_UN)
            finally:
                fcntl.flock(ancestor_handle.fileno(), fcntl.LOCK_UN)
    finally:
        os.close(anchor_handle)


def add_dependency(
    home: Path, card_id: str, dependency_id: str, agent: str = "", reason: str = ""
) -> bool:
    """Append an idempotent dependency addition for a coordination card."""
    return amend_dependency(home, card_id, dependency_id, "add_dependency", agent, reason)


def remove_dependency(
    home: Path, card_id: str, dependency_id: str, agent: str = "", reason: str = ""
) -> bool:
    """Append an idempotent dependency removal for a coordination card."""
    return amend_dependency(home, card_id, dependency_id, "remove_dependency", agent, reason)


def mirror_coord_create(home: Path, task) -> None:
    """Mirror a coord Task creation into the CardStore (best-effort)."""
    from .card import _swimlane_for_tags

    tags_lower = {t.lower() for t in task.tags}
    kind = "epic" if "epic" in tags_lower else "task"
    CardStore(home).create(
        CardCore(
            id=task.id,
            kind=kind,
            title=task.title,
            description=task.description,
            created_by=task.created_by,
            created_at=task.created_at,
            acceptance_criteria=list(getattr(task, "acceptance_criteria", []) or []),
            dependencies=list(task.dependencies),
            initial_priority=task.priority.value,
            initial_swimlane=_swimlane_for_tags(task.tags),
            initial_labels=list(task.tags),
            meta=dict(task.meta),
        )
    )


def mirror_coord_create_claimed(
    home: Path,
    task,
    owner: str,
    claim_revision: str = "",
) -> str:
    """Create a card whose first observable fold is already claimed."""
    from .card import _swimlane_for_tags

    tags_lower = {t.lower() for t in task.tags}
    kind = "epic" if "epic" in tags_lower else "task"
    store = CardStore(home)
    existing = store._load_core(task.id)
    if existing is not None:
        stored_revision = existing.get("initial_claim_revision")
        if not isinstance(stored_revision, str) or not stored_revision:
            raise ValueError(f"CardStore create-and-claim conflict for {task.id}")
        revision = claim_revision or stored_revision
    else:
        revision = claim_revision or uuid.uuid4().hex
    expected = CardCore(
        id=task.id,
        kind=kind,
        title=task.title,
        description=task.description,
        created_by=task.created_by,
        created_at=task.created_at,
        acceptance_criteria=list(getattr(task, "acceptance_criteria", []) or []),
        dependencies=list(task.dependencies),
        initial_priority=task.priority.value,
        initial_swimlane=_swimlane_for_tags(task.tags),
        initial_labels=list(task.tags),
        initial_owner=owner,
        initial_claim_revision=revision,
        meta=dict(task.meta),
    )
    if existing is not None:
        if CardCore.model_validate(existing).model_dump() != expected.model_dump():
            raise ValueError(f"CardStore create-and-claim conflict for {task.id}")
        current = store.fold(task.id)
        if (
            current is None
            or current.owner != owner
            or current.status != _CLAIM_COLUMN
            or current.meta.get("_claim_revision") != revision
        ):
            raise ValueError(
                f"CardStore create-and-claim claim is no longer current for {task.id}"
            )
        return revision

    store.create(expected)
    created = store._load_core(task.id)
    if created is None or CardCore.model_validate(created).model_dump() != expected.model_dump():
        raise ValueError(f"CardStore create-and-claim conflict for {task.id}")
    return revision


def mirror_coord_claim(
    home: Path,
    task_id: str,
    agent: str,
    transition_id: str = "",
    claim_revision: str = "",
) -> str:
    """Mirror a coord claim into the CardStore."""
    revision = claim_revision or uuid.uuid4().hex
    CardStore(home).append_event(
        task_id,
        "claim",
        agent,
        owner=agent,
        claim_revision=revision,
        transition_id=transition_id or uuid.uuid4().hex,
    )
    return revision


def mirror_coord_complete(home: Path, task_id: str, agent: str, transition_id: str = "") -> None:
    """Mirror a coord completion into the CardStore."""
    CardStore(home).append_event(
        task_id, "complete", agent, transition_id=transition_id or uuid.uuid4().hex
    )


def current_claim_precondition(home: Path, task_id: str, owner: str) -> str | None:
    """Return the exact claim revision, or a safe retry no-op marker."""
    card = CardStore(home).fold(task_id)
    if card is None:
        raise ValueError(f"CardStore card {task_id} not found")
    if card.owner == owner:
        revision = card.meta.get("_claim_revision")
        if isinstance(revision, str) and revision:
            return revision
        raise ValueError(f"CardStore claim on {task_id} has no revision")
    conflicts = [
        conflict
        for conflict in card.meta.get("claim_conflicts", [])
        if isinstance(conflict, dict) and conflict.get("owner") == owner
    ]
    if len(conflicts) > 1:
        raise ValueError(f"CardStore claim conflict for {task_id} owned by {owner} is ambiguous")
    if conflicts:
        conflict = conflicts[0]
        revision = conflict.get("claim_revision")
        authoritative_revision = conflict.get("existing_claim_revision")
        if (
            isinstance(revision, str)
            and revision
            and isinstance(authoritative_revision, str)
            and authoritative_revision
            and conflict.get("existing_owner") == card.owner
            and authoritative_revision == card.meta.get("_claim_revision")
        ):
            return revision
        raise ValueError(f"CardStore claim conflict on {task_id} has no exact revision")
    if card.owner is None and card.status == Column.BACKLOG:
        for event in reversed(CardStore(home)._read_events(task_id)):
            if event.get("action") == "release_claim" and event.get("released_owner") == owner:
                return None
    raise ValueError(f"CardStore owner conflict for {task_id}: expected {owner}")


def mirror_coord_release(
    home: Path,
    task_id: str,
    owner: str,
    actor: str,
    expected_claim_revision: str | None,
    transition_id: str = "",
) -> bool:
    """Mirror a release only when its owner and revision still match."""
    if expected_claim_revision is None:
        return False
    CardStore(home).append_event(
        task_id,
        "release_claim",
        actor,
        released_owner=owner,
        expected_claim_revision=expected_claim_revision,
        transition_id=transition_id or uuid.uuid4().hex,
    )
    return True


def mirror_coord_move(
    home: Path,
    task_id: str,
    column: str,
    agent: str,
    order: Optional[int] = None,
    transition_id: str = "",
) -> None:
    """Mirror a kanban move into the CardStore."""
    CardStore(home).append_event(
        task_id,
        "move",
        agent or "mcp",
        column=column,
        order=order,
        transition_id=transition_id or uuid.uuid4().hex,
    )


def mirror_coord_describe(
    home: Path,
    task_id: str,
    agent: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> None:
    """Mirror a describe (title/description edit) into the CardStore.

    Only the fields actually supplied are written, so a caller editing just the
    description never emits a null title that would blank the folded one.
    """
    payload: dict[str, str] = {}
    if title is not None:
        payload["title"] = title
    if description is not None:
        payload["description"] = description
    if not payload:
        return
    CardStore(home).append_event(task_id, "describe", agent or "mcp", **payload)


def mirror_coord_archive(home: Path, task_id: str, agent: str) -> None:
    """Mirror a coord archival into the CardStore."""
    CardStore(home).append_event(task_id, "archive", agent or "archive")


# Store-served open count may lag legacy by a few cards mid-sync; anything
# beyond this is drift worth alerting on (card ba4af853 was legacy ~310 vs
# store 427).
OPEN_DRIFT_THRESHOLD = 5


def _open_count(cards: dict, known_status_ids: Optional[set[str]] = None) -> int:
    """Count comparable coord-board OPEN cards.

    Args:
        cards: Projected cards keyed by identifier.
        known_status_ids: Optional card ids whose immutable legacy record
            carries an explicit ``status`` field. Other cards are unknown.

    Returns:
        The number of non-archived task cards explicitly known to be open.
    """
    return sum(
        1
        for card_id, card in cards.items()
        if (known_status_ids is None or card_id in known_status_ids)
        and not card.archived
        and card.kind.value in ("task", "epic")
        and card.status.value == "backlog"
    )


def _legacy_status_ids(home: Path) -> set[str]:
    """Return task ids with status physically present in the legacy record.

    Args:
        home: Shared SKCapstone root.

    Returns:
        Task ids whose immutable birth record explicitly carries ``status``.
    """
    from .coordination import Board

    card_ids: set[str] = set()
    for path in sorted(Board(home).tasks_dir.glob("*.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        card_id = record.get("id") if isinstance(record, dict) else None
        if isinstance(card_id, str) and card_id and "status" in record:
            card_ids.add(card_id)
    return card_ids


def parity_check(home: Path, open_drift_threshold: int = OPEN_DRIFT_THRESHOLD) -> dict:
    """Diff the legacy board against the CardStore fold.

    Compares only fields physically maintained by each legacy record. A
    projected default for an absent legacy field is unknown, not disagreement.
    The open-count alert likewise compares only records with explicit legacy
    lifecycle state.

    Returns:
        dict: ``{"checked", "matched", "mismatches", "missing",
        "open_legacy", "open_store", "open_drift", "open_drift_threshold",
        "open_alert"}``.
    """
    from .card import KanbanBoard

    store = CardStore(home)
    # Force the LEGACY projection for the comparison side (otherwise KanbanBoard
    # would return the store and we would be comparing the store to itself). This
    # keeps parity a real drift detector for legacy hot-backup vs the store.
    with _forced_legacy_read():
        legacy = {c.id: c for c in KanbanBoard(home).cards(include_archived=True)}
    stored = {c.id: c for c in store.list_cards(include_archived=True)}
    legacy_status_ids = _legacy_status_ids(home)

    # Coarse lifecycle bucket: legacy coord can only derive todo/active/done from
    # its claim files, so kanban-native column moves (ready<->doing<->review) made
    # on the board live only in the store and must NOT read as backup drift. The
    # monitor still catches real divergence (a card done/archived in one but not
    # the other, or a different owner).
    def _bucket(status_value: str) -> str:
        return {
            "backlog": "todo",
            "ready": "active",
            "doing": "active",
            "review": "active",
            "done": "done",
        }.get(status_value, status_value)

    mismatches: list[dict] = []
    informational: list[dict] = []
    missing: list[str] = []
    matched = 0
    for cid, lc in legacy.items():
        sc = stored.get(cid)
        if sc is None:
            missing.append(cid)
            continue
        # GATING diffs: state the mirror is supposed to keep in step, and that
        # reconcile_from_legacy() can actually converge. A diff here means the
        # mirror is genuinely broken.
        diff = {}
        # Legacy task JSON is a birth record. A projected status default for
        # a record without that field is unknown and cannot disagree.
        if cid in legacy_status_ids and _bucket(lc.status.value) != _bucket(sc.status.value):
            diff["status"] = [lc.status.value, sc.status.value]
        if (lc.owner or None) != (sc.owner or None):
            diff["owner"] = [lc.owner, sc.owner]
        if lc.archived != sc.archived:
            diff["archived"] = [lc.archived, sc.archived]
        if lc.acceptance_criteria != sc.acceptance_criteria:
            diff["acceptance_criteria"] = [
                lc.acceptance_criteria,
                sc.acceptance_criteria,
            ]

        # INFORMATIONAL diffs: priority and swimlane are written STORE-ONLY by
        # the dashboard, so legacy is the stale side by design and
        # reconcile_from_legacy() deliberately refuses to touch them (see its
        # docstring). Counting them as gate failures made the gate
        # UNSATISFIABLE: it reported a drift class that no legitimate action
        # could clear, so `parity --check` could sit red forever with nothing to
        # do about it. A gate nobody can satisfy is a gate everybody learns to
        # ignore, and then it is not a gate at all.
        #
        # They are still REPORTED, just not fatal. Removing a signal the tool
        # knows is false is not the same as weakening the check; hiding it
        # entirely would be.
        info = {}
        if lc.priority != sc.priority:
            info["priority"] = [lc.priority, sc.priority]
        if lc.swimlane != sc.swimlane:
            info["swimlane"] = [lc.swimlane, sc.swimlane]

        if diff:
            mismatches.append({"id": cid, "diff": diff})
        else:
            matched += 1
        if info:
            informational.append({"id": cid, "diff": info})
    # Compare only lifecycle values the legacy side actually carries. A
    # status-less birth record projected to backlog is unknown, not evidence
    # that the CardStore completion is wrong.
    open_legacy = _open_count(legacy, legacy_status_ids)
    open_store = _open_count(stored, legacy_status_ids)
    open_drift = abs(open_legacy - open_store)
    return {
        "checked": len(legacy),
        "matched": matched,
        "mismatches": mismatches,
        "informational": informational,
        "missing": missing,
        "open_legacy": open_legacy,
        "open_store": open_store,
        "open_drift": open_drift,
        "open_drift_threshold": open_drift_threshold,
        "open_alert": open_drift > open_drift_threshold,
    }


def _would_uncomplete(diff: dict) -> bool:
    """True when converging this diff onto legacy would move a card OUT of done.

    ``diff`` maps field -> ``[legacy_value, store_value]``. Only status can
    un-complete: the archived/reopen branch targets ``diff["status"][0]`` too,
    so if status is absent the two sides already agree and nothing moves.
    """
    if "status" not in diff:
        return False
    legacy_status, store_status = diff["status"][0], diff["status"][1]
    return store_status == Column.DONE.value and legacy_status != Column.DONE.value


def reconcile_from_legacy(
    home: Path, dry_run: bool = True, allow_uncomplete: bool = False
) -> dict:
    """One-time repair: append corrective store events where the fold still
    diverges from the authoritative legacy board.

    The fold now consumes the legacy archive index and the card_events overlay
    directly, so the only residual drift is state that lives ONLY in mutable
    legacy files with no per-event timestamps: claims/completions recorded in
    ``agents/*.json`` before the mirror was enabled (status + owner). This
    walks ``parity_check`` mismatches and appends move/assign/unassign/archive
    events (writer ``reconcile``) to converge the store on legacy.

    Priority/swimlane diffs are intentionally NOT touched: the dashboard
    writes those store-only, so there legacy is the stale side.

    Additive and idempotent: pure appends, and a second run finds no diffs.

    NEVER un-completes work. This routine converges the store ONTO legacy, a
    premise that was safe before the Phase-4 read cutover and is not safe now:
    the board is served FROM the store, so legacy is a projection that lags, and
    a card completed in the store but not yet reflected in legacy looks
    identical to real drift. Converging that card would move it out of ``done``
    and the parity gate would go green BECAUSE the completion was destroyed.
    Observed live on card b24c71b5 on 2026-08-17 (store had a real ``complete``
    event; legacy still said ``ready``). So a card whose STORE state is ``done``
    is skipped entirely, not partially converged, and reported for a human.
    ``allow_uncomplete=True`` opts back in once a direction of authority is
    decided (see card be8d5561).

    Returns:
        dict: ``{"fixed": n}`` or ``{"would_fix": n}`` when dry_run, plus
        ``{"skipped_uncomplete": [ids]}``.
    """
    par = parity_check(home)
    store = CardStore(home)
    count = 0
    skipped: list[str] = []
    for m in par["mismatches"]:
        cid = m["id"]
        diff = m["diff"]
        # Guard BEFORE building actions, so a done card is skipped whole rather
        # than having its owner rewritten while its status is left alone.
        if not allow_uncomplete and _would_uncomplete(diff):
            skipped.append(cid)
            continue
        actions: list[tuple[str, dict]] = []
        if "archived" in diff:
            legacy_archived = diff["archived"][0]
            if legacy_archived:
                actions.append(("archive", {}))
            else:
                actions.append(("reopen", {"column": diff.get("status", [None])[0]}))
        if "status" in diff:
            legacy_col = diff["status"][0]
            if legacy_col in {c.value for c in Column}:
                actions.append(("move", {"column": legacy_col}))
        if "owner" in diff:
            legacy_owner = diff["owner"][0]
            if legacy_owner:
                actions.append(("assign", {"owner": legacy_owner}))
            else:
                actions.append(("unassign", {}))
        if not actions:
            continue
        count += 1
        if dry_run:
            continue
        for action, payload in actions:
            store.append_event(cid, action, "reconcile", **payload)
    key = "would_fix" if dry_run else "fixed"
    return {key: count, "skipped_uncomplete": skipped}


# Column -> legacy coord status. Legacy has no 'review' state (its status is
# derived purely from agent claim/complete files), so review folds to its
# closest active legacy equivalent, in_progress.
_COLUMN_TO_LEGACY_STATUS = {
    Column.BACKLOG.value: "open",
    Column.READY.value: "claimed",
    Column.DOING.value: "in_progress",
    Column.REVIEW.value: "in_progress",
    Column.DONE.value: "done",
}

# Synthetic owner used to hold a done/claimed card that has no owner in the
# store, so its non-open status still survives a round-trip to the legacy board
# (legacy status lives only in agent files, which require an owner).
_EXPORT_OWNER = "legacy-export"


def export_to_legacy(home: Path, dry_run: bool = False) -> dict:
    """Rebuild a current legacy coord board (tasks/ + agents/) from the store.

    The inverse of :func:`import_from_legacy` and the rollback safety net for
    Phase 4e-retire: once legacy stops being hot-written, flipping
    ``SKCOORD_CARD_STORE=0`` and running this reconstructs a fully-current
    legacy projection from the event-sourced store, so the one-way door has a
    code path back.

    Two layers, treated differently:

    * **Task files** (immutable birth-facts) are only written for store cards
      that have no legacy file yet (i.e. cards born after retirement). Existing
      task files are left untouched -- they are immutable and carry richer
      fields (``notes``) the store does not model. ``acceptance_criteria`` for a
      synthesized file comes from the current folded card projection, so a
      rollback preserves the latest accepted amendment.
    * **Agent files** (the mutable status layer) are rebuilt: the coord
      task-status fields (``current_task``, ``claimed_tasks``,
      ``completed_tasks``) are recomputed from the store, while identity fields
      (``capabilities``, ``itil_claims``, ``host``, ``notes``, ``state``) on any
      existing agent file are preserved.

    Only coord-origin ``task``/``epic`` cards drive the coord status layer; ITIL
    cards (incident/problem/change) have their own store and are ignored here.

    Column -> legacy status: backlog->open (no agent entry), ready->claimed,
    doing->in_progress, review->in_progress, done->done. Because a legacy agent
    holds exactly one ``current_task``, an owner with multiple active
    (doing/review) cards keeps the first as ``current_task`` and the rest fall
    to ``claimed_tasks`` -- an inherent legacy limitation, not data loss (every
    card stays owned and non-done).

    Args:
        home: Agent home whose ``coordination/`` board is rebuilt.
        dry_run: When True, compute counts without writing any files.

    Returns:
        dict: ``{"cards": t, "tasks_written": n, "agents_written": m}``.
    """
    from .atomic_io import atomic_write_text
    from .coordination import AgentFile, Board, Task, TaskPriority, _slugify_filename

    store = CardStore(home)
    board = Board(home)
    board.ensure_dirs()
    cards = [c for c in store.list_cards(include_archived=True)]
    coord_cards = [c for c in cards if c.kind.value in ("task", "epic")]

    # --- Task files: synthesize only for cards with no legacy file ---
    existing_ids: set[str] = set()
    for f in board.tasks_dir.glob("*.json"):
        try:
            existing_ids.add(json.loads(f.read_text(encoding="utf-8")).get("id"))
        except Exception:  # noqa: BLE001
            continue
    tasks_written = 0
    for c in coord_cards:
        if c.id in existing_ids:
            continue
        try:
            priority = TaskPriority(c.priority)
        except ValueError:
            priority = TaskPriority.MEDIUM
        task = Task(
            id=c.id,
            title=c.title,
            description=c.description,
            priority=priority,
            tags=list(c.labels),
            created_by=c.originator,
            created_at=c.created_at or _now_iso(),
            acceptance_criteria=list(c.acceptance_criteria),
            dependencies=list(c.dependencies),
        )
        tasks_written += 1
        if dry_run:
            continue
        slug = _slugify_filename(task.title)[:40]
        path = board.tasks_dir / f"{task.id}-{slug}.json"
        atomic_write_text(path, json.dumps(task.model_dump(), indent=2) + "\n")

    # --- Agent status layer: recompute from the store, preserve identity ---
    claimed: dict[str, list[str]] = {}
    completed: dict[str, list[str]] = {}
    in_progress: dict[str, list[str]] = {}
    for c in coord_cards:
        if c.meta.get("voided"):
            continue
        status = _COLUMN_TO_LEGACY_STATUS.get(c.status.value, "open")
        if status == "open":
            continue
        owner = c.owner or _EXPORT_OWNER
        if status == "done":
            completed.setdefault(owner, []).append(c.id)
        elif status == "in_progress":
            in_progress.setdefault(owner, []).append(c.id)
            claimed.setdefault(owner, []).append(c.id)
        else:  # claimed
            claimed.setdefault(owner, []).append(c.id)

    existing_agents = {a.agent: a for a in board.load_agents()}
    owners = set(claimed) | set(completed) | set(in_progress) | set(existing_agents)
    agents_written = 0
    for owner in sorted(owners):
        base = existing_agents.get(owner)
        af = base.model_copy(deep=True) if base is not None else AgentFile(agent=owner)
        af.claimed_tasks = sorted(set(claimed.get(owner, [])))
        af.completed_tasks = sorted(set(completed.get(owner, [])))
        ip = in_progress.get(owner, [])
        af.current_task = ip[0] if ip else None
        agents_written += 1
        if dry_run:
            continue
        board.save_agent(af)

    return {
        "cards": len(coord_cards),
        "tasks_written": tasks_written,
        "agents_written": agents_written,
    }
