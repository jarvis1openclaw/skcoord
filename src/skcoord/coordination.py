"""
SKCapstone Coordination - Multi-agent task board.

Conflict-free design: each agent writes only to its own files.
Syncthing propagates everything. Zero write conflicts.

Directory layout:
    ~/.skcapstone/coordination/
    ├── tasks/           # One JSON file per task (creator owns it)
    ├── agents/          # One JSON file per agent (self-managed)
    └── BOARD.md         # Human-readable overview (auto-generated)
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import re
import socket
import stat
import time
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Optional, Sequence

from pydantic import BaseModel, Field, field_validator

from .atomic_io import atomic_write_text

logger = logging.getLogger(__name__)


def validate_shared_home(home: Path) -> Path:
    """Return the SKCapstone root, rejecting its coordination child.

    Callers pass the shared root that owns sibling ``coordination/`` and
    ``cards/`` directories. Accepting the coordination child as that root
    silently doubles the path and strands append-only writes.
    """
    root = Path(home).expanduser()
    if root.name == "coordination":
        raise ValueError(
            "coordination home must be the shared root, not its coordination subdirectory"
        )
    return root


@contextmanager
def _board_mutation_lock(home: Path, timeout_seconds: float = 5.0):
    """Lock agent projections before sorted per-card locks.

    Lock order is always board, then card identifiers in lexical order, then
    any lifecycle-only lock. ``flock`` is local-host coordination only. Remote
    writers converge through ordered, preconditioned card events.
    """
    from .card_store import _open_lockfile

    deadline = time.monotonic() + timeout_seconds
    with _open_lockfile(home, "board-mutations.lock", "board mutation") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("timed out acquiring board mutation lock")
                time.sleep(0.01)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _now_iso() -> str:
    """UTC now as an ISO-8601 string (shared by autopilot mutators)."""
    return datetime.now(timezone.utc).isoformat()


# Joule Economy work grade vocabulary. Canonical source of truth:
# skcapstone/docs/superpowers/specs/joule-grade-vocabulary.json, owned by
# skcapstone/docs/superpowers/specs/2026-08-14-joule-economy-design.md
# section 3. That file says "Consume this file, do not retype the enums" but
# skcoord is a separate repo from skcapstone with no cheap runtime import
# today, so these are a deliberate hardcoded stopgap. If the vocabulary JSON
# changes, these four constants must be updated to match in the same change.
# size and risk are separate axes and must never share labels - risk never
# accepts an S/M/L/XL value.
_GRADE_SIZE_RANK = {"S": 0, "M": 1, "L": 2, "XL": 3}
_GRADE_RISK_RANK = {"low": 0, "med": 1, "high": 2, "crit": 3}
_GRADE_SENSITIVITY_VALUES = {"public", "internal", "secret"}
_GRADE_POOL_VALUES = {"private", "public"}
_GRADE_CLASS_BY_RANK = {0: "S", 1: "M", 2: "L", 3: "XL"}


def _slugify_filename(text: str) -> str:
    """Convert text to a filesystem-safe slug.

    Removes or replaces characters that are illegal in filenames:
    - Forward slash (/) → dash (-)
    - Backslash (\\) → dash (-)
    - Colon (:) → dash (-)
    - Other special chars → removed

    Args:
        text: Input string (e.g., task title)

    Returns:
        Safe filename slug
    """
    slug = text.lower().strip()
    # Replace path separators and other illegal chars with dash
    slug = re.sub(r'[/\\:*?"<>|]', "-", slug)
    # Remove remaining non-word chars (except dash and space)
    slug = re.sub(r"[^\w\s-]", "", slug)
    # Convert spaces and underscores to single dash
    slug = re.sub(r"[\s_]+", "-", slug)
    # Remove leading/trailing dashes
    return slug.strip("-")


class TaskPriority(str, Enum):
    """Task urgency levels."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TaskStatus(str, Enum):
    """Task lifecycle states.

    Derived from agent files, not stored on the task itself.
    A task is 'open' until an agent claims it via their agent file.
    """

    OPEN = "open"
    CLAIMED = "claimed"
    IN_PROGRESS = "in_progress"
    REVIEW = "review"
    DONE = "done"
    BLOCKED = "blocked"


class AgentState(str, Enum):
    """Agent availability."""

    ACTIVE = "active"
    IDLE = "idle"
    OFFLINE = "offline"


class Task(BaseModel):
    """A unit of work on the coordination board.

    Task files are written once by the creator and are effectively
    immutable. Status is derived from agent claim files, not from
    the task file itself. This eliminates write conflicts.
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:8])
    title: str
    description: str = ""
    priority: TaskPriority = TaskPriority.MEDIUM
    tags: list[str] = Field(default_factory=list)
    created_by: str = ""
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    acceptance_criteria: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    meta: dict = Field(default_factory=dict)


class AgentFile(BaseModel):
    """An agent's self-managed status and claim record.

    Each agent owns exactly one file: agents/{name}.json.
    Only that agent writes to it. Other agents read it to
    see claims, progress, and availability.
    """

    agent: str
    last_seen: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    host: str = Field(default_factory=socket.gethostname)
    state: AgentState = AgentState.ACTIVE
    current_task: Optional[str] = None
    claimed_tasks: list[str] = Field(default_factory=list)
    completed_tasks: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    itil_claims: list[str] = Field(default_factory=list)
    notes: str = ""

    @field_validator("state", mode="before")
    @classmethod
    def normalize_legacy_terminal_state(cls, value: object) -> object:
        """Map the old task outcome spelling to an availability state."""
        return AgentState.IDLE if value == "completed" else value

    @field_validator("agent", mode="before")
    @classmethod
    def validate_agent_name(cls, value: object) -> str:
        """Require one portable filename stem for an agent projection."""
        if not isinstance(value, str) or not re.fullmatch(
            r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,126}[A-Za-z0-9])?", value
        ):
            raise ValueError("agent name must be a canonical filename stem")
        return value


class TaskView(BaseModel):
    """A task enriched with derived status from agent claims."""

    task: Task
    status: TaskStatus = TaskStatus.OPEN
    claimed_by: Optional[str] = None


@dataclass(frozen=True)
class TaskViewReadBatch:
    """One bounded owner result and its owner-maintained population identity."""

    card_ids: Sequence[str]
    population_state: str


@dataclass(frozen=True)
class TaskViewReadScope:
    """Owner-authorized keyset reader for bounded task-view pages.

    The owner reader returns no more than the requested identifiers plus an
    exact state identity maintained by the authorization source. SKCoord never
    materializes or revisions the complete eligible population. Scope is
    bounded to 256 Unicode characters and 1024 UTF-8 bytes, which fits the
    cursor's explicit maximum encoded size even at the worst Unicode bound.
    """

    authorization_scope: str
    read_page: Callable[[str | None, int], TaskViewReadBatch]

    def __post_init__(self) -> None:
        from .card_store import _TASK_VIEW_SCOPE_MAX_BYTES, _TASK_VIEW_SCOPE_MAX_CHARACTERS

        scope = self.authorization_scope
        if not isinstance(scope, str) or not 1 <= len(scope) <= _TASK_VIEW_SCOPE_MAX_CHARACTERS:
            raise ValueError(
                "authorization scope must contain 1 to 256 characters and at most 1024 UTF-8 bytes"
            )
        try:
            scope_bytes = scope.encode()
        except UnicodeEncodeError as exc:
            raise ValueError("authorization scope must be valid UTF-8") from exc
        if len(scope_bytes) > _TASK_VIEW_SCOPE_MAX_BYTES:
            raise ValueError(
                "authorization scope must contain 1 to 256 characters and at most 1024 UTF-8 bytes"
            )
        if not callable(self.read_page):
            raise TypeError("task-view scope requires an owner page reader")


class TaskViewPage(BaseModel):
    """One bounded page plus an opaque continuation cursor."""

    model_config = {"extra": "forbid", "frozen": True}

    items: tuple[TaskView, ...]
    population_state: str
    next_cursor: str | None
    has_more: bool
    eligible_records_touched: int = Field(ge=0, le=201)


class Board:
    """The coordination board - reads tasks and agent files to
    present a unified view of work across all agents.

    Args:
        home: Path to ~/.skcapstone (or test equivalent).
              In multi-agent mode, pass the shared root (not the
              per-agent home) so all agents see the same board.
    """

    def __init__(self, home: Path) -> None:
        self.home = validate_shared_home(home)
        self.coord_dir = self.home / "coordination"
        self.tasks_dir = self.coord_dir / "tasks"
        self.agents_dir = self.coord_dir / "agents"
        self.archive_dir = self.coord_dir / "archive"

    def ensure_dirs(self) -> None:
        """Create coordination directories if they don't exist."""
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        self.agents_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def agent_projection_path(self, name: str) -> Path:
        """Resolve one canonical agent projection destination inside agents/."""
        canonical = AgentFile.validate_agent_name(name)
        if self.coord_dir.is_symlink() or self.agents_dir.is_symlink():
            raise ValueError("agent projection directory must not be a symlink")
        coordination = self.coord_dir.resolve(strict=True)
        agents = self.agents_dir.resolve(strict=True)
        try:
            agents.relative_to(coordination)
        except ValueError as exc:
            raise ValueError("agent projection directory escapes coordination") from exc
        path = agents / f"{canonical}.json"
        if path.parent != agents:
            raise ValueError("agent projection destination escapes the agents directory")
        return path

    def archived_ids(self) -> set[str]:
        """Union of task ids archived by any writer.

        Reads per-writer archive index files ``archive/<host>.jsonl`` (each
        line ``{"id", "archived_at", "archived_by"}``). Conflict-free: every
        writer appends only to its own host file.
        """
        ids: set[str] = set()
        if not self.archive_dir.exists():
            return ids
        for f in sorted(self.archive_dir.glob("*.jsonl")):
            try:
                for line in f.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    tid = json.loads(line).get("id")
                    if tid:
                        ids.add(tid)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping malformed archive index %s: %s", f.name, exc)
                continue
        return ids

    def archive_task(self, task_id: str, by: str = "") -> None:
        """Archive a task by appending to this host's archive index.

        Never mutates the task file; the task stays on disk but drops out of
        the default board views.
        """
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        index = self.archive_dir / f"{socket.gethostname()}.jsonl"
        entry = {"id": task_id, "archived_at": _now_iso(), "archived_by": by}
        with index.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
        self._mirror_card_store("archive", task_id=task_id, agent=by)

    def load_tasks(self, include_archived: bool = False) -> list[Task]:
        """Load all task files from tasks/ directory.

        Args:
            include_archived: When False (default), tasks present in the
                archive index are omitted.

        Returns:
            list[Task]: All tasks on the board.
        """
        archived = set() if include_archived else self.archived_ids()
        tasks: list[Task] = []
        if not self.tasks_dir.exists():
            return tasks
        for f in sorted(self.tasks_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                task = Task.model_validate(data)
                if task.id in archived:
                    continue
                tasks.append(task)
            except Exception as exc:  # noqa: BLE001
                # A malformed task file must NOT vanish silently - a dropped task
                # is invisible to the board (it counts as neither open nor done).
                # Log loudly so schema drift (e.g. notes-as-string) is caught.
                logger.warning("Skipping malformed task file %s: %s", f.name, exc)
                continue
        return tasks

    def load_agents(self) -> list[AgentFile]:
        """Load all agent status files from agents/ directory.

        Returns:
            list[AgentFile]: All agent records.
        """
        agents: list[AgentFile] = []
        if not self.agents_dir.exists():
            return agents
        if self.agents_dir.is_symlink() or not self.agents_dir.is_dir():
            raise ValueError("agent projection directory must be a regular directory")
        for f in sorted(self.agents_dir.glob("*.json")):
            try:
                if f.is_symlink() or not f.is_file():
                    raise ValueError("agent projection must be a regular file")
                data = json.loads(f.read_text(encoding="utf-8"))
                agent = AgentFile.model_validate(data)
                if agent.agent != f.stem:
                    raise ValueError("agent payload identity does not match its filename")
                agents.append(agent)
            except Exception as exc:  # noqa: BLE001
                # A dropped agent file loses that agent's claims/completions from
                # the derived board status - surface it instead of swallowing.
                logger.warning("Skipping malformed agent file %s: %s", f.name, exc)
                continue
        return agents

    def load_agent(self, name: str) -> Optional[AgentFile]:
        """Load a specific agent's file.

        Args:
            name: Agent name (matches filename).

        Returns:
            AgentFile or None if not found.
        """
        canonical = AgentFile.validate_agent_name(name)
        if not self.agents_dir.exists():
            return None
        path = self.agent_projection_path(canonical)
        if not path.exists():
            return None
        if path.is_symlink() or not path.is_file():
            raise ValueError("agent projection must be a regular file")
        data = json.loads(path.read_text(encoding="utf-8"))
        agent = AgentFile.model_validate(data)
        if agent.agent != canonical:
            raise ValueError("agent payload identity does not match its filename")
        return agent

    def save_agent(self, agent: AgentFile) -> Path:
        """Write an agent's status file.

        Args:
            agent: The agent record to save.

        Returns:
            Path to the written file.
        """
        self.ensure_dirs()
        agent.last_seen = datetime.now(timezone.utc).isoformat()
        path = self.agent_projection_path(agent.agent)
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ValueError("agent projection destination must be a regular file")
        self._replace_agent_projection_bytes(
            agent.agent, (json.dumps(agent.model_dump(), indent=2) + "\n").encode("utf-8")
        )
        return path

    def _agent_projection_directory_fd(self) -> int:
        """Open the validated agents directory and pin it for one operation."""
        from .card_store import _open_coordination_child_directory

        return _open_coordination_child_directory(self.home, "agents")

    def _snapshot_agent_projection(self, name: str) -> tuple[Path, bytes | None]:
        """Take a raw-byte snapshot without following a projection symlink."""
        canonical = AgentFile.validate_agent_name(name)
        self.ensure_dirs()
        path = self.agent_projection_path(canonical)
        directory_fd = self._agent_projection_directory_fd()
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            os.close(directory_fd)
            raise RuntimeError("safe agent projection reads require O_NOFOLLOW support")
        flags = os.O_RDONLY | no_follow
        try:
            try:
                descriptor = os.open(f"{canonical}.json", flags, dir_fd=directory_fd)
            except FileNotFoundError:
                return path, None
            try:
                file_stat = os.fstat(descriptor)
                if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1:
                    raise ValueError("agent projection must be a regular single-link file")
                chunks: list[bytes] = []
                while True:
                    chunk = os.read(descriptor, 65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
                return path, b"".join(chunks)
            finally:
                os.close(descriptor)
        finally:
            os.close(directory_fd)

    def _replace_agent_projection_bytes(self, name: str, payload: bytes) -> Path:
        """Crash-atomically replace one projection with no-follow protection."""
        canonical = AgentFile.validate_agent_name(name)
        path = self.agent_projection_path(canonical)
        directory_fd = self._agent_projection_directory_fd()
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            os.close(directory_fd)
            raise RuntimeError("safe agent projection writes require O_NOFOLLOW support")
        temp_name = f".{canonical}.restore-{uuid.uuid4().hex}.tmp"
        descriptor = -1
        try:
            try:
                existing = os.stat(f"{canonical}.json", dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                existing = None
            if existing is not None and (
                stat.S_ISLNK(existing.st_mode)
                or not stat.S_ISREG(existing.st_mode)
                or existing.st_nlink != 1
            ):
                raise ValueError("agent projection destination must be a regular single-link file")
            descriptor = os.open(
                temp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | no_follow,
                0o600,
                dir_fd=directory_fd,
            )
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
            temporary = os.stat(temp_name, dir_fd=directory_fd, follow_symlinks=False)
            opened = os.fstat(descriptor)
            if (
                stat.S_ISLNK(temporary.st_mode)
                or not stat.S_ISREG(temporary.st_mode)
                or temporary.st_nlink != 1
                or (temporary.st_dev, temporary.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ValueError("agent projection temporary destination is unsafe")
            try:
                current = os.stat(f"{canonical}.json", dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                current = None
            if current is not None and (
                stat.S_ISLNK(current.st_mode)
                or not stat.S_ISREG(current.st_mode)
                or current.st_nlink != 1
            ):
                raise ValueError("agent projection destination must be a regular single-link file")
            os.replace(
                temp_name,
                f"{canonical}.json",
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
                descriptor = -1
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            raise
        finally:
            os.close(directory_fd)
        return path

    def _restore_agent_projection(self, name: str, original: bytes | None) -> None:
        """Restore an exact raw snapshot, including a safely absent file."""
        canonical = AgentFile.validate_agent_name(name)
        if original is not None:
            self._replace_agent_projection_bytes(canonical, original)
            return
        directory_fd = self._agent_projection_directory_fd()
        try:
            try:
                existing = os.stat(f"{canonical}.json", dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if (
                stat.S_ISLNK(existing.st_mode)
                or not stat.S_ISREG(existing.st_mode)
                or existing.st_nlink != 1
            ):
                raise ValueError("agent projection destination must be a regular single-link file")
            os.unlink(f"{canonical}.json", dir_fd=directory_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _append_release_recovery(self, payload: dict) -> Path:
        """Durably append one no-follow recovery record for a failed mutation."""
        from .card_store import _open_coordination_child_directory

        directory_fd = _open_coordination_child_directory(self.home, "recovery")
        filename = f"{socket.gethostname()}.jsonl"
        descriptor = -1
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            os.close(directory_fd)
            raise RuntimeError("safe recovery records require O_NOFOLLOW support")
        try:
            try:
                existing = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                existing = None
            if existing is not None and (
                stat.S_ISLNK(existing.st_mode)
                or not stat.S_ISREG(existing.st_mode)
                or existing.st_nlink != 1
            ):
                raise ValueError("recovery record destination is unsafe")
            descriptor = os.open(
                filename,
                os.O_APPEND | os.O_CREAT | os.O_WRONLY | no_follow,
                0o600,
                dir_fd=directory_fd,
            )
            encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
            offset = 0
            while offset < len(encoded):
                offset += os.write(descriptor, encoded[offset:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.fsync(directory_fd)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(directory_fd)
        return self.coord_dir / "recovery" / filename

    def _recover_store_failure(
        self,
        *,
        operation: str,
        task_id: str,
        owner: str,
        actor: str,
        original: bytes | None,
        error: Exception,
        transition_ids: list[tuple[str, str]],
    ) -> None:
        """Persist an intent then restore the legacy projection fail-closed."""
        record = {
            "record_id": uuid.uuid4().hex,
            "operation": operation,
            "task_id": task_id,
            "owner": owner,
            "actor": actor,
            "phase": "card_store_write_uncertain",
            "error": str(error),
            "legacy_snapshot_present": original is not None,
            "transition_ids": [
                {"task_id": transition_task_id, "transition_id": transition_id}
                for transition_task_id, transition_id in transition_ids
            ],
            "recorded_at": _now_iso(),
        }
        try:
            self._append_release_recovery(record)
        except Exception as receipt_error:
            try:
                self._restore_agent_projection(owner, original)
            except Exception as restore_error:
                raise RuntimeError(
                    "mutation recovery record and restore both failed"
                ) from restore_error
            raise RuntimeError(
                "mutation recovery record could not be persisted"
            ) from receipt_error
        try:
            self._restore_agent_projection(owner, original)
        except Exception as restore_error:
            record["phase"] = "legacy_restore_failed"
            record["restore_error"] = str(restore_error)
            self._append_release_recovery(record)
            raise RuntimeError("recovery record written; board is fail-closed") from restore_error
        record["phase"] = "legacy_restored_card_store_uncertain"
        self._append_release_recovery(record)

    def _store_transitions_are_durable(self, transitions: list[tuple[str, str]]) -> bool:
        """Return whether every exact intended CardStore transition exists."""
        from .card_store import CardStore

        return all(
            CardStore(self.home).has_transition(task_id, transition_id)
            for task_id, transition_id in transitions
        )

    def _store_transitions_are_applied(
        self,
        transitions: list[tuple[str, str]],
        expected_states: list[tuple[str, str | None, str]],
    ) -> bool:
        """Return whether durable events also folded to their intended state.

        A write can fsync successfully and then raise in a wrapper. That is a
        successful mutation only if the deterministic fold reflects the exact
        intended result. A cross-host event may instead make a preconditioned
        event conflict, so event presence alone must never bless a divergent
        legacy projection.
        """
        from .card_store import CardStore

        if not self._store_transitions_are_durable(transitions):
            return False
        store = CardStore(self.home)
        for task_id, expected_owner, expected_status in expected_states:
            card = store.fold(task_id)
            if (
                card is None
                or card.owner != expected_owner
                or card.status.value != expected_status
            ):
                return False
        return True

    def _release_transition_is_applied(
        self,
        task_id: str,
        transition_id: str,
        owner: str,
        revision: str,
        before,
    ) -> bool:
        """Return whether one exact current or conflicting claim was released."""
        from .card import Column
        from .card_store import CardStore

        store = CardStore(self.home)
        if not store.has_transition(task_id, transition_id):
            return False
        card = store.fold(task_id)
        if card is None:
            return False
        if before.owner == owner and before.meta.get("_claim_revision") == revision:
            return (
                card.owner is None
                and card.status == Column.BACKLOG
                and "_claim_revision" not in card.meta
            )
        conflicts = list(before.meta.get("claim_conflicts", []))
        matching = [
            conflict
            for conflict in conflicts
            if isinstance(conflict, dict)
            and conflict.get("owner") == owner
            and conflict.get("claim_revision") == revision
        ]
        if len(matching) != 1:
            return False
        expected_conflicts = [conflict for conflict in conflicts if conflict is not matching[0]]
        return (
            card.owner == before.owner
            and card.status == before.status
            and card.meta.get("_claim_revision") == before.meta.get("_claim_revision")
            and list(card.meta.get("claim_conflicts", [])) == expected_conflicts
        )

    def _card_matches_snapshot(self, task_id: str, snapshot) -> bool:
        """Return whether a CardStore card has its captured owner and column."""
        from .card_store import CardStore

        current = CardStore(self.home).fold(task_id)
        return (
            current is not None
            and current.owner == snapshot.owner
            and current.status == snapshot.status
            and current.order == snapshot.order
        )

    def _compensate_partial_claim(
        self,
        *,
        task_id: str,
        owner: str,
        claim_revision: str,
        claim_transition: str,
        snapshot,
    ) -> list[tuple[str, str]]:
        """Undo a durable target claim when its paired bump cannot be mirrored.

        Claiming a second current task consists of two CardStore transitions.
        If the target claim became durable but the bumped-card demotion did not,
        restore the target card to its exact captured owner, column, and order
        before restoring the raw legacy agent projection.
        """
        from .card_store import CardStore

        if snapshot is None or not self._store_transitions_are_durable(
            [(task_id, claim_transition)]
        ):
            return []
        current = CardStore(self.home).fold(task_id)
        if current is None or current.owner != owner or current.status.value != "doing":
            return []

        transitions: list[tuple[str, str]] = []
        release_transition = uuid.uuid4().hex
        try:
            self._mirror_card_store(
                "release",
                task_id=task_id,
                owner=owner,
                actor=owner,
                expected_claim_revision=claim_revision,
                transition_id=release_transition,
            )
        except Exception:
            if not self._store_transitions_are_applied(
                [(task_id, release_transition)], [(task_id, None, "backlog")]
            ):
                raise
        transitions.append((task_id, release_transition))

        if snapshot.owner is not None:
            restore_claim_transition = uuid.uuid4().hex
            self._mirror_card_store(
                "claim",
                task_id=task_id,
                agent=snapshot.owner,
                transition_id=restore_claim_transition,
                claim_revision=uuid.uuid4().hex,
            )
            transitions.append((task_id, restore_claim_transition))

        restored = CardStore(self.home).fold(task_id)
        if restored is None:
            raise RuntimeError("claim compensation could not fold the target card")
        if restored.status != snapshot.status or restored.order != snapshot.order:
            move_transition = uuid.uuid4().hex
            self._mirror_card_store(
                "move",
                task_id=task_id,
                agent=owner,
                column=snapshot.status.value,
                order=snapshot.order,
                transition_id=move_transition,
            )
            transitions.append((task_id, move_transition))

        if not self._card_matches_snapshot(task_id, snapshot):
            raise RuntimeError("claim compensation did not restore the target card")
        return transitions

    def _assert_no_claim_conflict(self, task_id: str, owner: str) -> None:
        """Block completion until a concurrent cross-host ownership conflict is resolved."""
        from .card_store import CardStore

        card = CardStore(self.home).fold(task_id)
        if card is None:
            return
        conflicts = card.meta.get("claim_conflicts")
        if conflicts:
            raise ValueError(
                f"Task {task_id} has unresolved concurrent claim conflicts; "
                "explicit release or reconciliation is required"
            )
        if card.owner not in {None, owner}:
            raise ValueError(f"Task {task_id} is owned by {card.owner}, not {owner}")

    def create_task(self, task: Task) -> Path:
        """Write a new task file.

        Args:
            task: The task to create.

        Returns:
            Path to the written file.
        """
        self.ensure_dirs()
        slug = _slugify_filename(task.title)[:40]
        # Reason: filename includes id + slug for human readability
        filename = f"{task.id}-{slug}.json"
        path = self.tasks_dir / filename
        # CardStore is the creation policy choke point. Govern before writing the
        # legacy projection so a refused CLI or MCP call cannot leave an orphan.
        self._mirror_card_store("create", task=task)
        atomic_write_text(path, json.dumps(task.model_dump(), indent=2) + "\n")
        return path

    def _mirror_card_store(self, op: str, **kw) -> None:
        """Flag-gated dual-write into the event-sourced CardStore (Phase 4).

        A no-op unless ``SKCOORD_CARD_STORE`` is ``1``/``dual``. Lifecycle
        mutations fail closed so callers can restore their raw agent snapshot.
        """
        try:
            from . import card_store

            if not card_store.card_store_write_enabled():
                return
            if op == "create":
                card_store.mirror_coord_create(self.home, kw["task"])
            elif op == "claim":
                card_store.mirror_coord_claim(
                    self.home,
                    kw["task_id"],
                    kw["agent"],
                    transition_id=kw.get("transition_id", ""),
                    claim_revision=kw.get("claim_revision", ""),
                )
            elif op == "complete":
                card_store.mirror_coord_complete(
                    self.home,
                    kw["task_id"],
                    kw["agent"],
                    transition_id=kw.get("transition_id", ""),
                )
            elif op == "archive":
                card_store.mirror_coord_archive(self.home, kw["task_id"], kw["agent"])
            elif op == "demote":
                card_store.mirror_coord_move(
                    self.home,
                    kw["task_id"],
                    "ready",
                    kw["agent"],
                    transition_id=kw.get("transition_id", ""),
                )
            elif op == "move":
                card_store.mirror_coord_move(
                    self.home,
                    kw["task_id"],
                    kw["column"],
                    kw["agent"],
                    order=kw.get("order"),
                    transition_id=kw.get("transition_id", ""),
                )
            elif op == "add_label":
                card_store.CardStore(self.home).append_event(
                    kw["task_id"],
                    "add_label",
                    kw["agent"],
                    label=kw["label"],
                    transition_id=kw.get("transition_id", ""),
                )
            elif op == "remove_label":
                card_store.CardStore(self.home).append_event(
                    kw["task_id"],
                    "remove_label",
                    kw["agent"],
                    label=kw["label"],
                    transition_id=kw.get("transition_id", ""),
                )
            elif op == "release":
                card_store.mirror_coord_release(
                    self.home,
                    kw["task_id"],
                    kw["owner"],
                    kw["actor"],
                    kw["expected_claim_revision"],
                    transition_id=kw.get("transition_id", ""),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("CardStore mirror (%s) failed: %s", op, exc)
            if op in {
                "create",
                "claim",
                "complete",
                "demote",
                "move",
                "release",
                "add_label",
                "remove_label",
            }:
                raise

    def _task_raw_path(self, task_id: str) -> Path:
        """Resolve one existing task JSON path without accepting traversal."""
        from .card_store import validate_card_lock_identifier

        validate_card_lock_identifier(task_id)
        matches = sorted(self.tasks_dir.glob(f"{task_id}-*.json"))
        if not matches:
            exact = self.tasks_dir / f"{task_id}.json"
            if exact.exists():
                matches = [exact]
        if not matches:
            raise FileNotFoundError(f"No task file for id {task_id} in {self.tasks_dir}")
        path = matches[0]
        if path.is_symlink() or not path.is_file():
            raise ValueError("task destination must be a regular file")
        return path

    @staticmethod
    def _read_regular_file_bytes(path: Path) -> bytes:
        """Read one task file through a pinned parent descriptor.

        The pre-open ``Path`` checks are useful diagnostics only. The actual
        mutation input must be opened with ``O_NOFOLLOW`` and verified after
        open so a symlink replacement cannot redirect a task rewrite.
        """
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None or path.name in {"", ".", ".."}:
            raise ValueError("task destination is unsafe")
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | no_follow)
        except OSError as exc:
            raise ValueError("task parent is unsafe") from exc
        descriptor = -1
        try:
            existing = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                stat.S_ISLNK(existing.st_mode)
                or not stat.S_ISREG(existing.st_mode)
                or existing.st_nlink != 1
            ):
                raise ValueError("task destination must be a regular single-link file")
            descriptor = os.open(path.name, os.O_RDONLY | no_follow, dir_fd=directory_fd)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise ValueError("task destination must be a regular single-link file")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(directory_fd)

    def _write_task_raw(self, task_id: str, mutate: Callable[[dict], None]) -> Path:
        """Atomically mutate a task's raw JSON dict, preserving all keys.

        This is the ONLY task-file mutation helper. It locates
        tasks/<id>-*.json, loads the raw dict (NOT through the Task model, so
        non-model keys such as meta.autopilot are never dropped), applies
        mutate(d) in place, and writes the result back atomically via a temp
        file plus os.replace.

        Single-writer safety is a hard precondition: only one process may call
        this at a time. autopilot-daily is pinned to a single node
        (nodes: [noroc2027]) for exactly this reason.

        Raises:
            FileNotFoundError: If no task file matches the id.
        """
        path = self._task_raw_path(task_id)
        data = json.loads(self._read_regular_file_bytes(path).decode("utf-8"))
        mutate(data)
        atomic_write_text(path, json.dumps(data, indent=2) + "\n")
        return path

    def score_task(
        self,
        task_id: str,
        round: int,
        score: int,
        notes: str = "",
        harness: str = "",
        phase: str | None = None,
        ref: str | None = None,
    ) -> Path:
        """Record an autopilot grade on a task (meta.autopilot.scores[]).

        Idempotent: a re-grade of the same (round, harness) replaces that entry
        in place instead of appending a duplicate. Goes through _write_task_raw,
        so all other keys are preserved.
        """

        def _mutate(d: dict) -> None:
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            scores = ap.setdefault("scores", [])
            entry = {
                "round": round,
                "score": score,
                "notes": notes,
                "ts": _now_iso(),
                "harness": harness,
            }
            for i, existing in enumerate(scores):
                if existing.get("round") == round and existing.get("harness") == harness:
                    scores[i] = entry
                    break
            else:
                scores.append(entry)
            if phase:
                ap["phase"] = phase
            if ref:
                ap["pr" if ref.startswith("http") else "artifact"] = ref

        return self._write_task_raw(task_id, _mutate)

    def set_grade(
        self,
        task_id: str,
        *,
        size: str,
        risk: str,
        sensitivity: str,
        joule_estimate: int | None = None,
        joule_bounty: int | None = None,
        graded_by: str = "",
        grader_model: str = "",
        rubric_version: int = 1,
        confidence: float | None = None,
        pool: str = "private",
        session_id: str | None = None,
        node: str | None = None,
    ) -> Path:
        """Write a Joule Economy work grade onto a card (meta.grade).

        Vocabulary and rule: skcapstone/docs/superpowers/specs/joule-grade-
        vocabulary.json, section 3 of the Joule Economy design. `size` is
        reasoning difficulty (S/M/L/XL); `risk` is blast radius (low/med/
        high/crit). The two axes are validated against disjoint vocabularies,
        so `risk` can never accept an S/M/L/XL value; that collision already
        happened once downstream and the axes exist to be read apart.

        `model_class` is derived as CLASS[max(size_rank, risk_rank)] and
        stored alongside the inputs so the block stays queryable without
        re-deriving it on every read.

        `sensitivity` is REQUIRED and deliberately has no default. It is a
        data-exposure gate, not an estimate: `internal` carries ceiling 1, so
        a defaulted value would let a card nobody classified leave sovereign
        hardware. The card most likely to arrive unclassified is exactly the
        one built on sealed or credential-bearing content, so an absent
        decision must not resolve to the permissive middle value. Absent has
        to stay distinguishable from decided. Do not add a default back for
        caller convenience; make the caller choose.

        `joule_estimate` is what the work is predicted to cost; `joule_bounty`
        is what the worker competes against (P4's `earned = bounty - actual`).
        `pool` gates whether the card may reach an untrusted outside worker
        (P5), so it is validated the same as the other enum fields rather
        than accepted as a free string.

        Idempotent: re-grading the same card replaces meta.grade in place
        rather than appending or duplicating. Written as a sibling of
        meta.autopilot via _write_task_raw, so every other key on the card
        survives untouched.

        Raises:
            ValueError: size, risk, sensitivity, or pool is outside its
                vocabulary.

        Returns:
            Path to the written task file.
        """
        if size not in _GRADE_SIZE_RANK:
            raise ValueError(f"invalid size {size!r}, must be one of {sorted(_GRADE_SIZE_RANK)}")
        if risk not in _GRADE_RISK_RANK:
            raise ValueError(f"invalid risk {risk!r}, must be one of {sorted(_GRADE_RISK_RANK)}")
        if sensitivity not in _GRADE_SENSITIVITY_VALUES:
            raise ValueError(
                f"invalid sensitivity {sensitivity!r}, must be one of "
                f"{sorted(_GRADE_SENSITIVITY_VALUES)}"
            )
        if pool not in _GRADE_POOL_VALUES:
            raise ValueError(f"invalid pool {pool!r}, must be one of {sorted(_GRADE_POOL_VALUES)}")

        rank = max(_GRADE_SIZE_RANK[size], _GRADE_RISK_RANK[risk])
        model_class = _GRADE_CLASS_BY_RANK[rank]

        def _mutate(d: dict) -> None:
            meta = d.setdefault("meta", {})
            grade = {
                "size": size,
                "risk": risk,
                "sensitivity": sensitivity,
                "model_class": model_class,
                "joule_estimate": joule_estimate,
                "joule_bounty": joule_bounty,
                "graded_by": graded_by,
                "grader_model": grader_model,
                "rubric_version": rubric_version,
                "confidence": confidence,
                "pool": pool,
                "graded_at": _now_iso(),
            }
            if session_id is not None:
                grade["session_id"] = session_id
            if node is not None:
                grade["node"] = node
            meta["grade"] = grade

        return self._write_task_raw(task_id, _mutate)

    # -- cross-run failure memory (meta.autopilot.attempts[]) ------------------
    # Sibling of scores[]/edits[]: a terminal non-pass of an autopilot build used
    # to vanish when the run returned, so the next run walked into the identical
    # wall. These two methods are the STORAGE half only; skcoord holds the facts
    # and skharness decides what an agent ever reads (and owns the run journal).
    _MAX_ATTEMPTS = 10

    def record_attempt(
        self,
        task_id: str,
        run_id: str,
        round: int,
        outcome: str,
        tried: str,
        why_failed: str,
        replacement_hint: str = "",
    ) -> Path:
        """Record one distilled terminal non-pass on a task (meta.autopilot.attempts[]).

        Each entry is ONE line per field, already distilled by the caller: raw
        logs, diffs, and tracebacks stay in the run journal, which `run_id`
        points at. `outcome` is a closed vocabulary (no_op | ci_red |
        direct_fail).

        Idempotent: a re-record of the same (run_id, outcome) replaces that entry
        in place rather than appending a duplicate, so a retried finalize or a
        crash-resume cannot double-count one failure. Mirrors score_task's
        (round, harness) match. Goes through _write_task_raw, so sibling keys are
        preserved.

        The trailing cap is a corruption guard, not the forgetting policy: the
        reader (skharness build_prior_feedback) bounds what reaches a prompt.
        """

        def _mutate(d: dict) -> None:
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            attempts = ap.setdefault("attempts", [])
            entry = {
                "run_id": run_id,
                "ts": _now_iso(),
                "round": round,
                "outcome": outcome,
                "tried": tried,
                "why_failed": why_failed,
                "replacement_hint": replacement_hint,
            }
            for i, existing in enumerate(attempts):
                if existing.get("run_id") == run_id and existing.get("outcome") == outcome:
                    attempts[i] = entry
                    break
            else:
                attempts.append(entry)
            if len(attempts) > self._MAX_ATTEMPTS:
                attempts[:] = attempts[-self._MAX_ATTEMPTS :]

        return self._write_task_raw(task_id, _mutate)

    def clear_attempts(self, task_id: str) -> list[dict]:
        """Wipe the card's failure memory and hand the removed entries back.

        Called on a final PASS so a flaky-CI false failure cannot haunt every
        future run of the card. Deliberately does NOT write the run journal:
        skcoord has no journal code, and the autopilot run journal is
        skharness-owned. The caller archives what it gets back, which keeps the
        "skcoord stores facts, skharness decides" split intact.

        Touches ONLY ``attempts``. ``successes`` (below) is a SIBLING key and is
        never read or written here: a success recorded on the same pass that
        triggers this clear must survive it, not be destroyed by it.
        """
        removed: list[dict] = []

        def _mutate(d: dict) -> None:
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            removed.extend(ap.get("attempts") or [])
            ap["attempts"] = []

        self._write_task_raw(task_id, _mutate)
        return removed

    # -- cross-run SUCCESS memory (meta.autopilot.successes[]) -----------------
    # A sibling of attempts[], deliberately a DIFFERENT key. clear_attempts above
    # wipes attempts[] wholesale on every terminal pass, so a success stored
    # there would be destroyed by the very event that created it. successes[]
    # is never touched by clear_attempts and has no clear method of its own:
    # nothing in this codebase forgets a success by wiping the card:
    # forgetting for successes is a READER policy (skharness), same as failures.

    def record_success(
        self,
        task_id: str,
        run_id: str,
        round: int,
        outcome: str,
        tried: str,
        why_succeeded: str,
        approach_hint: str = "",
    ) -> Path:
        """Record one distilled terminal PASS on a task (meta.autopilot.successes[]).

        Mirrors record_attempt exactly (idempotency key, corruption cap, additive
        write via _write_task_raw) except for the storage key and success-phrased
        fields: raw logs stay in the run journal, which `run_id` points at.

        Idempotent: a re-record of the same (run_id, outcome) replaces that entry
        in place rather than appending a duplicate, so a retried finalize or a
        crash-resume cannot double-count one success.

        The trailing cap is a corruption guard, not the forgetting policy: the
        reader (skharness build_prior_success_feedback) bounds what reaches a
        prompt.
        """

        def _mutate(d: dict) -> None:
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            successes = ap.setdefault("successes", [])
            entry = {
                "run_id": run_id,
                "ts": _now_iso(),
                "round": round,
                "outcome": outcome,
                "tried": tried,
                "why_succeeded": why_succeeded,
                "approach_hint": approach_hint,
            }
            for i, existing in enumerate(successes):
                if existing.get("run_id") == run_id and existing.get("outcome") == outcome:
                    successes[i] = entry
                    break
            else:
                successes.append(entry)
            if len(successes) > self._MAX_ATTEMPTS:
                successes[:] = successes[-self._MAX_ATTEMPTS :]

        return self._write_task_raw(task_id, _mutate)

    def update_task(
        self,
        task_id: str,
        description: str | None = None,
        acceptance_criteria: list[str] | None = None,
        add_tags: list[str] | None = None,
        remove_tags: list[str] | None = None,
        run_id: str | None = None,
    ) -> Path:
        """Rewrite task fields and snapshot each change for reversibility.

        Backs the Phase-0 stale action. A None argument leaves that field
        untouched. Every field that actually changes is snapshotted into
        meta.autopilot.edits[] as {field, old, new, ts, run_id}. Goes through
        the atomic raw-dict helper.
        """

        tag_events: list[tuple[str, str, str]] = []

        def _mutate(d: dict) -> None:
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            edits = ap.setdefault("edits", [])
            ts = _now_iso()
            if description is not None and description != d.get("description", ""):
                edits.append(
                    {
                        "field": "description",
                        "old": d.get("description", ""),
                        "new": description,
                        "ts": ts,
                        "run_id": run_id,
                    }
                )
                d["description"] = description
            if acceptance_criteria is not None and acceptance_criteria != d.get(
                "acceptance_criteria", []
            ):
                edits.append(
                    {
                        "field": "acceptance_criteria",
                        "old": d.get("acceptance_criteria", []),
                        "new": acceptance_criteria,
                        "ts": ts,
                        "run_id": run_id,
                    }
                )
                d["acceptance_criteria"] = acceptance_criteria
            if add_tags or remove_tags:
                existing = list(d.get("tags", []))
                merged = existing + [t for t in (add_tags or []) if t not in existing]
                if remove_tags:
                    merged = [t for t in merged if t not in set(remove_tags)]
                if merged != existing:
                    edits.append(
                        {
                            "field": "tags",
                            "old": existing,
                            "new": merged,
                            "ts": ts,
                            "run_id": run_id,
                        }
                    )
                    d["tags"] = merged
                    tag_events.extend(
                        ("add_label", tag, uuid.uuid4().hex)
                        for tag in merged
                        if tag not in existing
                    )
                    tag_events.extend(
                        ("remove_label", tag, uuid.uuid4().hex)
                        for tag in existing
                        if tag not in merged
                    )

        original_path = self._task_raw_path(task_id)
        original_bytes = self._read_regular_file_bytes(original_path)
        path = self._write_task_raw(task_id, _mutate)
        completed_events: list[tuple[str, str, str]] = []
        try:
            for action, tag, transition_id in tag_events:
                try:
                    self._mirror_card_store(
                        action,
                        task_id=task_id,
                        agent=run_id or "coord",
                        label=tag,
                        transition_id=transition_id,
                    )
                except Exception:
                    if not self._store_transitions_are_durable([(task_id, transition_id)]):
                        raise
                completed_events.append((action, tag, transition_id))
        except Exception as mirror_error:
            try:
                for action, tag, _ in reversed(completed_events):
                    inverse = "remove_label" if action == "add_label" else "add_label"
                    self._mirror_card_store(
                        inverse,
                        task_id=task_id,
                        agent=run_id or "coord",
                        label=tag,
                        transition_id=uuid.uuid4().hex,
                    )
                atomic_write_text(path, original_bytes.decode("utf-8"))
                self._append_release_recovery(
                    {
                        "record_id": uuid.uuid4().hex,
                        "operation": "label_mirror",
                        "task_id": task_id,
                        "actor": run_id or "coord",
                        "phase": "legacy_task_restored",
                        "error": str(mirror_error),
                        "recorded_at": _now_iso(),
                    }
                )
            except Exception as compensation_error:
                self._append_release_recovery(
                    {
                        "record_id": uuid.uuid4().hex,
                        "operation": "label_mirror",
                        "task_id": task_id,
                        "actor": run_id or "coord",
                        "phase": "label_compensation_failed",
                        "error": str(mirror_error),
                        "compensation_error": str(compensation_error),
                        "recorded_at": _now_iso(),
                    }
                )
                raise RuntimeError(
                    "label mirror compensation failed; board is fail-closed"
                ) from compensation_error
            raise
        return path

    def close_task_obsolete(self, task_id: str, reason: str, run_id: str | None = None) -> Path:
        """Mark a task obsolete on the task file itself.

        Task files carry no status field: done/claimed status is derived from
        agents/*.json, and autopilot is not the completing agent, so faking a
        done state via an agent file would misattribute completion. Instead
        obsolete is recorded as a machine-readable meta.autopilot.obsolete block
        plus a human-readable line appended to notes[]. Reversible and
        auditable, via the atomic raw-dict helper.
        """

        def _mutate(d: dict) -> None:
            ts = _now_iso()
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            ap["obsolete"] = {"reason": reason, "run_id": run_id, "ts": ts}
            notes = d.setdefault("notes", [])
            if isinstance(notes, list):
                notes.append(f"[obsolete {ts}] {reason}")

        return self._write_task_raw(task_id, _mutate)

    def mark_decomposed(
        self, task_id: str, children: list[str], run_id: str | None = None
    ) -> Path:
        """Park a task that autopilot split into child subtasks.

        Symmetric with close_task_obsolete: task files carry no status field, so a
        decomposed parent is recorded as a machine-readable
        meta.autopilot.decomposed block (the child ids) plus a human-readable
        notes[] line. phase0_assess skips any card carrying this marker, so the
        parent drops out of the build pool while its children carry the work.
        Reversible and auditable.
        """

        def _mutate(d: dict) -> None:
            ts = _now_iso()
            ap = d.setdefault("meta", {}).setdefault("autopilot", {})
            ap["decomposed"] = {"children": list(children), "run_id": run_id, "ts": ts}
            notes = d.setdefault("notes", [])
            if isinstance(notes, list):
                notes.append(
                    f"[decomposed {ts}] into {len(children)} subtasks: {', '.join(children)}"
                )

        return self._write_task_raw(task_id, _mutate)

    def unblocked_task_ids(self) -> set[str]:
        """Task ids whose dependencies are all completed (Phase-0 compute).

        A task is unblocked when every folded dependency is done. Tasks with no
        dependencies are trivially unblocked. This must read the same folded
        projection as ``claim_task`` so append-only dependency amendments cannot
        be bypassed by an eligibility scan.

        Returns:
            set[str]: Task ids that are unblocked.
        """
        views = self._legacy_task_views(include_archived=True)
        status_by_id = {view.task.id: view.status for view in views}
        # ``autopilot-staged`` children live in the "Proposed" lane: decomposed but
        # not yet released by a human. They must never be selected, assessed, or
        # built, so they are excluded from the unblocked set until
        # ``skos autopilot release <epic>`` strips the tag.
        return {
            view.task.id
            for view in self._legacy_task_views()
            if all(
                status_by_id.get(dep_id) == TaskStatus.DONE for dep_id in view.task.dependencies
            )
            and "autopilot-staged" not in (view.task.tags or [])
        }

    def release_stale_claims(self, agent: str, older_than_seconds: int) -> list[str]:
        """Release an agent's uncompleted claims if it has gone stale.

        AgentFile carries no per-claim timestamp, only last_seen, so staleness
        is keyed on last_seen: if the agent has not been seen for
        older_than_seconds, all of its claimed_tasks (uncompleted by
        construction) are released and current_task is cleared if it pointed at
        a released id. Returns the released ids (empty if the agent is unknown,
        fresh, or holds no claims).
        """
        from .card_store import card_mutation_lock, validate_card_lock_identifier

        canonical = AgentFile.validate_agent_name(agent)
        with _board_mutation_lock(self.home):
            af = self.load_agent(canonical)
            if af is None:
                return []
            try:
                last_seen = datetime.fromisoformat(af.last_seen)
            except (ValueError, TypeError):
                return []
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - last_seen).total_seconds()
            if age < older_than_seconds:
                return []
            released = sorted(dict.fromkeys(af.claimed_tasks))
            if not released:
                return []
            for task_id in released:
                validate_card_lock_identifier(task_id)
            changed: list[str] = []
            for task_id in released:
                with card_mutation_lock(self.home, task_id):
                    if self._release_claim_locked(
                        canonical,
                        task_id,
                        actor="stale-release",
                        allow_missing_card_store=True,
                    ):
                        changed.append(task_id)
            return changed

    def get_task_view_page(
        self,
        scope: TaskViewReadScope,
        *,
        limit: int,
        cursor: str | None = None,
        include_archived: bool = False,
    ) -> TaskViewPage:
        """Read at most ``limit + 1`` owner-authorized CardStore records.

        The authorization owner supplies a bounded keyset reader and exact
        population-state identity; this method never enumerates the CardStore.
        Cursors are process-local, opaque, integrity-protected, and bound to
        that state, the authorization scope, limit, and archive mode. A forged,
        malformed, restarted, stale, or scope-shifted cursor fails closed.

        Existing callers remain on :meth:`get_task_views`, whose unpaginated
        return type and behavior are unchanged.
        """
        from .card_store import task_view_page_from_store

        return task_view_page_from_store(
            self.home,
            scope,
            limit=limit,
            cursor=cursor,
            include_archived=include_archived,
        )

    def get_task_views(self, include_archived: bool = False) -> list[TaskView]:
        """Build enriched task views with derived status.

        Cross-references tasks against all agent claim files to
        determine each task's effective status and who claimed it.

        Args:
            include_archived: When False (default), archived tasks are omitted.

        Returns:
            list[TaskView]: Tasks with derived status.
        """
        # Read cutover (Phase 4e): the event-sourced CardStore is the default
        # read source. Legacy is still written as a hot backup and remains the
        # rollback target (SKCOORD_CARD_STORE=0).
        try:
            from . import card_store

            if card_store.card_store_read_enabled():
                store_views = card_store.task_views_from_store(
                    self.home, include_archived=include_archived
                )
                # Catastrophe guard: never silently empty the board if the store
                # is behind. Fall back to the legacy scan only when the store is
                # empty but legacy task files exist.
                if store_views or not any(self.tasks_dir.glob("*.json")):
                    return store_views
                logger.warning(
                    "CardStore returned no tasks but legacy files exist; "
                    "falling back to the legacy projection (catastrophe guard)."
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("CardStore read failed, falling back to legacy: %s", exc)

        return self._legacy_task_views(include_archived=include_archived)

    def _legacy_task_views(self, include_archived: bool = False) -> list[TaskView]:
        """Derive task views from the authoritative legacy task + agent files.

        This is the pre-cutover projection: task files cross-referenced against
        agent claim/complete files. It is the source of truth for destructive
        maintenance (archival/aging) regardless of the read flag, because the
        store mirror is best-effort and a claim/complete recorded only in the
        legacy agent file must never be missed by a sweep.
        """
        tasks = self.load_tasks(include_archived=include_archived)
        # Acceptance criteria amendments are governance state, just like
        # dependency amendments. The rollback projection must fold the latest
        # CardStore event without rewriting either immutable birth record. A
        # known but unreadable CardStore card propagates failure so a claim
        # cannot proceed under stale criteria.
        from .card_store import CardStore, current_acceptance_criteria

        criteria_store = CardStore(self.home)
        tasks = [
            task.model_copy(
                update={
                    "acceptance_criteria": current_acceptance_criteria(
                        self.home,
                        task.id,
                        birth_criteria=task.acceptance_criteria,
                        store=criteria_store,
                    )
                }
            )
            for task in tasks
        ]
        # Dependency amendments are governance state, not mutable task-file
        # contents. Apply their append-only fold even when the CardStore read
        # kill switch selects the legacy projection, so rollback cannot reopen a
        # gate that the active projection still enforces.
        try:
            from .card_store import current_dependencies

            tasks = [
                task.model_copy(
                    update={
                        "dependencies": current_dependencies(
                            self.home, task.id, birth_dependencies=task.dependencies
                        )
                    }
                )
                for task in tasks
            ]
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dependency amendment fold failed: %s", exc)
        agents = self.load_agents()

        claimed_map: dict[str, str] = {}
        completed_set: set[str] = set()
        in_progress_set: set[str] = set()

        for ag in agents:
            for tid in ag.completed_tasks:
                completed_set.add(tid)
            for tid in ag.claimed_tasks:
                claimed_map[tid] = ag.agent
            if ag.current_task:
                in_progress_set.add(ag.current_task)
                claimed_map[ag.current_task] = ag.agent

        views: list[TaskView] = []
        for t in tasks:
            if t.id in completed_set:
                status = TaskStatus.DONE
            elif t.id in in_progress_set:
                status = TaskStatus.IN_PROGRESS
            elif t.id in claimed_map:
                status = TaskStatus.CLAIMED
            else:
                status = TaskStatus.OPEN
            views.append(
                TaskView(
                    task=t,
                    status=status,
                    claimed_by=claimed_map.get(t.id),
                )
            )
        return views

    @staticmethod
    def _parse_board_timestamp(raw: object, *, cutoff: datetime) -> datetime:
        """Parse an ISO timestamp; unparseable values count as older than cutoff."""
        try:
            ts = datetime.fromisoformat(str(raw))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except (ValueError, TypeError):
            return cutoff - timedelta(days=1)

    @staticmethod
    def _is_human_held_task(task: Task) -> bool:
        """True when backlog aging must not treat the card as abandoned."""
        tags = {str(tag).lower() for tag in task.tags}
        if "human-gate" in tags:
            return True
        return "[human]" in task.title.lower()

    def archive_done_tasks(
        self,
        older_than_days: int = 14,
        now: Optional[datetime] = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Age done tasks off the active board.

        Status for eligibility prefers the authoritative legacy claim/completion
        projection (Phase 4e). CardStore-only done cards are also eligible, and
        CardStore ``updated_at`` is preferred as the completion age when present.

        Archiving appends to this host's archive index and never mutates the
        task file.

        Args:
            older_than_days: Age threshold in days.
            now: Reference time (defaults to UTC now); injectable for tests.
            dry_run: When True, return the eligible ids without writing.

        Returns:
            list[str]: The task ids archived (or that would be, if dry_run).
        """
        ref = now or datetime.now(timezone.utc)
        cutoff = ref - timedelta(days=older_than_days)
        done_ages: dict[str, object] = {}

        # Authoritative legacy status: a completion recorded only in the legacy
        # agent file must count even when the store mirror is behind.
        for view in self._legacy_task_views():
            if view.status != TaskStatus.DONE:
                continue
            done_ages[view.task.id] = (
                view.task.meta.get("_board_updated_at") or view.task.created_at
            )

        # CardStore may hold done cards with no legacy task file (cutover gap).
        try:
            from . import card_store
            from .card import Column

            if card_store.card_store_read_enabled():
                store = card_store.CardStore(self.home)
                for card in store.list_cards(
                    include_archived=False, degrade_unreadable=True
                ):
                    if getattr(card.kind, "value", card.kind) not in ("task", "epic"):
                        continue
                    if card.status != Column.DONE:
                        continue
                    age = card.updated_at or card.created_at or done_ages.get(card.id)
                    if card.updated_at:
                        done_ages[card.id] = card.updated_at
                    else:
                        done_ages.setdefault(card.id, age)
        except Exception as exc:  # noqa: BLE001
            logger.warning("CardStore archive scan failed: %s", exc)

        eligible: list[str] = []
        for tid, age_raw in done_ages.items():
            ts = self._parse_board_timestamp(age_raw, cutoff=cutoff)
            if ts <= cutoff:
                eligible.append(tid)
        if not dry_run:
            for tid in eligible:
                self.archive_task(tid, by="archive-done")
        return eligible

    def age_stale_open(
        self,
        older_than_days: int = 90,
        now: Optional[datetime] = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Archive ancient unclaimed open tasks (backlog rot).

        Only tasks that are still OPEN on the authoritative legacy projection
        (never claimed, never worked) and whose created_at exceeds
        ``older_than_days`` are eligible. Cards tagged ``human-gate`` or titled
        with ``[HUMAN]`` are skipped so recorded decisions are not archived as
        abandoned backlog.

        Args:
            older_than_days: Age threshold in days.
            now: Reference time (defaults to UTC now); injectable for tests.
            dry_run: When True, return the eligible ids without writing.

        Returns:
            list[str]: The task ids archived (or that would be, if dry_run).
        """
        ref = now or datetime.now(timezone.utc)
        cutoff = ref - timedelta(days=older_than_days)
        eligible: list[str] = []
        # Authoritative legacy status: a claim recorded only in the legacy agent
        # file must protect a task from backlog aging (store may show it open).
        for view in self._legacy_task_views():
            if view.status != TaskStatus.OPEN:
                continue
            if self._is_human_held_task(view.task):
                continue
            ts = self._parse_board_timestamp(view.task.created_at, cutoff=cutoff)
            if ts <= cutoff:
                eligible.append(view.task.id)
        if not dry_run:
            for tid in eligible:
                self.archive_task(tid, by="age-backlog")
        return eligible

    def prune_stale_locks(
        self,
        older_than_days: int = 7,
        now: Optional[datetime] = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Remove idle coordination lock files older than the threshold.

        Lock files are empty flock targets. Stale ones accumulate under
        Syncthing and slow directory scans. Only unlinks by mtime; does not
        touch locks newer than the cutoff.

        Args:
            older_than_days: Keep locks touched more recently than this.
            now: Reference time (defaults to UTC now); injectable for tests.
            dry_run: When True, return paths that would be removed.

        Returns:
            list[str]: Relative lock filenames removed (or that would be).
        """
        ref = now or datetime.now(timezone.utc)
        cutoff_ts = (ref - timedelta(days=older_than_days)).timestamp()
        locks_dir = self.coord_dir / "locks"
        removed: list[str] = []
        if not locks_dir.is_dir():
            return removed
        for path in sorted(locks_dir.glob("*.lock")):
            try:
                if path.stat().st_mtime > cutoff_ts:
                    continue
            except OSError:
                continue
            removed.append(path.name)
            if not dry_run:
                try:
                    path.unlink()
                except OSError as exc:
                    logger.warning("Failed to prune lock %s: %s", path.name, exc)
                    removed.pop()
        return removed

    def _claim_task(
        self, agent_name: str, task_id: str, force: bool = False
    ) -> tuple[AgentFile, str | None]:
        """Have an agent claim a task.

        Args:
            agent_name: The claiming agent's name.
            task_id: The task ID to claim.
            force: Compatibility flag retained for callers. It never bypasses
                dependency, review, or human gates.

        Returns:
            Updated AgentFile.

        Raises:
            ValueError: If task doesn't exist, is owned by another agent in a
                claimed/in_progress/review/done state, or has incomplete
                dependencies.
        """
        views = self.get_task_views()
        target = None
        for v in views:
            if v.task.id == task_id:
                target = v
                break

        if target is None:
            raise ValueError(f"Task {task_id} not found")
        # Kanban moves to ready/doing/review carry no owner, yet the card
        # folds to CLAIMED/IN_PROGRESS/REVIEW with claimed_by None. The
        # lifecycle reconciler only projects claims for cards WITH an owner
        # (lifecycle.py), so an ownerless card in any of those states is
        # unclaimed and claimable. cbca4c17 fixed this for ready; 47e8d509
        # extends the same rule to doing/review (see also 257d6b9a on stale
        # claim/kanban reconciliation). Only the claim gate relaxes: the card
        # still SHOWS as doing/review on the board, so genuine WIP is not
        # masked. OWNED cards in those states keep refusing a different
        # claimant, and DONE refuses ownerless or not.
        ownerless_active = target.claimed_by is None and target.status in (
            TaskStatus.CLAIMED,
            TaskStatus.IN_PROGRESS,
            TaskStatus.REVIEW,
        )
        if (
            target.status
            in (
                TaskStatus.DONE,
                TaskStatus.CLAIMED,
                TaskStatus.IN_PROGRESS,
                TaskStatus.REVIEW,
            )
            and not ownerless_active
            and target.claimed_by != agent_name
        ):
            raise ValueError(
                f"Task {task_id} already {target.status.value} by "
                f"{target.claimed_by or 'unknown owner'}"
            )
        if target.task.dependencies:
            # Dependency statuses are looked up across archived tasks too, so a
            # dependency that was completed and later archived still counts as
            # done. An unknown dependency ID (no card anywhere) is treated as
            # incomplete and fails closed. ``force`` is deliberately not an
            # escape hatch for dependency gates.
            dep_views = {v.task.id: v for v in self.get_task_views(include_archived=True)}
            incomplete = [
                dep_id
                for dep_id in target.task.dependencies
                if dep_id not in dep_views or dep_views[dep_id].status != TaskStatus.DONE
            ]
            if incomplete:
                raise ValueError(
                    f"Task {task_id} has incomplete dependencies: {', '.join(incomplete)}"
                )

        agent = self.load_agent(agent_name) or AgentFile(agent=agent_name)
        # Capture the task being bumped out of current_task: it stays claimed but
        # is no longer in_progress, so legacy derives it as CLAIMED (ready). The
        # store must demote it too, else the cutover read shows it as doing.
        bumped = agent.current_task if agent.current_task not in (None, task_id) else None
        if task_id not in agent.claimed_tasks:
            agent.claimed_tasks.append(task_id)
        agent.current_task = task_id
        agent.state = AgentState.ACTIVE
        self.save_agent(agent)
        return agent, bumped

    def claim_task(self, agent_name: str, task_id: str, force: bool = False) -> AgentFile:
        """Claim under the board lock and every affected card lock.

        A new current task demotes the old current task. Both IDs therefore
        take sorted card locks after the shared board lock, preventing a second
        card from escaping the mutation protocol.
        """
        from .card_store import (
            CardStore,
            card_mutation_lock,
            card_store_write_enabled,
            validate_card_lock_identifier,
        )

        canonical = AgentFile.validate_agent_name(agent_name)
        validate_card_lock_identifier(task_id)
        with _board_mutation_lock(self.home):
            current = self.load_agent(canonical)
            affected = {task_id}
            if current is not None and current.current_task not in (None, task_id):
                affected.add(validate_card_lock_identifier(current.current_task))
            with ExitStack() as stack:
                for card_id in sorted(affected):
                    stack.enter_context(card_mutation_lock(self.home, card_id))
                _, original = self._snapshot_agent_projection(canonical)
                target_snapshot = (
                    CardStore(self.home).fold(task_id) if card_store_write_enabled() else None
                )
                agent, bumped = self._claim_task(canonical, task_id, force)
                transitions = [(task_id, uuid.uuid4().hex)]
                target_claim_revision = uuid.uuid4().hex
                if bumped is not None:
                    transitions.append((bumped, uuid.uuid4().hex))
                try:
                    self._mirror_card_store(
                        "claim",
                        task_id=task_id,
                        agent=canonical,
                        transition_id=transitions[0][1],
                        claim_revision=target_claim_revision,
                    )
                    if bumped is not None:
                        self._mirror_card_store(
                            "demote",
                            task_id=bumped,
                            agent=canonical,
                            transition_id=transitions[1][1],
                        )
                except Exception as exc:
                    expected_states = [(task_id, canonical, "doing")]
                    if bumped is not None:
                        expected_states.append((bumped, canonical, "ready"))
                    if self._store_transitions_are_applied(transitions, expected_states):
                        return agent
                    compensation: list[tuple[str, str]] = []
                    if bumped is not None:
                        try:
                            compensation = self._compensate_partial_claim(
                                task_id=task_id,
                                owner=canonical,
                                claim_revision=target_claim_revision,
                                claim_transition=transitions[0][1],
                                snapshot=target_snapshot,
                            )
                        except Exception as compensation_error:
                            self._recover_store_failure(
                                operation="claim",
                                task_id=task_id,
                                owner=canonical,
                                actor=canonical,
                                original=original,
                                error=compensation_error,
                                transition_ids=transitions + compensation,
                            )
                            raise RuntimeError(
                                "claim compensation failed; board is fail-closed"
                            ) from compensation_error
                    self._recover_store_failure(
                        operation="claim",
                        task_id=task_id,
                        owner=canonical,
                        actor=canonical,
                        original=original,
                        error=exc,
                        transition_ids=transitions + compensation,
                    )
                    raise
                return agent

    def release_claim(
        self,
        owner: str,
        task_id: str,
        actor: str = "",
        expected_claim_revision: str | None = None,
    ) -> bool:
        """Release one exact active claim without completing the task."""
        from .card_store import card_mutation_lock, validate_card_lock_identifier

        canonical_owner = AgentFile.validate_agent_name(owner)
        validate_card_lock_identifier(task_id)
        audit_actor = AgentFile.validate_agent_name(actor or canonical_owner)
        with _board_mutation_lock(self.home), card_mutation_lock(self.home, task_id):
            return self._release_claim_locked(
                canonical_owner,
                task_id,
                actor=audit_actor,
                expected_claim_revision=expected_claim_revision,
            )

    def _release_claim_locked(
        self,
        owner: str,
        task_id: str,
        *,
        actor: str,
        expected_claim_revision: str | None = None,
        allow_missing_card_store: bool = False,
    ) -> bool:
        """Release one exact claim while the board and card locks are held."""
        from .card_store import (
            CardStore,
            card_store_write_enabled,
            current_claim_precondition,
        )

        agent = self.load_agent(owner)
        if agent is None:
            raise ValueError(f"claim owner {owner} not found")
        if task_id not in agent.claimed_tasks and agent.current_task != task_id:
            return False
        current_claim_revision: str | None = None
        card_before = None
        should_mirror = card_store_write_enabled()
        if should_mirror:
            try:
                card_before = CardStore(self.home).fold(task_id)
                current_claim_revision = current_claim_precondition(self.home, task_id, owner)
                releases_conflict = card_before is not None and card_before.owner != owner
                if releases_conflict and (
                    not isinstance(expected_claim_revision, str) or not expected_claim_revision
                ):
                    raise ValueError(
                        f"exact expected claim revision required to release conflict on {task_id}"
                    )
                if (
                    expected_claim_revision is not None
                    and current_claim_revision != expected_claim_revision
                ):
                    raise ValueError(
                        f"claim revision conflict for {task_id}: expected "
                        f"{expected_claim_revision}, current {current_claim_revision}"
                    )
            except ValueError:
                if (
                    not allow_missing_card_store
                    or CardStore(self.home)._load_core(task_id) is not None
                ):
                    raise
                should_mirror = False
        _, original = self._snapshot_agent_projection(owner)
        agent.claimed_tasks = [claimed for claimed in agent.claimed_tasks if claimed != task_id]
        if agent.current_task == task_id:
            agent.current_task = None
        self.save_agent(agent)
        if not should_mirror:
            return True
        transitions = [(task_id, uuid.uuid4().hex)]
        try:
            self._mirror_card_store(
                "release",
                task_id=task_id,
                owner=owner,
                actor=actor,
                expected_claim_revision=current_claim_revision,
                transition_id=transitions[0][1],
            )
            if (
                card_before is None
                or current_claim_revision is None
                or not self._release_transition_is_applied(
                    task_id,
                    transitions[0][1],
                    owner,
                    current_claim_revision,
                    card_before,
                )
            ):
                raise RuntimeError("release transition did not reach its postcondition")
        except Exception as exc:
            if (
                card_before is not None
                and current_claim_revision is not None
                and self._release_transition_is_applied(
                    task_id,
                    transitions[0][1],
                    owner,
                    current_claim_revision,
                    card_before,
                )
            ):
                return True
            self._recover_store_failure(
                operation="release_claim",
                task_id=task_id,
                owner=owner,
                actor=actor,
                original=original,
                error=exc,
                transition_ids=transitions,
            )
            raise
        return True

    def _complete_task(self, agent_name: str, task_id: str) -> AgentFile:
        """Mark a task as completed by an agent.

        Args:
            agent_name: The completing agent's name.
            task_id: The task ID completed.

        Returns:
            Updated AgentFile.
        """
        agent = self.load_agent(agent_name) or AgentFile(agent=agent_name)
        if task_id in agent.claimed_tasks:
            agent.claimed_tasks.remove(task_id)
        if task_id not in agent.completed_tasks:
            agent.completed_tasks.append(task_id)
        if agent.current_task == task_id:
            agent.current_task = agent.claimed_tasks[0] if agent.claimed_tasks else None
        if agent.current_task:
            agent.state = AgentState.ACTIVE
        elif agent.state == AgentState.ACTIVE:
            agent.state = AgentState.IDLE
        self.save_agent(agent)

        return agent

    def _incomplete_completion_dependencies(self, task_id: str) -> list[str]:
        """Return every folded dependency that prevents normal completion.

        Reads archived cards as well as active cards so an archived completed
        dependency remains satisfied. Unknown identifiers and every status
        other than ``done`` remain blocking. The target's dependencies come
        from ``get_task_views`` and therefore include append-only CardStore
        amendments under either projection mode.

        Args:
            task_id: Card whose normal completion is being considered.

        Returns:
            Ordered dependency identifiers that are absent or not done.

        Raises:
            ValueError: If the target card is unknown to the folded board.
        """
        views = self.get_task_views(include_archived=True)
        by_id = {view.task.id: view for view in views}
        target = by_id.get(task_id)
        if target is None:
            raise ValueError(f"Task {task_id} not found")
        return [
            dependency_id
            for dependency_id in target.task.dependencies
            if dependency_id not in by_id or by_id[dependency_id].status != TaskStatus.DONE
        ]

    def complete_task(self, agent_name: str, task_id: str) -> AgentFile:
        """Complete under locks, then mint Joules after those locks are released."""
        from .card_store import card_mutation_lock, validate_card_lock_identifier

        canonical = AgentFile.validate_agent_name(agent_name)
        validate_card_lock_identifier(task_id)
        should_mint = False
        with _board_mutation_lock(self.home), card_mutation_lock(self.home, task_id):
            self._assert_no_claim_conflict(task_id, canonical)
            for existing in self.load_agents():
                if task_id in existing.completed_tasks:
                    return existing
            incomplete = self._incomplete_completion_dependencies(task_id)
            if incomplete:
                raise ValueError(
                    f"Task {task_id} has incomplete dependencies: {', '.join(incomplete)}"
                )
            _, original = self._snapshot_agent_projection(canonical)
            agent = self._complete_task(canonical, task_id)
            transitions = [(task_id, uuid.uuid4().hex)]
            try:
                self._mirror_card_store(
                    "complete",
                    task_id=task_id,
                    agent=canonical,
                    transition_id=transitions[0][1],
                )
            except Exception as exc:
                if self._store_transitions_are_applied(transitions, [(task_id, None, "done")]):
                    should_mint = True
                else:
                    self._recover_store_failure(
                        operation="complete",
                        task_id=task_id,
                        owner=canonical,
                        actor=canonical,
                        original=original,
                        error=exc,
                        transition_ids=transitions,
                    )
                    raise
            else:
                should_mint = True
        if should_mint:
            _mint_joules_for_task(self, task_id, canonical)
        return agent

    def generate_board_md(
        self,
        include_done: bool = False,
        include_idle_agents: bool = False,
    ) -> str:
        """Generate a human-readable BOARD.md from current state.

        Args:
            include_done: When False (default), list live columns only and
                summarize Done as a count so agents are not flooded.
            include_idle_agents: When False (default), list only agents with a
                current task or non-idle state.

        Returns:
            Markdown string for the board overview.
        """
        views = self.get_task_views()
        agents = self.load_agents()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        lines = [
            "# SKCapstone Coordination Board",
            f"*Auto-generated {now} - do not edit manually*",
            "",
        ]

        done_views = [v for v in views if v.status == TaskStatus.DONE]
        for section_status, header in [
            (TaskStatus.IN_PROGRESS, "In Progress"),
            (TaskStatus.CLAIMED, "Claimed"),
            (TaskStatus.OPEN, "Open"),
            (TaskStatus.BLOCKED, "Blocked"),
            (TaskStatus.REVIEW, "Review"),
        ]:
            section_tasks = [v for v in views if v.status == section_status]
            if not section_tasks:
                continue
            lines.append(f"## {header} ({len(section_tasks)})")
            lines.append("")
            for v in section_tasks:
                t = v.task
                assignee = f" @{v.claimed_by}" if v.claimed_by else ""
                priority_icon = {"critical": "!!!", "high": "!!", "medium": "!", "low": ""}.get(
                    t.priority.value, ""
                )
                tags_str = " ".join(f"`{tag}`" for tag in t.tags)
                lines.append(f"- **[{t.id}]** {t.title}{assignee} {priority_icon} {tags_str}")
                if t.description:
                    lines.append(f"  > {t.description[:120]}")
            lines.append("")

        if include_done and done_views:
            lines.append(f"## Done ({len(done_views)})")
            lines.append("")
            for v in done_views:
                t = v.task
                assignee = f" @{v.claimed_by}" if v.claimed_by else ""
                lines.append(f"- **[{t.id}]** {t.title}{assignee}")
            lines.append("")
        elif done_views:
            lines.append(f"## Done ({len(done_views)} hidden)")
            lines.append("")
            lines.append(
                "- Done rows omitted from the default board. "
                "Regenerate with include_done=True or `coord status --include-done`."
            )
            lines.append("")

        if agents:
            shown = agents
            if not include_idle_agents:
                shown = [
                    ag
                    for ag in agents
                    if ag.current_task or ag.state == AgentState.ACTIVE
                ]
            lines.append(f"## Agents ({len(shown)}" + (f" of {len(agents)}" if len(shown) != len(agents) else "") + ")")
            lines.append("")
            for ag in shown:
                state_icon = {"active": "🟢", "idle": "🟡", "offline": "⚫"}.get(
                    ag.state.value, "?"
                )
                current = f" working on `{ag.current_task}`" if ag.current_task else ""
                lines.append(f"- {state_icon} **{ag.agent}** ({ag.host}){current}")
                if ag.notes:
                    lines.append(f"  > {ag.notes[:120]}")
            lines.append("")

        return "\n".join(lines)

    def write_board_md(self, include_done: bool = False) -> Path:
        """Write BOARD.md to the coordination directory.

        Returns:
            Path to the written file.
        """
        self.ensure_dirs()
        content = self.generate_board_md(include_done=include_done)
        path = self.coord_dir / "BOARD.md"
        atomic_write_text(path, content)
        return path


# Priority → (work_category, event_type, joules) mapping for Joule minting
_PRIORITY_JOULE_MAP: dict[str, tuple[str, str, int]] = {
    "critical": ("development", "bug_fix", 500),
    "high": ("development", "code_commit", 100),
    "medium": ("community", "support_ticket", 50),
    "low": ("development", "documentation", 25),
}


def _mint_joules_for_task(board: Board, task_id: str, agent_name: str) -> None:
    """Mint Joules via SKJoule when a task is completed.

    Looks up the task to get priority/title, maps priority to a work
    category and Joule amount, then calls JouleEngine.auto_tokenize_task().
    Failures are silently caught so tokenization never blocks task completion.

    Args:
        board: The Board instance (used to look up task data).
        task_id: ID of the completed task.
        agent_name: Agent who completed the task.
    """
    try:
        from skcapstone.skjoule import JouleEngine

        # Find the task to get its metadata
        task_data: dict[str, object] = {"id": task_id, "completed_by": agent_name}
        for t in board.load_tasks():
            if t.id == task_id:
                task_data.update(t.model_dump())
                break

        priority = str(task_data.get("priority", "medium"))
        category, _event_type, joules = _PRIORITY_JOULE_MAP.get(
            priority, ("community", "support_ticket", 50)
        )

        # Inject tags so auto_tokenize_task picks up the right category
        tags = list(task_data.get("tags", []))  # type: ignore[arg-type]
        if category == "development":
            if "dev" not in tags and "development" not in tags:
                tags.append("dev")
        elif category == "community":
            if "community" not in tags:
                tags.append("community")
        task_data["tags"] = tags

        # Use assignee if available, else fall back to the active workspace agent.
        from skcapstone import active_agent_name

        worker = (
            task_data.get("completed_by")
            or task_data.get("created_by")
            or active_agent_name()
            or "agent"
        )
        task_data["completed_by"] = worker

        engine = JouleEngine()
        record = engine.auto_tokenize_task(task_data)

        if record:
            title = task_data.get("title", task_id)
            print(f"[SKJoule] Minted {record.joules} Joules for task: {title}")
    except Exception as exc:
        # Never let tokenization failure block task completion
        logger.warning("Joule tokenization failed for task %s (non-fatal): %s", task_id, exc)


_BRIEFING_PROTOCOL = """\
# SKCapstone Agent Coordination Protocol

You are an AI agent participating in a multi-agent coordination system.
This protocol works with ANY tool: Cursor, Claude Code, Aider, Windsurf,
Cline, a plain terminal, or anything that can run shell commands.

## Quick Start

1. Check what's available:   skcapstone coord status
2. Claim a task:             skcapstone coord claim <id> --agent <you>
3. Do the work
4. Mark complete:            skcapstone coord complete <id> --agent <you>
5. Create new tasks:         skcapstone coord create --title "..." --by <you>
6. Update the board:         skcapstone coord board
7. Show this protocol:       skcapstone coord briefing
8. Machine-readable:         skcapstone coord briefing --format json

## Directory Layout

All data lives at ~/.skcapstone/coordination/ and syncs via Syncthing.

  ~/.skcapstone/coordination/
  ├── tasks/       # One JSON per task (creator writes once, then immutable)
  ├── agents/      # One JSON per agent (only that agent writes to its own)
  └── BOARD.md     # Human-readable overview (any agent can regenerate)

## Conflict-Free Design

- Each agent ONLY writes to its own file: agents/<your_name>.json
- Task files are write-once by the creator, then immutable
- BOARD.md can be regenerated by anyone from the source JSON files
- Syncthing propagates changes - no SSH, no APIs, no manual relay

## Task JSON Schema

{
  "id": "8-char hex",
  "title": "string",
  "description": "string",
  "priority": "critical|high|medium|low",
  "tags": ["string"],
  "created_by": "agent_name",
  "created_at": "ISO-8601",
  "acceptance_criteria": ["string"],
  "dependencies": ["task_id"],
  "notes": ["string"]
}

## Agent JSON Schema

{
  "agent": "your_name",
  "last_seen": "ISO-8601",
  "host": "hostname",
  "state": "active|idle|offline",
  "current_task": "task_id or null",
  "claimed_tasks": ["task_id"],
  "completed_tasks": ["task_id"],
  "capabilities": ["string"],
  "notes": "freeform text"
}

## Agent Names

- jarvis  - CapAuth, vault sync, crypto, testing
- opus    - Runtime, tokens, documentation, architecture
- lumina  - FEB, memory, trust, emotional intelligence
- human   - When the human creates tasks directly

## Rules

1. Read before you write - always check the board first
2. Own your file - only write to agents/<your_name>.json
3. Tasks are immutable - don't edit task files after creation
4. Claim before working - so others don't duplicate effort
5. Complete when done - move tasks to completed_tasks promptly
6. Create discovered work - if you find something needed, add a task
7. Update BOARD.md - regenerate periodically for human visibility

## Programmatic Access (Python)

    from skcoord.coordination import Board
    board = Board(Path("~/.skcapstone").expanduser())
    tasks = board.get_task_views()      # All tasks with status
    board.claim_task("my_name", "abc1")  # Claim a task
    board.complete_task("my_name", "abc1")  # Complete it

## Integration

The ~/.skcapstone/ directory is synced by Syncthing across all devices.
When you update your agent file or create a task, it propagates to:
- Other AI sessions on the same machine
- Other machines in the Syncthing mesh
- The Docker Swarm cluster (sksync.skstack01.douno.it)
"""


def get_briefing_text(home: Path, include_done: bool = False) -> str:
    """Return the full coordination protocol as plain text.

    Appends a live snapshot of current tasks and agents if the
    coordination directory exists. Done cards are summarized by default.

    Args:
        home: Path to ~/.skcapstone
        include_done: When True, list every done card in the snapshot.

    Returns:
        Protocol text with optional live status appended.
    """
    text = _BRIEFING_PROTOCOL

    board = Board(home)
    views = board.get_task_views()
    agents = board.load_agents()

    if views or agents:
        text += "\n## Current Board Snapshot\n\n"
        live = [v for v in views if v.status != TaskStatus.DONE]
        done = [v for v in views if v.status == TaskStatus.DONE]
        snapshot = list(live)
        if include_done:
            snapshot.extend(done)
        else:
            text += f"  Done cards hidden: {len(done)} (pass include_done=True for full list)\n"
        for v in snapshot:
            status_icon = {
                "open": "[ ]",
                "claimed": "[~]",
                "in_progress": "[>]",
                "review": "[?]",
                "done": "[x]",
            }.get(v.status.value, "[?]")
            text += f"  {status_icon} [{v.task.id}] {v.task.title}"
            if v.claimed_by:
                text += f"  (by {v.claimed_by})"
            text += "\n"

        if agents:
            active = [ag for ag in agents if ag.current_task or ag.state == AgentState.ACTIVE]
            text += f"\n### Active Agents ({len(active)} of {len(agents)})\n\n"
            for ag in active:
                text += f"  - {ag.agent} ({ag.state.value})"
                if ag.current_task:
                    text += f" -> {ag.current_task}"
                text += "\n"

    return text


def get_briefing_json(home: Path, include_done: bool = False) -> str:
    """Return the coordination protocol and live state as JSON.

    Useful for machine consumption by agents that prefer structured data.

    Args:
        home: Path to ~/.skcapstone
        include_done: When True, include done tasks in the tasks array.

    Returns:
        JSON string with protocol info and current board state.
    """
    board = Board(home)
    views = board.get_task_views()
    agents = board.load_agents()
    done_count = sum(1 for v in views if v.status == TaskStatus.DONE)
    if not include_done:
        views = [v for v in views if v.status != TaskStatus.DONE]
    active_agents = [ag for ag in agents if ag.current_task or ag.state == AgentState.ACTIVE]

    payload = {
        "protocol_version": "1.0",
        "coordination_dir": str(board.coord_dir),
        "done_hidden": done_count if not include_done else 0,
        "commands": {
            "status": "skcapstone coord status",
            "create": 'skcapstone coord create --title "..." --by <agent>',
            "claim": "skcapstone coord claim <id> --agent <name>",
            "complete": "skcapstone coord complete <id> --agent <name>",
            "board": "skcapstone coord board",
            "briefing": "skcapstone coord briefing",
            "maintain": "skcapstone coord maintain",
        },
        "rules": [
            "Read before you write",
            "Only write to agents/<your_name>.json",
            "Tasks are immutable after creation",
            "Claim before working",
            "Complete when done",
            "Create discovered work as new tasks",
        ],
        "agent_names": {
            "jarvis": "CapAuth, vault sync, crypto, testing",
            "opus": "Runtime, tokens, docs, architecture",
            "lumina": "FEB, memory, trust, emotional intelligence",
            "human": "Human-created tasks",
        },
        "tasks": [
            {
                "id": v.task.id,
                "title": v.task.title,
                "priority": v.task.priority.value,
                "status": v.status.value,
                "claimed_by": v.claimed_by,
                "tags": v.task.tags,
            }
            for v in views
        ],
        "agents": [
            {
                "name": ag.agent,
                "state": ag.state.value,
                "current_task": ag.current_task,
                "claimed": ag.claimed_tasks,
                "completed": ag.completed_tasks,
            }
            for ag in active_agents
        ],
    }
    return json.dumps(payload, indent=2)
