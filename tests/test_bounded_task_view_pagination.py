from __future__ import annotations

import base64
import bisect
import hashlib
import hmac
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from skcoord.card import Card, Column, Kind
from skcoord.card_store import (
    _TASK_VIEW_CURSOR_MAX_ENCODED_BYTES,
    CardCore,
    CardStore,
    _task_view_cursor,
    _task_view_cursor_position,
)
from skcoord.coordination import Board, TaskViewReadBatch, TaskViewReadScope


class OwnerIndex:
    """Synthetic authorization-owner index with bounded keyset reads."""

    def __init__(self, card_ids: tuple[str, ...], scope: str = "public-synthetic:tenant-a"):
        self.card_ids = card_ids
        self.scope = scope
        self.reads: list[tuple[str | None, int]] = []
        self._refresh_state()

    def _refresh_state(self) -> None:
        body = json.dumps(self.card_ids, separators=(",", ":")).encode()
        self.population_state = hashlib.sha256(body).hexdigest()

    def replace(self, card_ids: tuple[str, ...]) -> None:
        self.card_ids = card_ids
        self._refresh_state()

    def read_page(self, after: str | None, count: int) -> TaskViewReadBatch:
        self.reads.append((after, count))
        start = 0 if after is None else bisect.bisect_right(self.card_ids, after)
        return TaskViewReadBatch(self.card_ids[start : start + count], self.population_state)

    def read_scope(self) -> TaskViewReadScope:
        return TaskViewReadScope(
            authorization_scope=self.scope,
            read_page=self.read_page,
        )


class TenThousandOwnerIndex:
    """10,000-record owner whose page read constructs only requested IDs."""

    population_state = "public-synthetic-exact-state-10000"

    def __init__(self) -> None:
        self.generated = 0
        self.reads: list[tuple[str | None, int]] = []

    def read_page(self, after: str | None, count: int) -> TaskViewReadBatch:
        self.reads.append((after, count))
        start = 0 if after is None else int(after.rsplit("-", 1)[1]) + 1
        stop = min(start + count, 10_000)
        card_ids = tuple(f"public-{index:05d}" for index in range(start, stop))
        self.generated += len(card_ids)
        return TaskViewReadBatch(card_ids, self.population_state)


def _card(card_id: str, *, archived: bool = False, kind: Kind = Kind.TASK) -> Card:
    return Card(
        id=card_id,
        kind=kind,
        title=f"Public synthetic {card_id}",
        status=Column.BACKLOG,
        swimlane="feature",
        archived=archived,
        source="cards",
    )


def test_ten_thousand_record_sentinel_touches_only_two_owner_records(tmp_path: Path) -> None:
    owner = TenThousandOwnerIndex()
    scope = TaskViewReadScope(
        authorization_scope="public-synthetic:tenant-a",
        read_page=owner.read_page,
    )
    folds: list[str] = []

    def fold(_store, card_id):
        folds.append(card_id)
        return _card(card_id)

    with patch.object(CardStore, "fold", fold), patch.object(
        CardStore,
        "list_card_ids",
        side_effect=AssertionError("CardStore enumeration is forbidden"),
    ), patch.object(
        CardStore,
        "list_cards",
        side_effect=AssertionError("complete CardStore fold is forbidden"),
    ):
        page = Board(tmp_path).get_task_view_page(scope, limit=1)

    assert [item.task.id for item in page.items] == ["public-00000"]
    assert owner.reads == [(None, 2)]
    assert owner.generated == 2
    assert folds == ["public-00000", "public-00001"]
    assert page.eligible_records_touched == 2
    assert page.has_more is True
    assert page.next_cursor


def test_pages_are_deterministic_without_gaps_or_overlap(tmp_path: Path) -> None:
    card_ids = tuple(f"public-{index:03d}" for index in range(23))
    owner = OwnerIndex(card_ids)
    board = Board(tmp_path)
    seen: list[str] = []
    cursor = None
    touches = []

    with patch.object(CardStore, "fold", lambda _store, card_id: _card(card_id)):
        while True:
            page = board.get_task_view_page(owner.read_scope(), limit=5, cursor=cursor)
            seen.extend(item.task.id for item in page.items)
            touches.append(page.eligible_records_touched)
            cursor = page.next_cursor
            if not page.has_more:
                break

    assert seen == list(card_ids)
    assert len(seen) == len(set(seen))
    assert touches == [6, 6, 6, 6, 3]
    assert owner.reads == [
        (None, 6),
        ("public-004", 6),
        ("public-009", 6),
        ("public-014", 6),
        ("public-019", 6),
    ]


@pytest.mark.parametrize("scope_value", ["a" * 256, "\U0010ffff" * 256])
def test_maximum_ascii_and_unicode_scopes_round_trip_without_gaps(
    tmp_path: Path, scope_value: str
) -> None:
    card_ids = ("public-000", "public-001", "public-002")
    owner = OwnerIndex(card_ids, scope=scope_value)
    board = Board(tmp_path)
    cursor = None
    seen = []

    with patch.object(CardStore, "fold", lambda _store, card_id: _card(card_id)):
        while True:
            page = board.get_task_view_page(owner.read_scope(), limit=1, cursor=cursor)
            seen.extend(item.task.id for item in page.items)
            cursor = page.next_cursor
            if not page.has_more:
                break

    assert seen == list(card_ids)
    assert owner.reads == [(None, 2), ("public-000", 2), ("public-001", 2)]


def test_one_over_scope_limit_is_rejected() -> None:
    with pytest.raises(ValueError, match="1 to 256 characters.*1024 UTF-8 bytes"):
        TaskViewReadScope("\U0010ffff" * 257, lambda _after, _count: None)


def test_exact_encoded_cursor_limit_round_trips_and_one_over_is_rejected() -> None:
    maximum_unicode = "\U0010ffff"
    scope = maximum_unicode * 256
    cursor = _task_view_cursor(
        {
            "after": maximum_unicode * 128,
            "archived": False,
            "limit": 200,
            "population": maximum_unicode * 256,
            "scope": scope,
            "v": 2,
        }
    )

    assert len(cursor.encode("ascii")) == _TASK_VIEW_CURSOR_MAX_ENCODED_BYTES
    assert _task_view_cursor_position(
        cursor, scope=scope, limit=200, include_archived=False
    ) == (maximum_unicode * 128, maximum_unicode * 256)
    with pytest.raises(ValueError, match="malformed"):
        _task_view_cursor_position(
            cursor + "A", scope=scope, limit=200, include_archived=False
        )


def test_cursor_rejects_forgery_malformed_restart_and_rescope(tmp_path: Path) -> None:
    owner = OwnerIndex(("public-000", "public-001", "public-002"))
    board = Board(tmp_path)
    with patch.object(CardStore, "fold", lambda _store, card_id: _card(card_id)):
        first = board.get_task_view_page(owner.read_scope(), limit=1)
        assert first.next_cursor and "public-000" not in first.next_cursor

        raw = base64.urlsafe_b64decode(first.next_cursor + "=" * (-len(first.next_cursor) % 4))
        payload = json.loads(raw[:-32])
        payload["after"] = "public-001"
        forged_body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        forged = base64.urlsafe_b64encode(
            forged_body + hashlib.sha256(forged_body).digest()
        ).decode().rstrip("=")
        payload["after"] = "public-999"
        tampered_body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        wrong_secret = base64.urlsafe_b64encode(
            tampered_body + hmac.digest(b"attacker-known-key", tampered_body, "sha256")
        ).decode().rstrip("=")

        other_owner = OwnerIndex(owner.card_ids, scope="public-synthetic:tenant-b")
        for scope, cursor, limit in (
            (owner.read_scope(), "not-a-cursor", 1),
            (owner.read_scope(), forged, 1),
            (owner.read_scope(), wrong_secret, 1),
            (other_owner.read_scope(), first.next_cursor, 1),
            (owner.read_scope(), first.next_cursor, 2),
        ):
            with pytest.raises(ValueError, match="cursor"):
                board.get_task_view_page(scope, limit=limit, cursor=cursor)

        with patch("skcoord.card_store._TASK_VIEW_CURSOR_SECRET", b"r" * 32):
            with pytest.raises(ValueError, match="cursor"):
                board.get_task_view_page(owner.read_scope(), limit=1, cursor=first.next_cursor)


@pytest.mark.parametrize(
    "changed",
    [
        ("public-000", "public-001a", "public-002"),
        ("public-000", "public-002"),
        ("public-000", "public-001", "public-002", "public-003"),
    ],
)
def test_cursor_rejects_exact_population_change_without_caller_revision(
    tmp_path: Path, changed: tuple[str, ...]
) -> None:
    owner = OwnerIndex(("public-000", "public-001", "public-002"))
    board = Board(tmp_path)
    folded: list[str] = []

    def fold(_store, card_id):
        folded.append(card_id)
        return _card(card_id)

    with patch.object(CardStore, "fold", fold):
        first = board.get_task_view_page(owner.read_scope(), limit=1)
        owner.replace(changed)
        with pytest.raises(ValueError, match="population is stale"):
            board.get_task_view_page(owner.read_scope(), limit=1, cursor=first.next_cursor)

    assert folded == ["public-000", "public-001"]


def test_stale_record_unstable_owner_and_invalid_limits_fail_closed(tmp_path: Path) -> None:
    board = Board(tmp_path)
    owner = OwnerIndex(("public-000", "public-001"))
    with patch.object(CardStore, "fold", return_value=None):
        with pytest.raises(ValueError, match="stale"):
            board.get_task_view_page(owner.read_scope(), limit=1)

    unstable = TaskViewReadScope(
        authorization_scope="public-synthetic:tenant-a",
        read_page=lambda _after, _count: TaskViewReadBatch(
            ("public-001", "public-000"), "exact-state"
        ),
    )
    with patch.object(CardStore, "fold", lambda _store, card_id: _card(card_id)):
        with pytest.raises(ValueError, match="unstable order"):
            board.get_task_view_page(unstable, limit=1)

    for limit in (0, 201, True, 1.5):
        with pytest.raises(ValueError, match="between 1 and 200"):
            board.get_task_view_page(owner.read_scope(), limit=limit)


def test_one_unreadable_card_does_not_abort_bounded_estate_scan(tmp_path: Path) -> None:
    owner = OwnerIndex(("public-000", "public-001", "public-002"))

    def fold(_store, card_id):
        if card_id == "public-001":
            raise ValueError("CardStore event chain broken")
        return _card(card_id)

    with patch.object(CardStore, "fold", fold):
        page = Board(tmp_path).get_task_view_page(owner.read_scope(), limit=2)

    assert [item.task.id for item in page.items] == ["public-000", "public-001"]
    unreadable = page.items[1].task
    assert unreadable.meta == {
        "unreadable": True,
        "source": "cards/public-001",
        "reason": "CardStore event chain broken",
    }
    assert unreadable.tags == ["unreadable"]
    assert page.has_more is True
    assert page.next_cursor
    assert page.eligible_records_touched == 3


def test_broken_prev_hash_is_degraded_without_changing_event_bytes(tmp_path: Path) -> None:
    store = CardStore(tmp_path)
    card_ids = ("public-000", "public-001", "public-002")
    for card_id in card_ids:
        store.create(CardCore(id=card_id, title=f"Card {card_id}"))
    store.append_event("public-001", "note", "fixture", text="first")
    store.append_event("public-001", "note", "fixture", text="second")

    event_path = next((store.cards_dir / "public-001" / "events").glob("*.jsonl"))
    lines = event_path.read_bytes().splitlines(keepends=True)
    second_event = json.loads(lines[1])
    declared_prev_hash = second_event["prev_hash"].encode("ascii")
    assert len(declared_prev_hash) == 64
    lines[1] = lines[1].replace(declared_prev_hash, b"0" * 64, 1)
    event_path.write_bytes(b"".join(lines))
    corrupt_bytes = event_path.read_bytes()

    with pytest.raises(ValueError, match="public-001.*prev_hash mismatch"):
        store.fold("public-001")

    page = Board(tmp_path).get_task_view_page(OwnerIndex(card_ids).read_scope(), limit=2)

    assert [item.task.id for item in page.items] == ["public-000", "public-001"]
    assert page.items[1].task.meta["unreadable"] is True
    assert "prev_hash mismatch" in page.items[1].task.meta["reason"]
    assert event_path.read_bytes() == corrupt_bytes
    assert page.has_more is True
    assert page.eligible_records_touched == 3


def test_owner_cannot_exceed_limit_plus_one(tmp_path: Path) -> None:
    scope = TaskViewReadScope(
        authorization_scope="public-synthetic:tenant-a",
        read_page=lambda _after, count: TaskViewReadBatch(
            tuple(f"public-{index:03d}" for index in range(count + 1)), "exact-state"
        ),
    )
    with pytest.raises(ValueError, match="bounded request"):
        Board(tmp_path).get_task_view_page(scope, limit=1)


def test_unpaginated_callers_keep_existing_list_contract(tmp_path: Path) -> None:
    expected = [_card("public-000"), _card("public-001")]
    with patch.object(CardStore, "list_cards", return_value=expected):
        views = Board(tmp_path).get_task_views()
    assert isinstance(views, list)
    assert [view.task.id for view in views] == ["public-000", "public-001"]
