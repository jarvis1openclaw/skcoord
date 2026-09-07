"""Atomic task creation and ownership tests."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from skcoord.card_store import CardStore
from skcoord.coordination import Board, Task


def test_create_claimed_task_is_owned_on_first_fold(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    board = Board(tmp_path)
    task = Task(id="a1b2c3d4", title="Atomic owner", created_by="maker")

    path, revision = board.create_claimed_task(task, "maker")

    card = CardStore(tmp_path).fold(task.id)
    assert path.exists()
    assert card is not None
    assert (card.owner, card.status.value) == ("maker", "doing")
    assert card.meta["_claim_revision"] == revision


def test_create_claimed_task_retry_returns_same_revision(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    board = Board(tmp_path)
    task = Task(id="a1b2c3d5", title="Retry", created_by="maker")

    first = board.create_claimed_task(task, "maker")
    second = board.create_claimed_task(task, "maker")

    assert second[1] == first[1]


def test_create_claimed_task_concurrent_other_owner_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    task = Task(id="a1b2c3d6", title="Contended", created_by="maker")

    def create(owner):
        try:
            return Board(tmp_path).create_claimed_task(task, owner)[1]
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, ("one", "two")))

    assert sum(result is not None for result in results) == 1
    card = CardStore(tmp_path).fold(task.id)
    assert card is not None
    assert card.owner in {"one", "two"}
    assert card.meta["_claim_revision"] in results


def test_create_claimed_task_concurrent_retry_converges(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    task = Task(id="a1b2c3d8", title="Concurrent retry", created_by="maker")

    with ThreadPoolExecutor(max_workers=2) as pool:
        revisions = list(
            pool.map(lambda _: Board(tmp_path).create_claimed_task(task, "maker")[1], range(2))
        )

    assert revisions[0] == revisions[1]


def test_selector_cannot_claim_during_create_projection_window(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    task = Task(id="a1b2c3da", title="Selector race", created_by="maker")
    core_visible = Event()
    let_creator_finish = Event()
    from skcoord import coordination

    real_write = coordination.atomic_write_text

    def paused_write(path, content):
        if path.parent.name == "tasks":
            core_visible.set()
            assert let_creator_finish.wait(2)
        return real_write(path, content)

    monkeypatch.setattr(coordination, "atomic_write_text", paused_write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        creator = pool.submit(Board(tmp_path).create_claimed_task, task, "maker")
        assert core_visible.wait(2)
        attacker = pool.submit(Board(tmp_path).claim_task, "attacker", task.id)
        let_creator_finish.set()
        creator.result()
        with pytest.raises(ValueError, match="already in_progress by maker"):
            attacker.result()


def test_create_claimed_task_requires_cardstore(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "0")
    task = Task(id="a1b2c3d7", title="No fallback", created_by="maker")

    with pytest.raises(ValueError, match="requires the CardStore"):
        Board(tmp_path).create_claimed_task(task, "maker")

    assert not (tmp_path / "coordination" / "tasks").exists()


def test_create_claimed_task_rejects_incomplete_dependency_before_write(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    task = Task(
        id="a1b2c3d9",
        title="Blocked",
        created_by="maker",
        dependencies=["ffffffff"],
    )

    with pytest.raises(ValueError, match="incomplete dependencies"):
        Board(tmp_path).create_claimed_task(task, "maker")

    assert CardStore(tmp_path).fold(task.id) is None
