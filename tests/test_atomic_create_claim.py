"""Atomic task creation and ownership tests."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, local

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


def test_create_claimed_task_retry_after_new_current_task_is_read_only(tmp_path, monkeypatch):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    board = Board(tmp_path)
    first = Task(id="a1b2c3e3", title="First", created_by="maker")
    second = Task(id="a1b2c3e4", title="Second", created_by="maker")
    board.create_claimed_task(first, "maker")
    board.create_claimed_task(second, "maker")

    def snapshot():
        roots = (
            tmp_path / "cards",
            tmp_path / "coordination" / "tasks",
            tmp_path / "coordination" / "agents",
        )
        return {
            path.relative_to(tmp_path): path.read_bytes()
            for root in roots
            for path in root.rglob("*")
            if path.is_file()
        }

    before = snapshot()
    with pytest.raises(ValueError, match="claim is no longer current"):
        board.create_claimed_task(first, "maker")

    assert snapshot() == before
    agent = board.load_agent("maker")
    assert agent is not None
    assert agent.current_task == second.id
    store = CardStore(tmp_path)
    first_card = store.fold(first.id)
    second_card = store.fold(second.id)
    assert first_card is not None and (first_card.owner, first_card.status.value) == (
        "maker",
        "ready",
    )
    assert second_card is not None and (second_card.owner, second_card.status.value) == (
        "maker",
        "doing",
    )


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("kind", "epic"),
        ("title", "Changed title"),
        ("description", "Changed description"),
        ("created_by", "other-maker"),
        ("created_at", "2026-01-01T00:00:00+00:00"),
        ("acceptance_criteria", ["changed criterion"]),
        ("dependencies", ["ffffffff"]),
        ("initial_priority", "critical"),
        ("initial_swimlane", "expedite"),
        ("initial_labels", ["changed-label"]),
        ("initial_owner", "other-owner"),
        ("initial_claim_revision", "changed-revision"),
        ("meta", {"changed": True}),
    ],
)
def test_create_claimed_task_retry_rejects_any_immutable_mismatch_without_mutation(
    tmp_path, monkeypatch, field, changed
):
    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    board = Board(tmp_path)
    task = Task(id="a1b2c3ef", title="Immutable retry", created_by="maker")
    _, revision = board.create_claimed_task(task, "maker")
    core_path = tmp_path / "cards" / task.id / "core.json"
    legacy_path = next((tmp_path / "coordination" / "tasks").glob(f"{task.id}-*.json"))
    agent_path = tmp_path / "coordination" / "agents" / "maker.json"
    before = {path: path.read_bytes() for path in (core_path, legacy_path, agent_path)}

    from skcoord.card_store import mirror_coord_create_claimed

    if field in {
        "kind",
        "initial_priority",
        "initial_swimlane",
        "initial_labels",
        "initial_owner",
        "initial_claim_revision",
        "dependencies",
    }:
        core = CardStore(tmp_path)._load_core(task.id)
        assert core is not None
        core[field] = changed
        core_path.write_text(__import__("json").dumps(core), encoding="utf-8")
        mismatched_before = core_path.read_bytes()
        with pytest.raises(ValueError, match="create-and-claim conflict"):
            mirror_coord_create_claimed(tmp_path, task, "maker", revision)
        assert core_path.read_bytes() == mismatched_before
        assert legacy_path.read_bytes() == before[legacy_path]
        assert agent_path.read_bytes() == before[agent_path]
        return

    changed_task = task.model_copy(update={field: changed})
    with pytest.raises(ValueError, match="create-and-claim conflict"):
        board.create_claimed_task(changed_task, "maker")

    assert {path: path.read_bytes() for path in before} == before


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


def test_concurrent_same_owner_different_payload_has_one_winner(tmp_path, monkeypatch):
    from skcoord.card_store import mirror_coord_create_claimed

    monkeypatch.setenv("SKCOORD_CARD_STORE", "1")
    first_reads = Barrier(2)
    thread_state = local()
    real_load = CardStore._load_core

    def synchronized_first_load(store, card_id):
        if not getattr(thread_state, "read", False):
            thread_state.read = True
            first_reads.wait(2)
            return None
        return real_load(store, card_id)

    monkeypatch.setattr(CardStore, "_load_core", synchronized_first_load)
    tasks = (
        Task(id="a1b2c3ee", title="Race", description="one", created_by="maker"),
        Task(id="a1b2c3ee", title="Race", description="two", created_by="maker"),
    )

    def create(task):
        try:
            return mirror_coord_create_claimed(tmp_path, task, "maker")
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, tasks))

    assert sum(result is not None for result in results) == 1
    monkeypatch.setattr(CardStore, "_load_core", real_load)
    assert CardStore(tmp_path).fold("a1b2c3ee").description in {"one", "two"}


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


def test_create_claimed_task_rejects_incomplete_dependency_before_write(tmp_path, monkeypatch):
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
