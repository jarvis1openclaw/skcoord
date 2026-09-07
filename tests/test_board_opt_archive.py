"""Board scan-cost helpers: archive age, live BOARD, lock prune."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from skcoord.coordination import (
    AgentFile,
    AgentState,
    Board,
    Task,
    TaskPriority,
    get_briefing_text,
)


def _write_task(board: Board, task: Task) -> None:
    board.ensure_dirs()
    path = board.tasks_dir / f"{task.id}.json"
    path.write_text(task.model_dump_json(indent=2), encoding="utf-8")


def _write_agent(board: Board, agent: AgentFile) -> None:
    board.ensure_dirs()
    path = board.agents_dir / f"{agent.agent}.json"
    path.write_text(agent.model_dump_json(indent=2), encoding="utf-8")


def test_archive_done_ages_by_board_updated_at(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Completion meta beats created_at for archive eligibility."""
    monkeypatch.setenv("SKCOORD_CARD_STORE", "0")
    board = Board(tmp_path)
    now = datetime(2026, 9, 5, tzinfo=timezone.utc)
    old = Task(
        id="olddone01",
        title="old done",
        created_at=(now - timedelta(days=2)).isoformat(),
        meta={"_board_updated_at": (now - timedelta(days=5)).isoformat()},
    )
    fresh = Task(
        id="freshd02",
        title="fresh done",
        created_at=(now - timedelta(days=20)).isoformat(),
        meta={"_board_updated_at": (now - timedelta(days=1)).isoformat()},
    )
    _write_task(board, old)
    _write_task(board, fresh)
    _write_agent(
        board,
        AgentFile(
            agent="tester",
            state=AgentState.IDLE,
            completed_tasks=["olddone01", "freshd02"],
        ),
    )

    eligible = board.archive_done_tasks(older_than_days=3, now=now, dry_run=True)
    assert eligible == ["olddone01"]


def test_archive_done_falls_back_to_created_at(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKCOORD_CARD_STORE", "0")
    board = Board(tmp_path)
    now = datetime(2026, 9, 5, tzinfo=timezone.utc)
    task = Task(
        id="legacy01",
        title="legacy done",
        created_at=(now - timedelta(days=10)).isoformat(),
    )
    _write_task(board, task)
    _write_agent(
        board,
        AgentFile(agent="tester", state=AgentState.IDLE, completed_tasks=["legacy01"]),
    )
    eligible = board.archive_done_tasks(older_than_days=7, now=now, dry_run=True)
    assert eligible == ["legacy01"]


def test_age_stale_open_skips_human_gate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKCOORD_CARD_STORE", "0")
    board = Board(tmp_path)
    now = datetime(2026, 9, 5, tzinfo=timezone.utc)
    held = Task(
        id="humang01",
        title="[HUMAN] decide something",
        created_at=(now - timedelta(days=120)).isoformat(),
        tags=["human-gate"],
    )
    stale = Task(
        id="stale001",
        title="ancient backlog",
        created_at=(now - timedelta(days=120)).isoformat(),
    )
    _write_task(board, held)
    _write_task(board, stale)
    eligible = board.age_stale_open(older_than_days=90, now=now, dry_run=True)
    assert eligible == ["stale001"]


def test_generate_board_md_hides_done_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKCOORD_CARD_STORE", "0")
    board = Board(tmp_path)
    open_task = Task(id="open0001", title="open work", priority=TaskPriority.HIGH)
    done_task = Task(id="done0001", title="finished work")
    _write_task(board, open_task)
    _write_task(board, done_task)
    _write_agent(
        board,
        AgentFile(agent="worker", state=AgentState.IDLE, completed_tasks=["done0001"]),
    )
    md = board.generate_board_md()
    assert "open work" in md
    assert "finished work" not in md
    assert "Done (1 hidden)" in md
    md_full = board.generate_board_md(include_done=True)
    assert "finished work" in md_full


def test_briefing_hides_done_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKCOORD_CARD_STORE", "0")
    board = Board(tmp_path)
    _write_task(board, Task(id="open0002", title="live card"))
    _write_task(board, Task(id="done0002", title="done card"))
    _write_agent(
        board,
        AgentFile(agent="worker", state=AgentState.IDLE, completed_tasks=["done0002"]),
    )
    text = get_briefing_text(tmp_path)
    assert "live card" in text
    assert "done card" not in text
    assert "Done cards hidden: 1" in text


def test_prune_stale_locks_keeps_fresh(tmp_path: Path) -> None:
    board = Board(tmp_path)
    board.ensure_dirs()
    locks = board.coord_dir / "locks"
    locks.mkdir(parents=True, exist_ok=True)
    stale = locks / "stale.lock"
    fresh = locks / "fresh.lock"
    stale.write_text("", encoding="utf-8")
    fresh.write_text("", encoding="utf-8")
    old = datetime.now(timezone.utc) - timedelta(days=30)
    import os

    os.utime(stale, (old.timestamp(), old.timestamp()))
    removed = board.prune_stale_locks(older_than_days=7, dry_run=False)
    assert "stale.lock" in removed
    assert not stale.exists()
    assert fresh.exists()
