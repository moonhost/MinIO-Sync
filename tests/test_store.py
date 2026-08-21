"""持久化存储读写、格式兼容与异常处理测试。"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from minio_sync.store import SqliteStateStore


def _make_store(tmp_path: Path) -> SqliteStateStore:
    return SqliteStateStore(
        db_path=str(tmp_path / "sync_state.db"),
        initial_sync_time="2024-01-01T00:00:00Z",
    )


def test_last_sync_time_default_when_missing(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    assert store.load_last_sync_time() == datetime(2024, 1, 1, tzinfo=UTC)


def test_last_sync_time_roundtrip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    dt = datetime(2024, 1, 1, 10, 30, tzinfo=UTC)
    store.save_last_sync_time(dt)
    assert store.load_last_sync_time() == dt


def test_failed_files_empty_when_missing(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    assert store.load_failed_files() == []


def test_failed_files_roundtrip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    records = [{"path": "a.jpg", "retry_count": 2}]
    store.save_failed_files(records)
    assert store.load_failed_files() == records


def test_failed_files_add_and_remove(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    store.add_failed_record("a.jpg")
    store.add_failed_record("b.jpg")
    assert store.load_failed_files() == [
        {"path": "a.jpg", "retry_count": 0},
        {"path": "b.jpg", "retry_count": 0},
    ]
    store.remove_failed_record("a.jpg")
    assert store.load_failed_files() == [{"path": "b.jpg", "retry_count": 0}]


def test_pending_queue_roundtrip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    items = [{"path": "a.jpg", "size": 100}]
    store.save_pending_queue(items)
    assert store.load_pending_queue() == items


def test_pending_queue_add_and_remove(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    store.add_pending_item({"path": "a.jpg", "size": 100})
    store.add_pending_item({"path": "b.jpg", "size": 200})
    loaded = store.load_pending_queue()
    assert {item["path"] for item in loaded} == {"a.jpg", "b.jpg"}
    store.remove_pending_item("a.jpg")
    assert store.load_pending_queue() == [{"path": "b.jpg", "size": 200}]


def test_persist_queue_snapshot(tmp_path: Path) -> None:
    import queue

    from minio_sync.store import persist_queue_snapshot

    store = _make_store(tmp_path)
    q: queue.Queue = queue.Queue()
    q.put({"path": "a.jpg", "size": 1})
    q.put({"path": "b.jpg", "size": 2})
    persist_queue_snapshot(q, store)
    loaded = store.load_pending_queue()
    assert {item["path"] for item in loaded} == {"a.jpg", "b.jpg"}
    assert q.qsize() == 2
