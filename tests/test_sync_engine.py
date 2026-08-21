"""同步引擎核心逻辑测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from minio_sync.circuit_breaker import CircuitBreaker
from minio_sync.clock import ClockSync
from minio_sync.sync_engine import SyncEngine, SyncState
from tests.conftest import FakeMinioClient, FakeObject

UTC = UTC


def _make_engine(settings, store, fake_client, mocker) -> SyncEngine:
    breaker = CircuitBreaker(settings.max_continuous_fail, settings.fuse_recovery_seconds)
    clock = ClockSync.__new__(ClockSync)
    clock._offset = 0.0  # noqa: SLF001
    mocker.patch("minio_sync.sync_engine.get_minio_client", return_value=fake_client)
    mocker.patch("minio_sync.downloader.get_thread_minio_client", return_value=fake_client)
    state = SyncState(settings.max_queue_size)
    return SyncEngine(settings, store, breaker, clock, state)


def test_run_once_downloads_new_files_and_advances_timestamp(
    settings, store, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    now = datetime.now(UTC)
    # 对象需早于 safe_time（now - delta_seconds）
    fake_client.objects = [
        FakeObject("a.jpg", 10, now - timedelta(seconds=40)),
        FakeObject("b.jpg", 20, now - timedelta(seconds=50)),
    ]
    engine = _make_engine(settings, store, fake_client, mocker)
    engine.run_once()

    assert {p for _, p, _ in fake_client.downloaded} == {"a.jpg", "b.jpg"}
    # 时间戳应推进到 safe_time（当前时间 - delta）
    last_sync = store.load_last_sync_time()
    assert last_sync > now - timedelta(seconds=settings.delta_seconds + 5)
    # 无失败记录
    assert store.load_failed_files() == []


def test_run_once_retries_failed_files(
    settings, store, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    now = datetime.now(UTC)
    fake_client.objects = [
        FakeObject("a.jpg", 10, now - timedelta(seconds=40)),
        FakeObject("b.jpg", 20, now - timedelta(seconds=50)),
    ]
    store.save_failed_files([{"path": "b.jpg", "retry_count": 1}])
    engine = _make_engine(settings, store, fake_client, mocker)
    engine.run_once()

    # b.jpg 在失败列表中，重试成功后应从失败记录移除
    assert store.load_failed_files() == []
    assert "b.jpg" in {p for _, p, _ in fake_client.downloaded}


def test_run_once_records_new_failures(
    settings, store, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    now = datetime.now(UTC)
    fake_client.objects = [
        FakeObject("a.jpg", 10, now - timedelta(seconds=40)),
        FakeObject("b.jpg", 20, now - timedelta(seconds=50)),
    ]
    fake_client.fail_paths = {"b.jpg"}
    engine = _make_engine(settings, store, fake_client, mocker)
    engine.run_once()

    failed = store.load_failed_files()
    assert {r["path"] for r in failed} == {"b.jpg"}
    assert failed[0]["retry_count"] == 1


def test_run_once_skips_when_lock_held(
    settings, store, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    now = datetime.now(UTC)
    fake_client.objects = [FakeObject("a.jpg", 10, now - timedelta(seconds=40))]
    engine = _make_engine(settings, store, fake_client, mocker)
    engine.state.sync_lock.acquire()
    try:
        engine.run_once()
    finally:
        engine.state.sync_lock.release()
    assert fake_client.downloaded == []


def test_run_once_abandons_over_retry_files(
    settings, store, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    now = datetime.now(UTC)
    fake_client.objects = [FakeObject("a.jpg", 10, now - timedelta(seconds=40))]
    # 超过最大重试次数且本地存在 → 放弃
    import os

    os.makedirs(settings.local_sync_path, exist_ok=True)
    with open(os.path.join(settings.local_sync_path, "b.jpg"), "wb") as f:
        f.write(b"x" * 5)
    store.save_failed_files([{"path": "b.jpg", "retry_count": settings.max_retry_per_file}])
    engine = _make_engine(settings, store, fake_client, mocker)
    engine.run_once()
    # b.jpg 仍保留在失败记录中（被放弃）
    assert {r["path"] for r in store.load_failed_files()} == {"b.jpg"}
