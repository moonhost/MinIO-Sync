"""下载器：文件跳过逻辑、校验失败、临时文件清理、批量下载与熔断测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from minio_sync.circuit_breaker import CircuitBreaker
from minio_sync.clock import ClockSync
from minio_sync.downloader import batch_download, download_file, list_safe_new_files
from tests.conftest import FakeMinioClient, FakeObject

UTC = UTC


def _clock(offset: float = 0.0) -> ClockSync:
    clock = ClockSync.__new__(ClockSync)
    clock._offset = offset  # noqa: SLF001
    return clock


def _breaker(max_fail: int = 3) -> CircuitBreaker:
    return CircuitBreaker(max_fail=max_fail, recovery_seconds=60)


def _patch_client(mocker: pytest.MockFixture, client: FakeMinioClient) -> None:
    mocker.patch("minio_sync.downloader.get_thread_minio_client", return_value=client)


def test_list_safe_new_files_filters_by_time_window(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    now = datetime.now(UTC)
    # safe_time = now - delta_seconds；需满足 last_sync_time < obj_time < safe_time
    fake_client.objects = [
        FakeObject("old.jpg", 10, now - timedelta(hours=2)),  # 早于 last_sync_time
        FakeObject("new.jpg", 20, now - timedelta(seconds=40)),  # 在窗口内
        FakeObject("too_new.jpg", 30, now - timedelta(seconds=10)),  # 晚于 safe_time
        FakeObject("no_time.jpg", 40, None),
    ]
    result = list_safe_new_files(
        fake_client, settings, _clock(), settings.bucket, last_sync_time=now - timedelta(hours=1)
    )
    paths = {item["path"] for item in result}
    assert paths == {"new.jpg"}
    assert result[0]["size"] == 20


def test_download_skips_existing_complete_file(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    _patch_client(mocker, fake_client)
    local = settings.local_sync_path
    import os

    os.makedirs(local, exist_ok=True)
    target = os.path.join(local, "a.jpg")
    with open(target, "wb") as f:
        f.write(b"x" * 100)
    breaker = _breaker()
    ok, path = download_file(settings, breaker, settings.bucket, "a.jpg", expected_size=100)
    assert ok
    assert path == "a.jpg"
    assert fake_client.downloaded == []


def test_download_success_and_breaker_success(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    import os

    fake_client.objects = [FakeObject("a.jpg", 100, datetime.now(UTC))]
    _patch_client(mocker, fake_client)
    breaker = _breaker()
    breaker.record_failure()
    ok, _ = download_file(settings, breaker, settings.bucket, "a.jpg", expected_size=100)
    assert ok
    assert breaker.continuous_fail == 0
    expected_dest = os.path.join(settings.local_sync_path, "a.jpg.downloading")
    assert fake_client.downloaded == [(settings.bucket, "a.jpg", expected_dest)]


def test_download_size_mismatch_fails_and_cleans_tmp(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    fake_client.objects = [FakeObject("a.jpg", 100, datetime.now(UTC))]
    _patch_client(mocker, fake_client)
    breaker = _breaker()
    ok, _ = download_file(settings, breaker, settings.bucket, "a.jpg", expected_size=200)
    assert not ok
    assert breaker.continuous_fail == 1
    import os

    assert not os.path.exists(settings.local_sync_path + "/a.jpg.downloading")


def test_download_network_error_fails_and_cleans_tmp(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    fake_client.objects = [FakeObject("a.jpg", 100, datetime.now(UTC))]
    fake_client.fail_paths = {"a.jpg"}
    _patch_client(mocker, fake_client)
    breaker = _breaker()
    ok, _ = download_file(settings, breaker, settings.bucket, "a.jpg", expected_size=100)
    assert not ok
    assert breaker.continuous_fail == 1


def test_batch_download_all_success(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    fake_client.objects = [
        FakeObject("a.jpg", 10, datetime.now(UTC)),
        FakeObject("b.jpg", 20, datetime.now(UTC)),
    ]
    _patch_client(mocker, fake_client)
    breaker = _breaker()
    infos = [{"path": "a.jpg", "size": 10}, {"path": "b.jpg", "size": 20}]
    success, failed, fuse = batch_download(settings, breaker, infos)
    assert set(success) == {"a.jpg", "b.jpg"}
    assert failed == []
    assert not fuse


def test_batch_download_partial_failure(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    fake_client.objects = [
        FakeObject("a.jpg", 10, datetime.now(UTC)),
        FakeObject("b.jpg", 20, datetime.now(UTC)),
    ]
    fake_client.fail_paths = {"b.jpg"}
    _patch_client(mocker, fake_client)
    breaker = _breaker(max_fail=5)
    infos = [{"path": "a.jpg", "size": 10}, {"path": "b.jpg", "size": 20}]
    success, failed, fuse = batch_download(settings, breaker, infos)
    assert "a.jpg" in success
    assert "b.jpg" in failed
    assert not fuse


def test_batch_download_triggers_fuse(
    settings, fake_client: FakeMinioClient, mocker: pytest.MockFixture
) -> None:
    fake_client.objects = [
        FakeObject("a.jpg", 10, datetime.now(UTC)),
        FakeObject("b.jpg", 20, datetime.now(UTC)),
    ]
    fake_client.fail_all = True
    _patch_client(mocker, fake_client)
    breaker = _breaker(max_fail=2)
    infos = [{"path": "a.jpg", "size": 10}, {"path": "b.jpg", "size": 20}]
    success, failed, fuse = batch_download(settings, breaker, infos)
    assert success == []
    assert len(failed) >= 2
    assert fuse
    assert breaker.is_open
