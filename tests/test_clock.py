"""时钟偏移计算与 server_now 补偿测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from minio_sync.clock import ClockSync
from minio_sync.config import Settings


def _clock(offset: float = 0.0, settings: Settings | None = None) -> ClockSync:
    clock = ClockSync(settings or Settings(secure=False, verify_ssl=False, endpoint="localhost:9000"))
    clock._offset = offset  # noqa: SLF001
    return clock


def test_initial_offset_zero() -> None:
    clock = _clock()
    assert clock.offset == 0.0


def test_server_now_compensates_offset() -> None:
    clock = _clock(offset=120.0)
    now = clock.server_now()
    assert now.tzinfo is not None
    diff = (now - datetime.now(UTC)).total_seconds()
    assert 119.0 < diff < 121.0


def test_detect_offset_uses_date_header(mocker: pytest.MockFixture) -> None:
    server_time = datetime.now(UTC) + timedelta(seconds=60)
    date_str = server_time.strftime("%a, %d %b %Y %H:%M:%S GMT")

    class FakeResp:
        headers = {"Date": date_str}

    fake_pool = mocker.Mock()
    fake_pool.request.return_value = FakeResp()
    mocker.patch("minio_sync.clock.urllib3.PoolManager", return_value=fake_pool)

    clock = _clock(settings=Settings(secure=False, verify_ssl=False, endpoint="localhost:9000"))
    offset = clock.detect_offset()
    assert 55.0 < offset < 65.0
    assert clock.offset == offset


def test_detect_offset_missing_date_header(mocker: pytest.MockFixture) -> None:
    class FakeResp:
        headers = {}

    fake_pool = mocker.Mock()
    fake_pool.request.return_value = FakeResp()
    mocker.patch("minio_sync.clock.urllib3.PoolManager", return_value=fake_pool)

    clock = _clock(settings=Settings(secure=False, verify_ssl=False, endpoint="localhost:9000"))
    assert clock.detect_offset() == 0.0


def test_detect_offset_network_error_returns_zero(mocker: pytest.MockFixture) -> None:
    fake_pool = mocker.Mock()
    fake_pool.request.side_effect = ConnectionError("boom")
    mocker.patch("minio_sync.clock.urllib3.PoolManager", return_value=fake_pool)

    clock = _clock(settings=Settings(secure=False, verify_ssl=False, endpoint="localhost:9000"))
    assert clock.detect_offset() == 0.0


def test_recheck_updates_offset(mocker: pytest.MockFixture) -> None:
    server_time = datetime.now(UTC) + timedelta(seconds=30)
    date_str = server_time.strftime("%a, %d %b %Y %H:%M:%S GMT")

    class FakeResp:
        headers = {"Date": date_str}

    fake_pool = mocker.Mock()
    fake_pool.request.return_value = FakeResp()
    mocker.patch("minio_sync.clock.urllib3.PoolManager", return_value=fake_pool)

    clock = _clock(offset=0.0, settings=Settings(secure=False, verify_ssl=False, endpoint="localhost:9000"))
    clock.recheck()
    assert 25.0 < clock.offset < 35.0


def test_https_uses_cert_required(mocker: pytest.MockFixture) -> None:
    """HTTPS + verify_ssl=True 时必须启用证书验证。"""
    fake_pool = mocker.Mock()
    fake_pool.request.side_effect = ConnectionError("stop")
    pool_manager = mocker.patch("minio_sync.clock.urllib3.PoolManager", return_value=fake_pool)

    clock = _clock(settings=Settings(secure=True, verify_ssl=True, endpoint="cos.example.com"))
    clock.detect_offset()

    pool_kwargs = pool_manager.call_args.kwargs
    assert pool_kwargs["cert_reqs"] == "CERT_REQUIRED"
    assert fake_pool.request.call_args.args[1].startswith("https://")


def test_http_uses_cert_none(mocker: pytest.MockFixture) -> None:
    """HTTP 连接无需证书验证。"""
    fake_pool = mocker.Mock()
    fake_pool.request.side_effect = ConnectionError("stop")
    pool_manager = mocker.patch("minio_sync.clock.urllib3.PoolManager", return_value=fake_pool)

    clock = _clock(settings=Settings(secure=False, verify_ssl=True, endpoint="cos.example.com"))
    clock.detect_offset()

    pool_kwargs = pool_manager.call_args.kwargs
    assert pool_kwargs["cert_reqs"] == "CERT_NONE"
