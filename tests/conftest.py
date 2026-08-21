"""pytest 共享 fixture：临时目录、配置、MinIO 客户端 mock。"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from minio_sync.config import Settings
from minio_sync.store import SqliteStateStore


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """指向临时目录的测试配置（不读取真实 .env）。"""
    return Settings(
        endpoint="localhost:9000",
        access_key="test-access",
        secret_key="test-secret",
        bucket="test-bucket",
        secure=False,
        verify_ssl=False,
        local_sync_path=str(tmp_path / "local"),
        state_db_file=str(tmp_path / "sync_state.db"),
        sync_interval=1,
        delta_seconds=30,
        max_threads=2,
        max_continuous_fail=3,
        fuse_recovery_seconds=60,
        max_retry_per_file=2,
        sync_prefix="",
        initial_sync_time="2024-01-01T00:00:00Z",
        reconciliation_interval=1800,
        event_reconnect_delay=1,
        max_queue_size=100,
        clock_offset_recheck_interval=3600,
        log_level="WARNING",
        log_file="",
        log_max_bytes=1024,
        log_backup_count=1,
    )


@pytest.fixture
def store(settings: Settings) -> SqliteStateStore:
    return SqliteStateStore(
        db_path=settings.state_db_file,
        initial_sync_time=settings.initial_sync_time,
    )


class FakeObject:
    """模拟 minio Object 的轻量对象。"""

    def __init__(
        self,
        object_name: str,
        size: int,
        last_modified: datetime | None,
        etag: str | None = None,
    ) -> None:
        self.object_name = object_name
        self.size = size
        self.last_modified = last_modified
        self.etag = etag


class FakeMinioClient:
    """MinIO 客户端 mock：可配置对象列表、下载行为与失败模式。"""

    def __init__(self, objects: list[FakeObject] | None = None) -> None:
        self.objects = objects or []
        self.downloaded: list[tuple[str, str, str]] = []  # (bucket, path, dest)
        self.fail_paths: set[str] = set()
        self.fail_all = False
        self.fail_stat: set[str] = set()
        self.removed: list[str] = []

    def list_objects(self, bucket: str, prefix: str | None = None, recursive: bool = False) -> list[FakeObject]:
        if prefix:
            return [o for o in self.objects if o.object_name.startswith(prefix)]
        return list(self.objects)

    def stat_object(self, bucket: str, file_path: str) -> FakeObject:
        if file_path in self.fail_stat:
            raise ConnectionError(f"stat failed: {file_path}")
        for obj in self.objects:
            if obj.object_name == file_path:
                return obj
        raise ConnectionError(f"not found: {file_path}")

    def fget_object(self, bucket: str, file_path: str, file_path_local: str) -> None:
        if self.fail_all or file_path in self.fail_paths:
            raise ConnectionError(f"download failed: {file_path}")
        Path(file_path_local).parent.mkdir(parents=True, exist_ok=True)
        obj = self.stat_object(bucket, file_path)
        Path(file_path_local).write_bytes(b"x" * obj.size)
        self.downloaded.append((bucket, file_path, file_path_local))

    def remove_object(self, bucket: str, file_path: str) -> None:
        self.removed.append(file_path)

    def listen_bucket_notification(
        self, bucket: str, prefix: str = "", events: list[str] | None = None
    ) -> list[dict[str, Any]]:
        return []


@pytest.fixture
def fake_client() -> FakeMinioClient:
    return FakeMinioClient()


@pytest.fixture
def utc_now() -> datetime:
    return datetime.now(UTC)
