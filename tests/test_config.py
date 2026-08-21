"""配置加载、环境变量覆盖与校验测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from minio_sync.config import Settings
from minio_sync.exceptions import ConfigError


def test_defaults() -> None:
    settings = Settings()
    assert settings.endpoint == ""
    assert settings.bucket == ""
    assert settings.local_sync_path == ""
    assert settings.max_threads == 8
    assert settings.verify_ssl is True


def test_load_requires_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_ENDPOINT", "")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "")
    monkeypatch.setenv("MINIO_SECRET_KEY", "")
    monkeypatch.setenv("MINIO_BUCKET", "")
    monkeypatch.setenv("MINIO_LOCAL_SYNC_PATH", "")
    with pytest.raises(ConfigError):
        Settings.load()


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_ENDPOINT", "env.example.com")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "env-access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "env-secret")
    monkeypatch.setenv("MINIO_BUCKET", "env-bucket")
    monkeypatch.setenv("MINIO_LOCAL_SYNC_PATH", "/tmp/sync")
    monkeypatch.setenv("MINIO_MAX_THREADS", "16")
    monkeypatch.setenv("MINIO_SECURE", "false")
    monkeypatch.setenv("MINIO_VERIFY_SSL", "false")
    settings = Settings.load()
    assert settings.endpoint == "env.example.com"
    assert settings.access_key == "env-access"
    assert settings.secret_key == "env-secret"
    assert settings.bucket == "env-bucket"
    assert settings.local_sync_path == "/tmp/sync"
    assert settings.max_threads == 16
    assert settings.secure is False
    assert settings.verify_ssl is False


def test_cli_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_ENDPOINT", "env.example.com")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "env-access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "env-secret")
    monkeypatch.setenv("MINIO_BUCKET", "env-bucket")
    monkeypatch.setenv("MINIO_LOCAL_SYNC_PATH", "/tmp/sync")
    settings = Settings.load(log_level="DEBUG")
    assert settings.log_level == "DEBUG"


def test_runtime_files_resolve_to_base_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MINIO_ENDPOINT", "env.example.com")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "env-access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "env-secret")
    monkeypatch.setenv("MINIO_BUCKET", "env-bucket")
    monkeypatch.setenv("MINIO_LOCAL_SYNC_PATH", "/tmp/sync")
    monkeypatch.chdir(tmp_path)
    settings = Settings.load()
    assert settings.state_db_file == str(tmp_path / "minio_sync_state.db")
