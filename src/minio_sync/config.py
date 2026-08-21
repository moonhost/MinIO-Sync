"""配置加载：默认值 ← 环境变量 ← CLI 参数。

密钥（access_key / secret_key）仅从环境变量或 .env 读取，绝不写入源码。
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_type_hints

from dotenv import load_dotenv

from minio_sync.exceptions import ConfigError

# 运行时文件与 .env 的默认查找目录：当前工作目录。
# 从项目根目录启动（python -m minio_sync）时即项目根目录，
# 与旧版 sync_config.json 等文件位置保持一致。
BASE_DIR = Path.cwd()

# 字段名 → 环境变量名映射
_ENV_MAP: dict[str, str] = {
    "endpoint": "MINIO_ENDPOINT",
    "access_key": "MINIO_ACCESS_KEY",
    "secret_key": "MINIO_SECRET_KEY",
    "bucket": "MINIO_BUCKET",
    "secure": "MINIO_SECURE",
    "verify_ssl": "MINIO_VERIFY_SSL",
    "local_sync_path": "MINIO_LOCAL_SYNC_PATH",
    "sync_interval": "MINIO_SYNC_INTERVAL",
    "delta_seconds": "MINIO_DELTA_SECONDS",
    "max_threads": "MINIO_MAX_THREADS",
    "max_continuous_fail": "MINIO_MAX_CONTINUOUS_FAIL",
    "fuse_recovery_seconds": "MINIO_FUSE_RECOVERY_SECONDS",
    "max_retry_per_file": "MINIO_MAX_RETRY_PER_FILE",
    "sync_prefix": "MINIO_SYNC_PREFIX",
    "initial_sync_time": "MINIO_INITIAL_SYNC_TIME",
    "state_db_file": "MINIO_STATE_DB_FILE",
    "reconciliation_interval": "MINIO_RECONCILIATION_INTERVAL",
    "event_reconnect_delay": "MINIO_EVENT_RECONNECT_DELAY",
    "max_queue_size": "MINIO_MAX_QUEUE_SIZE",
    "clock_offset_recheck_interval": "MINIO_CLOCK_OFFSET_RECHECK_INTERVAL",
    "log_level": "MINIO_LOG_LEVEL",
    "log_file": "MINIO_LOG_FILE",
    "log_max_bytes": "MINIO_LOG_MAX_BYTES",
    "log_backup_count": "MINIO_LOG_BACKUP_COUNT",
}

# 运行时状态文件默认落在仓库根目录
_RUNTIME_FILES = {
    "state_db_file": "minio_sync_state.db",
}


def _coerce(value: Any, field_type: type) -> Any:
    if field_type is bool:
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(value)
    if field_type is int:
        return int(value)
    if field_type is float:
        return float(value)
    return value


@dataclass
class Settings:
    """同步服务全部配置项。"""

    # ---- MinIO ----
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""
    bucket: str = ""
    secure: bool = True
    verify_ssl: bool = True

    # ---- 路径 ----
    local_sync_path: str = ""
    state_db_file: str = ""

    # ---- 同步参数 ----
    sync_interval: int = 60
    delta_seconds: int = 30
    max_threads: int = 8
    max_continuous_fail: int = 20
    fuse_recovery_seconds: int = 300
    max_retry_per_file: int = 5
    sync_prefix: str = ""
    initial_sync_time: str = "2024-01-01T00:00:00Z"

    # ---- 模式 ----
    reconciliation_interval: int = 1800
    event_reconnect_delay: int = 5
    max_queue_size: int = 10000
    clock_offset_recheck_interval: int = 3600

    # ---- 日志 ----
    log_level: str = "INFO"
    log_file: str = "logs/minio_sync.log"
    log_max_bytes: int = 50 * 1024 * 1024
    log_backup_count: int = 10

    @classmethod
    def load(
        cls,
        log_level: str | None = None,
    ) -> Settings:
        """按 默认值 ← 环境变量 ← CLI 参数 的顺序加载配置。"""
        load_dotenv()  # 从当前工作目录向上查找 .env
        settings = cls()
        settings._apply_env()
        if log_level:
            settings.log_level = log_level
        settings._resolve_runtime_files()
        settings.validate()
        return settings

    def _apply_env(self) -> None:
        mapping: dict[str, Any] = {}
        for field_name, env_name in _ENV_MAP.items():
            value = os.environ.get(env_name)
            if value is not None and value != "":
                mapping[field_name] = value
        self._apply_mapping(mapping)

    def _apply_mapping(self, mapping: dict[str, Any]) -> None:
        # 使用 get_type_hints 解析字符串注解（from __future__ import annotations 下 f.type 为字符串）
        type_hints = get_type_hints(self.__class__)
        for key, value in mapping.items():
            if key not in type_hints:
                continue
            field_type = type_hints[key]
            try:
                setattr(self, key, _coerce(value, field_type))
            except (TypeError, ValueError) as e:
                raise ConfigError(f"配置项 {key} 的值无效: {value!r} ({e})") from e

    def _resolve_runtime_files(self) -> None:
        base = Path.cwd()
        for name, filename in _RUNTIME_FILES.items():
            if not getattr(self, name):
                setattr(self, name, str(base / filename))

    def validate(self) -> None:
        if not self.endpoint:
            raise ConfigError("缺少 MinIO endpoint，请在 .env 或环境变量中设置 MINIO_ENDPOINT")
        if not self.access_key:
            raise ConfigError("缺少 MinIO access_key，请在 .env 或环境变量中设置 MINIO_ACCESS_KEY")
        if not self.secret_key:
            raise ConfigError("缺少 MinIO secret_key，请在 .env 或环境变量中设置 MINIO_SECRET_KEY")
        if not self.bucket:
            raise ConfigError("缺少 MinIO bucket，请在 .env 或环境变量中设置 MINIO_BUCKET")
        if not self.local_sync_path:
            raise ConfigError("缺少本地同步路径，请在 .env 或环境变量中设置 MINIO_LOCAL_SYNC_PATH")
        if self.max_threads < 1:
            raise ConfigError("max_threads 必须 >= 1")


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="minio-sync", description="MinIO 增量同步服务")
    parser.add_argument("--log-level", help="日志级别（DEBUG/INFO/WARNING/ERROR）")
    parser.add_argument("--check", action="store_true", help="校验配置后退出，不启动服务")
    return parser.parse_args(argv)


__all__ = ["Settings", "parse_cli_args", "BASE_DIR"]
