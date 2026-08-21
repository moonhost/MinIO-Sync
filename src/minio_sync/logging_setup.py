"""日志系统：控制台 + 文件输出 + 轮转。"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from minio_sync.config import Settings

_CONSOLE_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
_FILE_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def setup_logging(settings: Settings) -> None:
    """初始化根日志器：控制台 + 轮转文件，并抑制 MinIO SDK 噪音。"""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_CONSOLE_FORMAT))
    root.addHandler(console)

    if settings.log_file:
        log_path = Path(settings.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=settings.log_max_bytes,
            backupCount=settings.log_backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(_FILE_FORMAT))
        root.addHandler(file_handler)

    # 抑制 MinIO SDK / urllib3 噪音，避免干扰主日志
    logging.getLogger("minio").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


__all__ = ["setup_logging"]
