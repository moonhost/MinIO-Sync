"""MinIO 客户端管理（线程本地存储）。"""

from __future__ import annotations

import logging
import threading

from minio import Minio

from minio_sync.config import Settings

logger = logging.getLogger(__name__)

_thread_local = threading.local()


def get_minio_client(settings: Settings) -> Minio:
    """创建 MinIO 客户端。cert_check 由 verify_ssl 控制，默认启用证书验证。"""
    logger.debug(
        f"创建 MinIO 客户端 | endpoint: {settings.endpoint} "
        f"| secure: {settings.secure} | verify_ssl: {settings.verify_ssl}"
    )
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
        cert_check=settings.verify_ssl,
    )


def get_thread_minio_client(settings: Settings) -> Minio:
    """获取当前线程的 MinIO 客户端（首次调用时创建并复用）。"""
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = get_minio_client(settings)
        _thread_local.client = client
        logger.debug(f"线程 {threading.current_thread().name} 创建新的 MinIO 客户端")
    return client


__all__ = ["get_minio_client", "get_thread_minio_client"]
