"""文件下载：新文件列举、单文件下载（含完整性校验）、多线程批量下载。"""

from __future__ import annotations

import contextlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

from minio import Minio

from minio_sync.circuit_breaker import CircuitBreaker
from minio_sync.client import get_thread_minio_client
from minio_sync.clock import ClockSync
from minio_sync.config import Settings
from minio_sync.store import FileInfo

logger = logging.getLogger(__name__)


def list_safe_new_files(
    client: Minio,
    settings: Settings,
    clock: ClockSync,
    bucket: str,
    last_sync_time: datetime,
) -> list[FileInfo]:
    """列举比上次同步时间新、且早于安全偏移时间的文件。"""
    new_files: list[FileInfo] = []
    now = clock.server_now()
    safe_time = now - timedelta(seconds=settings.delta_seconds)
    prefix = settings.sync_prefix if settings.sync_prefix else None

    logger.info(
        f"开始列举新文件 | bucket: {bucket} | prefix: {prefix or '/'} | "
        f"时间范围: ({last_sync_time.strftime('%Y-%m-%dT%H:%M:%SZ')}, "
        f"{safe_time.strftime('%Y-%m-%dT%H:%M:%SZ')})"
    )
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    scanned = 0
    log_interval = 1000
    for obj in objects:
        if obj.last_modified is None:
            continue

        scanned += 1
        logger.info(f"正在扫描：{scanned} : {obj.object_name}")
        if scanned % log_interval == 0:
            logger.info(f"已扫描 {scanned} 个对象，发现 {len(new_files)} 个新文件")

        obj_time = obj.last_modified
        if obj_time.tzinfo is None:
            obj_time = obj_time.replace(tzinfo=UTC)
        if last_sync_time < obj_time < safe_time:
            new_files.append(
                {
                    "path": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj_time,
                    "etag": obj.etag,
                }
            )
    logger.info(f"列举完成 | 扫描: {scanned} | 新文件: {len(new_files)}")
    return new_files


def download_file(
    settings: Settings,
    breaker: CircuitBreaker,
    bucket: str,
    file_path: str,
    expected_size: int | None = None,
) -> tuple[bool, str]:
    """下载单个文件到本地，返回 (是否成功, 文件路径)。

    已存在且完整则跳过；下载后做文件大小校验；失败时清理临时文件并记录熔断失败。
    """
    client = get_thread_minio_client(settings)
    local_path = os.path.join(settings.local_sync_path, file_path)
    tmp_path = local_path + ".downloading"
    obj_size: int
    try:
        if expected_size is not None:
            obj_size = expected_size
        else:
            obj_stat = client.stat_object(bucket, file_path)
            if obj_stat.size is None:
                raise ValueError(f"无法获取对象大小: {file_path}")
            obj_size = obj_stat.size

        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            if local_size == obj_size:
                logger.info(f"文件已存在且完整，跳过: {file_path}")
                breaker.record_success()
                return True, file_path

        if os.path.exists(tmp_path):
            logger.info(f"发现上次中断的临时文件，将重新下载: {file_path}")
            with contextlib.suppress(OSError):
                os.remove(tmp_path)

        local_dir = os.path.dirname(local_path)
        os.makedirs(local_dir, exist_ok=True)
        logger.info(f"开始下载: {file_path} ({obj_size} bytes)")
        client.fget_object(bucket, file_path, tmp_path)

        local_size = os.path.getsize(tmp_path)
        if local_size != obj_size:
            logger.error(
                f"文件完整性校验失败: {file_path} | 期望大小: {obj_size} | 实际大小: {local_size}"
            )
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            breaker.record_failure()
            return False, file_path

        os.replace(tmp_path, local_path)
        logger.info(f"下载完成: {file_path} ({obj_size} bytes)")
        breaker.record_success()
        return True, file_path
    except Exception as e:
        logger.error(f"下载失败: {file_path} | 错误: {type(e).__name__}: {e}")
        if os.path.exists(tmp_path):
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
        breaker.record_failure()
        return False, file_path


def batch_download(
    settings: Settings,
    breaker: CircuitBreaker,
    file_infos: list[FileInfo],
) -> tuple[list[str], list[str], bool]:
    """多线程批量下载，返回 (成功列表, 失败列表, 是否触发熔断)。"""
    bucket = settings.bucket
    success_files: list[str] = []
    fail_files: list[str] = []
    fuse_triggered = False

    logger.info(
        f"开始批量下载 | 文件数: {len(file_infos)} | 线程数: {settings.max_threads}")

    with ThreadPoolExecutor(max_workers=settings.max_threads) as executor:
        future_to_file = {
            executor.submit(
                download_file,
                settings,
                breaker,
                bucket,
                fi["path"],
                fi.get("size"),
            ): fi["path"]
            for fi in file_infos
        }

        for future in as_completed(future_to_file):
            try:
                ok, filename = future.result()
            except Exception as e:
                logger.error(f"下载任务异常: {e}")
                ok = False
                filename = future_to_file[future]

            if ok:
                success_files.append(filename)
            else:
                fail_files.append(filename)

            if breaker.is_open:
                logger.error("\n" + "=" * 80)
                logger.error("🔴 🔴 🔴 严重警告：连续失败达到阈值，同步已暂停！请检查网络/MinIO！")
                logger.error(f"当前连续失败数: {breaker.continuous_fail}")
                logger.error("=" * 80 + "\n")
                fuse_triggered = True
                for fut in future_to_file:
                    if not fut.done():
                        fut.cancel()
                break

    return success_files, fail_files, fuse_triggered


__all__ = ["list_safe_new_files", "download_file", "batch_download"]