"""事件监听与队列下载工作线程。"""

from __future__ import annotations

import logging
import queue

from minio_sync.circuit_breaker import CircuitBreaker
from minio_sync.client import get_minio_client
from minio_sync.config import Settings
from minio_sync.downloader import download_file
from minio_sync.store import StateStore
from minio_sync.sync_engine import SyncState

logger = logging.getLogger(__name__)


def event_listener(settings: Settings, state: SyncState, store: StateStore) -> None:
    """监听 MinIO Bucket Notification，将新文件事件推入下载队列。

    断线自动重连；队列满时丢弃事件（由对账机制补回）。
    """
    bucket = settings.bucket
    logger.info(f"事件监听器已进入主循环 | bucket: {bucket}")
    while not state.shutdown_event.is_set():
        try:
            client = get_minio_client(settings)
            prefix = settings.sync_prefix if settings.sync_prefix else ""
            logger.info(f"事件监听已启动 | bucket: {bucket} | prefix: {prefix or '/'}")
            events = client.listen_bucket_notification(
                bucket, prefix=prefix, events=("s3:ObjectCreated:*",)
            )
            for event in events:
                if state.shutdown_event.is_set():
                    break
                for record in event.get("Records", []):
                    try:
                        obj_key = record["s3"]["object"]["key"]
                        obj_size = record["s3"]["object"]["size"]
                        file_info = {"path": obj_key, "size": obj_size}
                        state.download_queue.put(file_info, timeout=5)
                        store.add_pending_item(file_info)
                        logger.info(f"收到新文件事件: {obj_key} ({obj_size} bytes)")
                    except (KeyError, queue.Full) as e:
                        if isinstance(e, queue.Full):
                            logger.warning(
                                f"下载队列已满({settings.max_queue_size})，丢弃事件: "
                                f"{obj_key}（将由对账机制补回）"
                            )
                        else:
                            logger.warning(f"事件记录格式异常，跳过: {e}")
        except Exception as e:
            if state.shutdown_event.is_set():
                break
            logger.error(
                f"事件监听异常，{settings.event_reconnect_delay}秒后重连: "
                f"{type(e).__name__}: {e}"
            )
            state.shutdown_event.wait(settings.event_reconnect_delay)
            if not state.shutdown_event.is_set():
                logger.info("事件监听重连成功，触发即时对账补偿断连期间可能遗漏的文件")
                state.reconciliation_trigger.set()
    logger.info("事件监听器已退出主循环")


def queue_download_worker(
    settings: Settings,
    state: SyncState,
    store: StateStore,
    breaker: CircuitBreaker,
) -> None:
    """从下载队列取任务执行下载，成功后移除持久化队列条目，失败则记入失败列表。"""
    bucket = settings.bucket
    logger.info(f"下载工作线程已启动 | bucket: {bucket}")
    while not state.shutdown_event.is_set():
        try:
            file_info = state.download_queue.get(timeout=1)
        except queue.Empty:
            continue

        ok, filepath = download_file(
            settings, breaker, bucket, file_info["path"], file_info.get("size")
        )
        if ok:
            logger.info(f"事件驱动下载成功: {filepath}")
            store.remove_pending_item(filepath)
        else:
            store.add_failed_record(filepath)
            store.remove_pending_item(filepath)
            logger.warning(f"事件驱动下载失败: {filepath}")

        state.download_queue.task_done()
    logger.info("下载工作线程已退出")


__all__ = ["event_listener", "queue_download_worker"]
