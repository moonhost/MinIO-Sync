"""同步核心逻辑与运行时共享状态封装。"""

from __future__ import annotations

import logging
import os
import queue
import threading
from datetime import timedelta

from minio_sync.circuit_breaker import CircuitBreaker
from minio_sync.client import get_minio_client
from minio_sync.clock import ClockSync
from minio_sync.config import Settings
from minio_sync.downloader import batch_download, list_safe_new_files
from minio_sync.store import FailedRecord, FileInfo, StateStore

logger = logging.getLogger(__name__)


class SyncState:
    """封装同步运行时的共享可变状态，替代模块顶层全局变量。"""

    def __init__(self, max_queue_size: int) -> None:
        self.sync_lock = threading.Lock()
        self.download_queue: queue.Queue[FileInfo] = queue.Queue(maxsize=max_queue_size)
        self.shutdown_event = threading.Event()
        self.reconciliation_trigger = threading.Event()


class SyncEngine:
    """一轮同步的执行器：重试失败文件 → 增量同步新文件 → 推进时间戳。"""

    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        breaker: CircuitBreaker,
        clock: ClockSync,
        state: SyncState,
    ) -> None:
        self.settings = settings
        self.store = store
        self.breaker = breaker
        self.clock = clock
        self.state = state

    def run_once(self) -> None:
        """执行一轮完整同步。防重叠：上一轮未完成则跳过。"""
        if not self.state.sync_lock.acquire(blocking=False):
            logger.warning("上一轮同步尚未完成，跳过本轮")
            return

        try:
            if not self.breaker.allow_request():
                logger.warning("熔断器处于开启状态，跳过本轮同步")
                return

            logger.info("\n===== 开始一轮同步（重试失败 + 增量同步）=====")
            try:
                client = get_minio_client(self.settings)
            except Exception as e:
                logger.error(f"MinIO 客户端创建失败: {type(e).__name__}: {e}")
                self.breaker.record_failure()
                return

            bucket = self.settings.bucket

            try:
                failed_records = self.store.load_failed_files()
                retryable = [
                    r for r in failed_records
                    if r.get("retry_count", 0) < self.settings.max_retry_per_file
                ]
                abandoned = [
                    r for r in failed_records
                    if r.get("retry_count", 0) >= self.settings.max_retry_per_file
                ]

                if abandoned:
                    retryable, abandoned = self._classify_abandoned(retryable, abandoned)

                retry_success: list[str] = []
                retry_failed: list[str] = []
                fuse_triggered = False

                if retryable:
                    retry_infos = [{"path": r["path"], "size": None} for r in retryable]
                    logger.info(f"发现失败文件 {len(retry_infos)} 个，开始重试...")
                    retry_success, retry_failed, fuse_triggered = batch_download(
                        self.settings, self.breaker, retry_infos
                    )
                    logger.info(
                        f"重试完成：成功 {len(retry_success)} 个，剩余失败 {len(retry_failed)} 个"
                    )

                    updated_retryable: list[FailedRecord] = []
                    for r in retryable:
                        if r["path"] in retry_failed:
                            r["retry_count"] = r.get("retry_count", 0) + 1
                            updated_retryable.append(r)
                    failed_records = updated_retryable + abandoned
                    self.store.save_failed_files(failed_records)
                    logger.debug(f"失败记录已更新，当前失败文件总数: {len(failed_records)}")

                    if fuse_triggered:
                        return

                # ====================== 增量同步新文件 ======================
                last_sync_time = self.store.load_last_sync_time()
                logger.info(f"上次同步时间: {last_sync_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
                new_files_raw = list_safe_new_files(
                    client, self.settings, self.clock, bucket, last_sync_time
                )

                failed_paths = {r["path"] for r in failed_records}
                new_files = [f for f in new_files_raw if f["path"] not in failed_paths]
                if len(new_files_raw) != len(new_files):
                    logger.info(
                        f"发现新文件 {len(new_files_raw)} 个，"
                        f"已排除失败记录中的 {len(new_files_raw) - len(new_files)} 个，"
                        f"实际待下载 {len(new_files)} 个"
                    )
                else:
                    logger.info(f"发现新文件 {len(new_files)} 个")

                new_success, new_failed, fuse_triggered = batch_download(
                    self.settings, self.breaker, new_files
                )
                logger.info(
                    f"新文件同步：成功 {len(new_success)} 个，失败 {len(new_failed)} 个"
                )

                new_failed_records = [
                    {"path": path, "retry_count": 1} for path in new_failed
                ]
                failed_records = failed_records + new_failed_records
                self.store.save_failed_files(failed_records)
                logger.debug(f"新失败记录已追加，当前失败文件总数: {len(failed_records)}")

                safe_now = self.clock.server_now() - timedelta(
                    seconds=self.settings.delta_seconds
                )
                self.store.save_last_sync_time(safe_now)

                if not new_failed:
                    self.breaker.reset_recovery_attempts()
                    logger.info(
                        f"✅ 本轮同步完成 | 总成功: {len(retry_success + new_success)} | "
                        f"剩余失败: {len(failed_records)}"
                    )
                else:
                    logger.warning(
                        f"⚠️ 本轮有 {len(new_failed)} 个新文件下载失败，"
                        f"已记录到失败列表等待重试，时间戳已推进至 safe_time"
                    )
                    logger.info(
                        f"本轮同步完成 | 总成功: {len(retry_success + new_success)} | "
                        f"剩余失败: {len(failed_records)}"
                    )

            except Exception as e:
                logger.error(f"同步任务执行异常: {type(e).__name__}: {e}")
                self.breaker.record_failure()

        finally:
            self.state.sync_lock.release()

    def _classify_abandoned(
        self, retryable: list[FailedRecord], abandoned: list[FailedRecord]
    ) -> tuple[list[FailedRecord], list[FailedRecord]]:
        """超重试上限的文件：本地不存在则重置计数重试，本地存在则放弃。"""
        reactivated: list[FailedRecord] = []
        truly_abandoned: list[FailedRecord] = []
        for r in abandoned:
            local_path = os.path.join(self.settings.local_sync_path, r["path"])
            if not os.path.exists(local_path):
                reactivated.append({"path": r["path"], "retry_count": 0})
            else:
                truly_abandoned.append(r)
        if reactivated:
            logger.warning(
                f"⚠️ {len(reactivated)} 个文件虽超过重试上限但本地不存在，重置重试计数重新下载: "
                f"{[r['path'] for r in reactivated]}"
            )
            retryable = [*retryable, *reactivated]
        if truly_abandoned:
            logger.warning(
                f"⚠️ {len(truly_abandoned)} 个文件已超过最大重试次数"
                f"({self.settings.max_retry_per_file})且本地已存在(可能不完整)，跳过: "
                f"{[r['path'] for r in truly_abandoned]}"
            )
        return retryable, truly_abandoned


__all__ = ["SyncState", "SyncEngine"]
