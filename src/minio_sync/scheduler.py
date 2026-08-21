"""调度器：hybrid 同步策略，统一优雅退出与信号处理。"""

from __future__ import annotations

import logging
import signal
import threading
from functools import partial
from typing import Protocol

import schedule

from minio_sync.circuit_breaker import CircuitBreaker
from minio_sync.clock import ClockSync
from minio_sync.config import Settings
from minio_sync.event_listener import event_listener, queue_download_worker
from minio_sync.store import SqliteStateStore, StateStore, persist_queue_snapshot
from minio_sync.sync_engine import SyncEngine, SyncState

logger = logging.getLogger(__name__)


class SyncStrategy(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...


def _detect_and_log_clock(clock: ClockSync) -> None:
    offset = clock.detect_offset()
    if abs(offset) > 1.0:
        logger.warning(
            f"检测到时钟偏移: {offset:+.1f} 秒 "
            f"（服务器时间{'快' if offset > 0 else '慢'}于本地）"
        )
    else:
        logger.info(f"时钟偏移检测: {offset:+.3f} 秒（正常）")


class HybridStrategy:
    """混合同步策略：事件驱动 + 周期对账兜底。"""

    def __init__(
        self,
        settings: Settings,
        engine: SyncEngine,
        clock: ClockSync,
        state: SyncState,
        store: StateStore,
    ) -> None:
        self.settings = settings
        self.engine = engine
        self.clock = clock
        self.state = state
        self.store = store

    def start(self) -> None:
        logger.info("混合同步服务正在启动...")
        self._setup_signals()
        _detect_and_log_clock(self.clock)
        self._restore_pending_queue()
        self._start_listener()
        self._start_workers()
        self._install_schedule()
        logger.info(
            f"混合同步服务已启动 | "
            f"下载线程: {self.settings.max_threads} | "
            f"队列容量: {self.settings.max_queue_size} | "
            f"熔断阈值: {self.settings.max_continuous_fail} | "
            f"熔断恢复: {self.settings.fuse_recovery_seconds}秒"
        )
        self.engine.run_once()

        try:
            while not self.state.shutdown_event.is_set():
                schedule.run_pending()
                if self.state.reconciliation_trigger.is_set():
                    self.state.reconciliation_trigger.clear()
                    logger.info("⚡ 收到即时对账触发（事件监听重连补偿）")
                    self.engine.run_once()
                self.state.shutdown_event.wait(1)
        except KeyboardInterrupt:
            self.state.shutdown_event.set()

        persist_queue_snapshot(self.state.download_queue, self.store)
        logger.info("同步服务已停止")

    def stop(self) -> None:
        logger.info("收到停止请求，正在关闭同步服务...")
        self.state.shutdown_event.set()

    def _restore_pending_queue(self) -> None:
        pending = self.store.load_pending_queue()
        if pending:
            logger.info(f"从持久化队列恢复 {len(pending)} 个待下载文件")
            for item in pending:
                try:
                    self.state.download_queue.put(item, timeout=5)
                except Exception:
                    logger.warning(f"恢复队列已满，跳过: {item['path']}")
        else:
            logger.info("无待恢复的持久化队列")

    def _start_listener(self) -> None:
        logger.info("启动事件监听线程...")
        thread = threading.Thread(
            target=event_listener,
            args=(self.settings, self.state, self.store),
            daemon=True,
            name="event-listener",
        )
        thread.start()
        logger.info("事件监听线程已启动")

    def _start_workers(self) -> None:
        logger.info(f"启动 {self.settings.max_threads} 个下载工作线程...")
        for i in range(self.settings.max_threads):
            thread = threading.Thread(
                target=queue_download_worker,
                args=(self.settings, self.state, self.store, self.engine.breaker),
                daemon=True,
                name=f"download-worker-{i}",
            )
            thread.start()
        logger.info(f"{self.settings.max_threads} 个下载工作线程已启动")

    def _install_schedule(self) -> None:
        schedule.every(self.settings.reconciliation_interval).seconds.do(
            self.engine.run_once
        )
        logger.info(f"对账模式已启动 | 间隔: {self.settings.reconciliation_interval}秒")
        schedule.every(60).seconds.do(
            partial(persist_queue_snapshot, self.state.download_queue, self.store)
        )
        logger.info("队列快照定时持久化已启动 | 间隔: 60秒")
        schedule.every(self.settings.clock_offset_recheck_interval).seconds.do(
            self.clock.recheck
        )
        logger.info(f"时钟偏移定时检测已启动 | 间隔: {self.settings.clock_offset_recheck_interval}秒")

    def _setup_signals(self) -> None:
        def handler(signum: int, frame: object) -> None:
            logger.info(f"收到信号 {signum}，正在优雅关闭...")
            self.stop()

        signal.signal(signal.SIGINT, handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handler)
        logger.info("信号处理器已注册 (SIGINT, SIGTERM)")


def run_service(settings: Settings) -> None:
    """组装全部组件并启动同步服务。"""
    logger.info("开始组装同步服务组件...")

    store = SqliteStateStore(
        db_path=settings.state_db_file,
        initial_sync_time=settings.initial_sync_time,
    )
    logger.info(f"状态存储已创建: {settings.state_db_file}")

    breaker = CircuitBreaker(
        settings.max_continuous_fail, settings.fuse_recovery_seconds
    )
    logger.info(f"熔断器已创建 | 阈值: {settings.max_continuous_fail} | 恢复时间: {settings.fuse_recovery_seconds}秒")

    clock = ClockSync(settings)
    logger.info("时钟同步器已创建")

    state = SyncState(settings.max_queue_size)
    logger.info(f"同步状态已创建 | 队列容量: {settings.max_queue_size}")

    engine = SyncEngine(settings, store, breaker, clock, state)
    logger.info("同步引擎已创建")

    strategy = HybridStrategy(settings, engine, clock, state, store)
    logger.info("混合同步策略已创建，准备启动服务...")
    strategy.start()


__all__ = ["run_service", "HybridStrategy", "SyncStrategy"]
