"""熔断器：连续失败保护与指数退避自动恢复。

状态机:
  CLOSED   ──(连续失败 >= 阈值)──▶ OPEN
  OPEN     ──(恢复时间到)────────▶ HALF_OPEN
  HALF_OPEN ──(成功)────────────▶ CLOSED
  HALF_OPEN ──(失败)────────────▶ OPEN（指数退避）
"""

from __future__ import annotations

import logging
import threading
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """线程安全的熔断器。"""

    def __init__(self, max_fail: int, recovery_seconds: int) -> None:
        self._max_fail = max_fail
        self._recovery_seconds = recovery_seconds
        self._state = CircuitState.CLOSED
        self._continuous_fail = 0
        self._opened_at: float | None = None
        self._recovery_attempts = 0
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        with self._lock:
            return self._state == CircuitState.OPEN

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    @property
    def continuous_fail(self) -> int:
        with self._lock:
            return self._continuous_fail

    @property
    def recovery_attempts(self) -> int:
        with self._lock:
            return self._recovery_attempts

    def allow_request(self) -> bool:
        """是否允许发起请求。OPEN 状态下恢复时间到则进入 HALF_OPEN 试探。"""
        with self._lock:
            if self._state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
                return True
            if self._opened_at is None:
                return False
            elapsed = time.monotonic() - self._opened_at
            recovery_wait = self._recovery_seconds * (2**self._recovery_attempts)
            if elapsed >= recovery_wait:
                self._state = CircuitState.HALF_OPEN
                self._recovery_attempts += 1
                logger.info(
                    f"熔断恢复期已到（第{self._recovery_attempts}次尝试，"
                    f"指数退避等待{recovery_wait}秒），试探性恢复..."
                )
                return True
            remaining = int(recovery_wait - elapsed)
            logger.warning(
                f"系统已熔断暂停，预计 {remaining} 秒后自动恢复尝试"
                f"（第{self._recovery_attempts + 1}次，指数退避）..."
            )
            return False

    def record_success(self) -> None:
        with self._lock:
            old_fail = self._continuous_fail
            self._continuous_fail = 0
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                self._recovery_attempts = 0
                self._opened_at = None
                logger.info("熔断试探成功，熔断器已关闭 (HALF_OPEN → CLOSED)")
            elif old_fail > 0:
                logger.debug(f"连续失败计数已重置: {old_fail} → 0")

    def record_failure(self) -> None:
        with self._lock:
            self._continuous_fail += 1
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.error("熔断试探失败，熔断器重新打开 (HALF_OPEN → OPEN，指数退避)")
            elif self._state == CircuitState.CLOSED and self._continuous_fail >= self._max_fail:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.error(
                    f"连续失败达到阈值({self._max_fail})，熔断器已打开 (CLOSED → OPEN)，同步暂停！"
                )
            else:
                logger.debug(f"记录失败，当前连续失败: {self._continuous_fail}/{self._max_fail}")

    def reset_recovery_attempts(self) -> None:
        """一轮同步完全成功时重置退避计数。"""
        with self._lock:
            old = self._recovery_attempts
            self._recovery_attempts = 0
            if old > 0:
                logger.info(f"熔断恢复尝试计数已重置: {old} → 0")

    def check_recovery(self) -> bool:
        """是否处于熔断打开状态（供健康检查等使用）。"""
        return self.is_open


__all__ = ["CircuitBreaker", "CircuitState"]
