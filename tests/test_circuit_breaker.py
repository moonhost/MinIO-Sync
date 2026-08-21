"""熔断器状态转换与指数退避恢复测试。"""

from __future__ import annotations

import time

from minio_sync.circuit_breaker import CircuitBreaker, CircuitState


def test_initial_state_closed() -> None:
    breaker = CircuitBreaker(max_fail=3, recovery_seconds=60)
    assert breaker.state == CircuitState.CLOSED
    assert not breaker.is_open
    assert breaker.allow_request()


def test_opens_after_threshold() -> None:
    breaker = CircuitBreaker(max_fail=3, recovery_seconds=60)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
    assert breaker.is_open
    assert not breaker.allow_request()


def test_success_resets_fail_count() -> None:
    breaker = CircuitBreaker(max_fail=3, recovery_seconds=60)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.continuous_fail == 0
    assert breaker.state == CircuitState.CLOSED


def test_recovery_after_wait_transitions_to_half_open() -> None:
    breaker = CircuitBreaker(max_fail=2, recovery_seconds=1)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
    assert not breaker.allow_request()
    time.sleep(1.1)
    assert breaker.allow_request()
    assert breaker.state == CircuitState.HALF_OPEN


def test_half_open_success_closes() -> None:
    breaker = CircuitBreaker(max_fail=2, recovery_seconds=1)
    breaker.record_failure()
    breaker.record_failure()
    time.sleep(1.1)
    assert breaker.allow_request()
    breaker.record_success()
    assert breaker.state == CircuitState.CLOSED
    assert breaker.recovery_attempts == 0


def test_half_open_failure_reopens_with_backoff() -> None:
    breaker = CircuitBreaker(max_fail=2, recovery_seconds=1)
    breaker.record_failure()
    breaker.record_failure()
    time.sleep(1.1)
    assert breaker.allow_request()  # → HALF_OPEN, attempts=1
    breaker.record_failure()  # → OPEN
    assert breaker.state == CircuitState.OPEN
    # 第二次恢复等待应为指数退避：1 * 2^1 = 2 秒
    assert not breaker.allow_request()
    time.sleep(1.1)
    assert not breaker.allow_request()  # 2 秒未到
    time.sleep(1.1)
    assert breaker.allow_request()  # 2 秒已到


def test_reset_recovery_attempts() -> None:
    breaker = CircuitBreaker(max_fail=2, recovery_seconds=1)
    breaker.record_failure()
    breaker.record_failure()
    time.sleep(1.1)
    breaker.allow_request()
    assert breaker.recovery_attempts == 1
    breaker.reset_recovery_attempts()
    assert breaker.recovery_attempts == 0


def test_check_recovery_reflects_open_state() -> None:
    breaker = CircuitBreaker(max_fail=1, recovery_seconds=60)
    assert not breaker.check_recovery()
    breaker.record_failure()
    assert breaker.check_recovery()
