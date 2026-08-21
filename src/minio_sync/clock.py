"""时钟偏移检测与服务器时间估算。"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import urllib3

from minio_sync.config import Settings

logger = logging.getLogger(__name__)


class ClockSync:
    """封装本地与 MinIO 服务器之间的时钟偏移检测。

    server_time ≈ local_time + offset
    offset 为正值表示服务器时间比本地快。
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._offset = 0.0
        cert_reqs = "CERT_REQUIRED" if (settings.secure and settings.verify_ssl) else "CERT_NONE"
        self._http = urllib3.PoolManager(
            cert_reqs=cert_reqs,
            timeout=urllib3.Timeout(connect=5, read=5),
        )

    @property
    def offset(self) -> float:
        return self._offset

    def detect_offset(self) -> float:
        """通过响应头 Date 计算偏移量并更新内部值。"""
        endpoint = self._settings.endpoint
        secure = self._settings.secure
        scheme = "https" if secure else "http"

        try:
            logger.debug(f"开始检测时钟偏移 | endpoint: {endpoint}")
            local_before = datetime.now(UTC)
            resp = self._http.request("HEAD", f"{scheme}://{endpoint}/")
            local_after = datetime.now(UTC)

            date_str = resp.headers.get("Date")
            if not date_str:
                logger.warning("服务器响应中无 Date 头，无法检测时钟偏移")
                self._offset = 0.0
                return 0.0

            server_time = parsedate_to_datetime(date_str)
            if server_time.tzinfo is None:
                server_time = server_time.replace(tzinfo=UTC)

            local_mid = local_before + (local_after - local_before) / 2
            self._offset = (server_time - local_mid).total_seconds()
            logger.info(f"时钟偏移检测完成 | 偏移量: {self._offset:+.3f} 秒")
            return self._offset
        except Exception as e:
            logger.warning(f"时钟偏移检测失败，使用默认值 0: {e}")
            self._offset = 0.0
            return 0.0

    def server_now(self) -> datetime:
        """估算的当前服务器时间（UTC）。"""
        return datetime.now(UTC) + timedelta(seconds=self._offset)

    def recheck(self) -> None:
        """定期重新检测偏移量，漂移超过 1 秒时告警。"""
        logger.debug("开始定期时钟偏移重检...")
        old_offset = self._offset
        new_offset = self.detect_offset()
        drift = new_offset - old_offset
        if abs(drift) > 1.0:
            logger.warning(
                f"时钟偏移量变化: {old_offset:+.1f}s → {new_offset:+.1f}s "
                f"(漂移 {drift:+.1f}s)，已更新"
            )
        else:
            logger.debug(f"时钟偏移稳定 | 当前偏移: {new_offset:+.3f}s | 漂移: {drift:+.3f}s")


__all__ = ["ClockSync"]
