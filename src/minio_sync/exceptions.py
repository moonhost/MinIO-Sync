"""自定义异常层次。"""


class MinioSyncError(Exception):
    """MinIO 同步基础异常。"""


class ConfigError(MinioSyncError):
    """配置错误。"""


class MinioConnectionError(MinioSyncError):
    """连接异常。"""


class DownloadError(MinioSyncError):
    """下载异常。"""


class IntegrityError(DownloadError):
    """文件完整性校验失败。"""


class CircuitBreakerOpenError(MinioSyncError):
    """熔断器已打开。"""


class StoreError(MinioSyncError):
    """持久化存储错误。"""


__all__ = [
    "MinioSyncError",
    "ConfigError",
    "MinioConnectionError",
    "DownloadError",
    "IntegrityError",
    "CircuitBreakerOpenError",
    "StoreError",
]
