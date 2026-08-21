# MinIO Sync

MinIO 对象存储增量同步工具 v3.0.0。

将 MinIO 桶中新增/更新的对象增量同步到本地目录，采用**事件驱动 + 周期对账**的混合模式，内置熔断保护、时钟偏移校正、文件完整性校验与失败自动重试。

## 特性

- **安全**：密钥从 `.env` / 环境变量读取，绝不写入源码；HTTPS 默认启用证书验证
- **混合同步**：事件驱动实时同步 + 周期对账兜底，兼顾实时性与可靠性
- **熔断保护**：连续失败自动熔断，指数退避自动恢复，防止雪崩
- **完整性校验**：下载后文件大小校验，已存在且完整则跳过
- **时钟校正**：自动检测本地与服务器时钟偏移，避免漏同步/重复同步
- **失败重试**：失败文件持久化记录到 SQLite，按重试上限自动重试；超限但本地不存在则重置计数重新下载
- **优雅退出**：信号处理 + 内存队列快照持久化，确保关闭时不丢数据
- **日志轮转**：控制台 + 文件双输出，50MB 自动轮转，抑制 SDK 噪音

## 环境要求

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/)（推荐）或 pip

## 安装

```bash
uv sync              # 安装运行依赖
uv sync --extra dev  # 安装开发依赖（pytest / ruff / mypy）
```

## 配置

配置优先级：**内置默认值 < 环境变量(.env) < CLI 参数**

### 1. 创建 `.env`（必填）

复制模板并填写真实密钥：

```bash
cp .env.example .env
```

最小必填项：

```env
MINIO_ENDPOINT=your-endpoint
MINIO_ACCESS_KEY=your-access-key
MINIO_SECRET_KEY=your-secret-key
MINIO_BUCKET=your-bucket
MINIO_LOCAL_SYNC_PATH=/path/to/local/sync
```

### 2. 环境变量一览

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `MINIO_ENDPOINT` | MinIO 服务地址（必填） | — |
| `MINIO_ACCESS_KEY` | 访问密钥（必填） | — |
| `MINIO_SECRET_KEY` | 秘密密钥（必填） | — |
| `MINIO_BUCKET` | 桶名（必填） | — |
| `MINIO_SECURE` | 是否 HTTPS | `true` |
| `MINIO_VERIFY_SSL` | 是否校验 SSL 证书 | `true` |
| `MINIO_LOCAL_SYNC_PATH` | 本地同步目录（必填） | — |
| `MINIO_STATE_DB_FILE` | 状态数据库路径 | `./minio_sync_state.db` |
| `MINIO_SYNC_INTERVAL` | 同步轮询间隔（秒） | `60` |
| `MINIO_DELTA_SECONDS` | 安全偏移时间（秒） | `30` |
| `MINIO_MAX_THREADS` | 下载线程数 | `8` |
| `MINIO_MAX_CONTINUOUS_FAIL` | 熔断阈值（连续失败次数） | `20` |
| `MINIO_FUSE_RECOVERY_SECONDS` | 熔断恢复基础等待（秒） | `300` |
| `MINIO_MAX_RETRY_PER_FILE` | 单文件最大重试次数 | `5` |
| `MINIO_SYNC_PREFIX` | 对象键前缀过滤 | `""` |
| `MINIO_INITIAL_SYNC_TIME` | 首次同步起始时间 | `2024-01-01T00:00:00Z` |
| `MINIO_RECONCILIATION_INTERVAL` | 对账周期（秒） | `1800` |
| `MINIO_EVENT_RECONNECT_DELAY` | 事件监听断线重连延迟（秒） | `5` |
| `MINIO_MAX_QUEUE_SIZE` | 事件下载队列容量 | `10000` |
| `MINIO_CLOCK_OFFSET_RECHECK_INTERVAL` | 时钟偏移重检间隔（秒） | `3600` |
| `MINIO_LOG_LEVEL` | 日志级别 | `INFO` |
| `MINIO_LOG_FILE` | 日志文件路径 | `logs/minio_sync.log` |
| `MINIO_LOG_MAX_BYTES` | 单个日志文件最大字节数 | `52428800`（50MB） |
| `MINIO_LOG_BACKUP_COUNT` | 日志轮转保留份数 | `10` |

完整变量清单见 [.env.example](.env.example)。

## 使用

```bash
# 启动同步服务
python -m minio_sync

# 仅校验配置，不启动服务
python -m minio_sync --check

# 指定日志级别
python -m minio_sync --log-level DEBUG

# 安装后也可直接使用命令
minio-sync
```

### 同步策略

服务采用 **混合同步策略（Hybrid）** 运行：

1. **事件驱动**：监听 MinIO Bucket Notification（`s3:ObjectCreated:*`），新文件事件即时推入下载队列
2. **周期对账**：每隔 `MINIO_RECONCILIATION_INTERVAL` 秒执行一次全量增量扫描，兜底补回事件遗漏的文件
3. **断线补偿**：事件监听断线重连后立即触发一次即时对账

> 不确定服务器是否支持事件通知？运行 `python scripts/check_notification.py` 检测。

### 熔断器状态机

```
CLOSED ──(连续失败 >= 阈值)──▶ OPEN
OPEN   ──(恢复时间到)────────▶ HALF_OPEN
HALF_OPEN ──(成功)──────────▶ CLOSED
HALF_OPEN ──(失败)──────────▶ OPEN（指数退避）
```

### 异常层次

```
MinioSyncError
├── ConfigError            # 配置错误
├── MinioConnectionError   # 连接异常
├── DownloadError          # 下载异常
│   └── IntegrityError     # 文件完整性校验失败
├── CircuitBreakerOpenError # 熔断器已打开
└── StoreError             # 持久化存储错误
```

## 运行时文件

程序在工作目录生成以下文件（已加入 `.gitignore`）：

| 文件 | 说明 |
|------|------|
| `minio_sync_state.db` | SQLite 数据库，存储同步时间戳、失败记录、待下载队列 |
| `logs/minio_sync.log` | 轮转日志文件 |

## 开发

```bash
# 单元测试
uv run pytest

# 代码质量
uv run ruff check src tests
uv run mypy src

# 格式化
uv run ruff format src tests
```

## 项目结构

```
MinIO_Sync/
├── pyproject.toml            # 项目元数据 + 依赖 + 工具配置
├── README.md
├── .env.example              # 环境变量模板（不含真实密钥）
├── .gitignore
├── .python-version           # Python 版本锁定（3.11）
├── src/minio_sync/           # 主包
│   ├── __init__.py           # 包导出 + 版本号
│   ├── __main__.py           # python -m minio_sync 入口
│   ├── config.py             # 配置加载（默认值 ← 环境变量 ← CLI）
│   ├── client.py             # MinIO 客户端管理（线程本地存储）
│   ├── clock.py              # 时钟偏移检测与服务器时间估算
│   ├── store.py              # SQLite 持久化存储（时间戳/失败记录/队列）
│   ├── downloader.py         # 文件下载（单文件 + 多线程批量）
│   ├── circuit_breaker.py    # 熔断器（CLOSED/OPEN/HALF_OPEN）
│   ├── sync_engine.py        # 同步核心逻辑（重试失败 + 增量同步）
│   ├── event_listener.py     # 事件监听 + 队列下载工作线程
│   ├── scheduler.py          # 混合调度策略 + 优雅退出
│   ├── logging_setup.py      # 日志系统（控制台 + 轮转文件）
│   └── exceptions.py         # 自定义异常层次
├── tests/                    # 单元测试
│   ├── conftest.py           # 共享 fixture
│   ├── test_circuit_breaker.py
│   ├── test_clock.py
│   ├── test_config.py
│   ├── test_downloader.py
│   ├── test_store.py
│   └── test_sync_engine.py
└── scripts/
    └── check_notification.py # 事件通知可用性检测（4步验证）
```