"""持久化存储：同步时间戳、失败记录、待下载队列。

基于 SQLite 实现，并发安全、单行操作高效、无需全量读写。
"""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

FileInfo = dict[str, Any]
FailedRecord = dict[str, Any]

_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class StateStore(Protocol):
    """持久化层抽象。"""

    def load_last_sync_time(self) -> datetime: ...
    def save_last_sync_time(self, dt: datetime) -> None: ...
    def load_failed_files(self) -> list[FailedRecord]: ...
    def save_failed_files(self, records: list[FailedRecord]) -> None: ...
    def load_pending_queue(self) -> list[FileInfo]: ...
    def save_pending_queue(self, items: list[FileInfo]) -> None: ...
    def add_pending_item(self, item: FileInfo) -> None: ...
    def remove_pending_item(self, path: str) -> None: ...
    def add_failed_record(self, path: str, retry_count: int = 0) -> None: ...
    def remove_failed_record(self, path: str) -> None: ...
    def increment_failed_retry(self, path: str) -> None: ...


class SqliteStateStore:
    """基于 SQLite 的存储实现：并发安全、单行操作高效、无需全量读写。"""

    def __init__(self, db_path: str, initial_sync_time: str) -> None:
        self._db_path = Path(db_path)
        self._initial_sync_time = initial_sync_time
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS failed_files (
                path TEXT PRIMARY KEY,
                retry_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS pending_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                size INTEGER,
                etag TEXT,
                last_modified TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_path
                ON pending_queue(path);
        """)
        conn.commit()
        logger.info(f"状态数据库已初始化: {self._db_path}")

    # ---- 同步时间戳 ----

    def load_last_sync_time(self) -> datetime:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT value FROM sync_state WHERE key = 'last_sync_time'"
        ).fetchone()
        if row is None:
            logger.info(f"无历史同步记录，使用初始同步时间: {self._initial_sync_time}")
            return self._parse_time(self._initial_sync_time)
        try:
            return self._parse_time(row["value"])
        except ValueError as e:
            logger.warning(f"同步时间解析失败，使用初始同步时间: {e}")
            return self._parse_time(self._initial_sync_time)

    def save_last_sync_time(self, dt: datetime) -> None:
        conn = self._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
            ("last_sync_time", dt.strftime(_TIME_FORMAT)),
        )
        conn.commit()
        logger.info(f"同步时间戳已更新: {dt.strftime(_TIME_FORMAT)}")

    # ---- 失败记录 ----

    def load_failed_files(self) -> list[FailedRecord]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT path, retry_count FROM failed_files ORDER BY path"
        ).fetchall()
        return [{"path": r["path"], "retry_count": r["retry_count"]} for r in rows]

    def save_failed_files(self, records: list[FailedRecord]) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM failed_files")
        conn.executemany(
            "INSERT INTO failed_files (path, retry_count) VALUES (?, ?)",
            [(r["path"], r.get("retry_count", 0)) for r in records],
        )
        conn.commit()
        logger.debug(f"失败记录已保存: {len(records)} 条")

    def add_failed_record(self, path: str, retry_count: int = 0) -> None:
        conn = self._get_conn()
        existing = conn.execute(
            "SELECT retry_count FROM failed_files WHERE path = ?", (path,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE failed_files SET retry_count = retry_count + 1 WHERE path = ?",
                (path,),
            )
            logger.debug(f"失败记录重试计数+1: {path} (原计数: {existing['retry_count']})")
        else:
            conn.execute(
                "INSERT INTO failed_files (path, retry_count) VALUES (?, ?)",
                (path, retry_count),
            )
            logger.info(f"新增失败记录: {path} (初始重试计数: {retry_count})")
        conn.commit()

    def remove_failed_record(self, path: str) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM failed_files WHERE path = ?", (path,))
        conn.commit()
        logger.info(f"失败记录已移除: {path}")

    def increment_failed_retry(self, path: str) -> None:
        conn = self._get_conn()
        conn.execute(
            "UPDATE failed_files SET retry_count = retry_count + 1 WHERE path = ?",
            (path,),
        )
        conn.commit()
        logger.debug(f"失败记录重试计数+1: {path}")

    # ---- 待下载队列 ----

    def load_pending_queue(self) -> list[FileInfo]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT path, size, etag, last_modified FROM pending_queue ORDER BY id"
        ).fetchall()
        result: list[FileInfo] = []
        for r in rows:
            item: FileInfo = {"path": r["path"]}
            if r["size"] is not None:
                item["size"] = r["size"]
            if r["etag"] is not None:
                item["etag"] = r["etag"]
            if r["last_modified"] is not None:
                item["last_modified"] = r["last_modified"]
            result.append(item)
        return result

    def save_pending_queue(self, items: list[FileInfo]) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM pending_queue")
        conn.executemany(
            "INSERT INTO pending_queue (path, size, etag, last_modified) VALUES (?, ?, ?, ?)",
            [
                (
                    item["path"],
                    item.get("size"),
                    item.get("etag"),
                    item.get("last_modified"),
                )
                for item in items
            ],
        )
        conn.commit()
        logger.debug(f"待下载队列已保存: {len(items)} 条")

    def add_pending_item(self, item: FileInfo) -> None:
        conn = self._get_conn()
        existing = conn.execute(
            "SELECT 1 FROM pending_queue WHERE path = ?", (item["path"],)
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO pending_queue (path, size, etag, last_modified) VALUES (?, ?, ?, ?)",
                (
                    item["path"],
                    item.get("size"),
                    item.get("etag"),
                    item.get("last_modified"),
                ),
            )
            conn.commit()
            logger.debug(f"新增待下载项: {item['path']}")

    def remove_pending_item(self, path: str) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM pending_queue WHERE path = ?", (path,))
        conn.commit()
        logger.debug(f"待下载项已移除: {path}")

    # ---- 工具 ----

    @staticmethod
    def _parse_time(time_str: str) -> datetime:
        return datetime.strptime(time_str, _TIME_FORMAT).replace(tzinfo=UTC)


def persist_queue_snapshot(
    download_queue: queue.Queue[FileInfo], store: StateStore
) -> None:
    """将内存下载队列快照持久化，避免优雅关闭时数据丢失。"""
    items: list[FileInfo] = []
    temp_list: list[FileInfo] = []
    try:
        while True:
            item = download_queue.get_nowait()
            items.append(item)
            temp_list.append(item)
    except queue.Empty:
        pass
    for item in temp_list:
        try:
            download_queue.put(item, timeout=1)
        except queue.Full:
            break
    if items:
        existing = store.load_pending_queue()
        existing_paths = {r["path"] for r in existing}
        for item in items:
            if item["path"] not in existing_paths:
                existing.append(item)
        store.save_pending_queue(existing)
        logger.info(f"下载队列快照已持久化: {len(items)} 个待下载文件")
    else:
        logger.debug("下载队列为空，无需持久化")


__all__ = [
    "FileInfo",
    "FailedRecord",
    "StateStore",
    "SqliteStateStore",
    "persist_queue_snapshot",
]
