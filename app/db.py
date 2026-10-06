"""SQLite 数据访问（图片上传监控系统）

表结构总览：
  meta                 系统元信息（首次取数时间、调度状态等）
  settings             界面可改的运行参数（优先级高于 .env）
  poll_runs            每次取数运行记录
  publish_records      领星上传队列快照（按 queue_unique_id 唯一）
  record_status_history 状态变化历史（仅在状态/结果变化时追加）
  follow_sessions      跟进会话（按 ASIN，同一 ASIN 同时只有一个 active）
  follow_events        跟进事件日志
  feishu_messages      飞书消息发送日志
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from app.config import settings
from app.config import now_iso

_write_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS poll_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_name  TEXT NOT NULL DEFAULT 'schedule',
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    status        TEXT NOT NULL DEFAULT 'running',
    window_start  TEXT,
    window_end    TEXT,
    http_requests INTEGER NOT NULL DEFAULT 0,
    api_total     INTEGER NOT NULL DEFAULT 0,
    fetched       INTEGER NOT NULL DEFAULT 0,
    inserted      INTEGER NOT NULL DEFAULT 0,
    updated       INTEGER NOT NULL DEFAULT 0,
    changed       INTEGER NOT NULL DEFAULT 0,
    processing    INTEGER NOT NULL DEFAULT 0,
    success       INTEGER NOT NULL DEFAULT 0,
    failed        INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    detail        TEXT
);
CREATE INDEX IF NOT EXISTS idx_poll_runs_started ON poll_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS publish_records (
    queue_unique_id  TEXT PRIMARY KEY,
    parent_asin      TEXT NOT NULL DEFAULT '',
    child_asin       TEXT NOT NULL DEFAULT '',
    msku             TEXT NOT NULL DEFAULT '',
    fnsku            TEXT NOT NULL DEFAULT '',
    item_name        TEXT NOT NULL DEFAULT '',
    store_name       TEXT NOT NULL DEFAULT '',
    country_name     TEXT NOT NULL DEFAULT '',
    marketplace_id   TEXT NOT NULL DEFAULT '',
    queue_type       INTEGER,
    queue_type_name  TEXT NOT NULL DEFAULT '',
    status           INTEGER,
    status_name      TEXT NOT NULL DEFAULT '',
    status_text      TEXT NOT NULL DEFAULT '',
    failure_reason   TEXT NOT NULL DEFAULT '',
    user_name        TEXT NOT NULL DEFAULT '',
    operate_time     TEXT,
    finish_time      TEXT,
    asin_url         TEXT NOT NULL DEFAULT '',
    main_image_url   TEXT NOT NULL DEFAULT '',
    is_archive       INTEGER NOT NULL DEFAULT 0,
    revision         TEXT NOT NULL DEFAULT '',
    raw_json         TEXT NOT NULL DEFAULT '',
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    status_changed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_records_parent_asin ON publish_records (parent_asin);
CREATE INDEX IF NOT EXISTS idx_records_status ON publish_records (status);
CREATE INDEX IF NOT EXISTS idx_records_operate ON publish_records (operate_time);
CREATE INDEX IF NOT EXISTS idx_records_last_seen ON publish_records (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS record_status_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_unique_id  TEXT NOT NULL,
    parent_asin      TEXT NOT NULL DEFAULT '',
    child_asin       TEXT NOT NULL DEFAULT '',
    status           INTEGER,
    status_name      TEXT NOT NULL DEFAULT '',
    status_text      TEXT NOT NULL DEFAULT '',
    failure_reason   TEXT NOT NULL DEFAULT '',
    operate_time     TEXT,
    finish_time      TEXT,
    change_kind      TEXT NOT NULL DEFAULT 'status_change',
    observed_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_asin ON record_status_history (parent_asin, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_history_queue ON record_status_history (queue_unique_id);

CREATE TABLE IF NOT EXISTS follow_sessions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_asin           TEXT NOT NULL,
    source                TEXT NOT NULL DEFAULT 'api_request',
    state                 TEXT NOT NULL DEFAULT 'active',
    owner                 TEXT NOT NULL DEFAULT '',
    request_ts            TEXT,
    request_payload       TEXT NOT NULL DEFAULT '',
    attempts              INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL DEFAULT 5,
    round_started_at      TEXT,
    round_deadline_at     TEXT,
    next_tick_at          TEXT,
    last_tick_at          TEXT,
    baseline_revision     TEXT,
    baseline_observed_at  TEXT,
    action_revision       TEXT DEFAULT '',
    last_revision         TEXT,
    last_observed_at      TEXT,
    last_status           INTEGER,
    last_status_name      TEXT NOT NULL DEFAULT '',
    last_status_text      TEXT NOT NULL DEFAULT '',
    last_failure_reason   TEXT NOT NULL DEFAULT '',
    observed_since_round  INTEGER NOT NULL DEFAULT 0,
    notify_on_failure     INTEGER NOT NULL DEFAULT 1,
    messages_sent         INTEGER NOT NULL DEFAULT 0,
    opened_at             TEXT NOT NULL,
    success_at            TEXT,
    closed_at             TEXT,
    close_reason          TEXT NOT NULL DEFAULT '',
    notes                 TEXT NOT NULL DEFAULT '',
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_state ON follow_sessions (state, next_tick_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_active_asin
    ON follow_sessions (parent_asin) WHERE state = 'active';

CREATE TABLE IF NOT EXISTS follow_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    parent_asin TEXT NOT NULL DEFAULT '',
    event      TEXT NOT NULL,
    detail     TEXT NOT NULL DEFAULT '',
    actor      TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_session ON follow_events (session_id, id DESC);

CREATE TABLE IF NOT EXISTS feishu_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL DEFAULT 'text',
    parent_asin TEXT NOT NULL DEFAULT '',
    session_id  INTEGER,
    targets     TEXT NOT NULL DEFAULT '',
    content     TEXT NOT NULL DEFAULT '',
    ok          INTEGER NOT NULL DEFAULT 0,
    error       TEXT NOT NULL DEFAULT '',
    dedupe_key  TEXT NOT NULL DEFAULT '',
    retried_at  TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_feishu_created ON feishu_messages (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feishu_asin ON feishu_messages (parent_asin, kind, created_at DESC);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def _connection() -> Iterator[sqlite3.Connection]:
    """显式关闭连接：sqlite3 的 with 语句只管事务，不会关闭连接"""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def init_schema() -> None:
    db_dir = os.path.dirname(os.path.abspath(settings.db_path))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with _write_lock, _connection() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)
        # 进程重启后，把上次没跑完的取数任务标记为中断（避免界面上一直显示「运行中」）
        conn.execute(
            "UPDATE poll_runs SET status='error', error='进程重启，任务中断', finished_at=? "
            "WHERE status='running'",
            (now_iso(),),
        )
        conn.commit()


def _migrate(conn: sqlite3.Connection) -> None:
    """老库补列（SQLite 的 ALTER TABLE ADD COLUMN 幂等处理）"""
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(feishu_messages)")}
        if "retried_at" not in columns:
            conn.execute("ALTER TABLE feishu_messages ADD COLUMN retried_at TEXT NOT NULL DEFAULT ''")
    except sqlite3.Error as exc:  # pragma: no cover - 迁移失败不阻塞启动
        import logging

        logging.getLogger("monitor.db").warning("数据库补列失败：%s", exc)


def execute(sql: str, *params: Any) -> int:
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        params = tuple(params[0])
    with _write_lock, _connection() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount


def insert(sql: str, *params: Any) -> int:
    """插入并返回自增主键（写锁内完成，避免并发取「最后一条」出错）"""
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        params = tuple(params[0])
    with _write_lock, _connection() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return int(cur.lastrowid or 0)


def fetchone(sql: str, *params: Any) -> Optional[dict]:
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        params = tuple(params[0])
    with _connection() as conn:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None


def fetchall(sql: str, *params: Any) -> list[dict]:
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        params = tuple(params[0])
    with _connection() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_meta(key: str, default: str = "") -> str:
    row = fetchone("SELECT value FROM meta WHERE key = ?", key)
    return (row or {}).get("value") or default


def set_meta(key: str, value: str) -> None:
    execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        key,
        str(value),
    )


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    with _write_lock, _connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
