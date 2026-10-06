"""业务核心：落库、状态分析、跟进状态机、飞书触发

跟进状态机（每个 ASIN 一个 active 会话）：
  1. 收到跟进请求 / 取数时发现「处理中」→ 建立会话，每 10 分钟查一次最新状态
  2. 观察到「失败」→ 发送重试消息（飞书），重新进入跟进状态
  3. 每个跟进轮次最长 30 分钟：轮次内没有出现更新的 ASIN 状态 → 判定重试消息失效，重发一次
  4. 重试消息最多发送 follow_max_attempts（默认 5）次，仍失败 → 发送「XX ASIN上传失败，人工检查图片」
  5. 观察到「成功」→ 结束跟进，发送「XX成功上传产品图片」
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

from app import db
from app import lingxing
from app import settings_store as store
from app.config import now_dt, now_iso, parse_dt
from app.feishu import FeishuNotifier

logger = logging.getLogger("monitor.services")

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_PROCESSING = "processing"
STATUS_UNKNOWN = "unknown"

_poll_lock = threading.Lock()
_eval_lock = threading.Lock()

ASIN_RE = re.compile(r"^[A-Z0-9]{6,20}$")


# ────────────────────────── 通用工具 ──────────────────────────

def _dt(value) -> Optional[dt.datetime]:
    return parse_dt(value)


def _iso(value) -> Optional[str]:
    parsed = parse_dt(value)
    return parsed.replace(microsecond=0).isoformat() if parsed else None


def normalize_asin(value: Any) -> str:
    return str(value or "").strip().upper()


def validate_asin(value: Any) -> str:
    asin = normalize_asin(value)
    if not asin:
        raise ValueError("ASIN 不能为空")
    if not ASIN_RE.match(asin):
        raise ValueError(f"ASIN 格式不正确：{asin}（应为 6-20 位字母数字）")
    return asin


def _ceil_tick(base: dt.datetime, interval_minutes: int) -> str:
    """下一个检查时间：base + interval"""
    return (base + dt.timedelta(minutes=max(1, interval_minutes))).replace(microsecond=0).isoformat()


# ────────────────────────── 落库 ──────────────────────────

def save_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """把一次取数结果写入快照表；状态变化时追加历史。

    Returns: inserted / updated / changed / changed_asins / records_by_asin
    """
    inserted = 0
    updated = 0
    changed = 0
    changed_asins: List[str] = []
    observed_at = now_iso()

    with db.transaction() as conn:
        for record in records:
            queue_id = str(record.get("queue_unique_id") or "").strip()
            if not queue_id:
                continue
            revision = lingxing.revision_of(record)
            existing = conn.execute(
                "SELECT queue_unique_id, revision, status FROM publish_records "
                "WHERE queue_unique_id = ?",
                (queue_id,),
            ).fetchone()
            values = (
                record.get("parent_asin", ""),
                record.get("child_asin", ""),
                record.get("msku", ""),
                record.get("fnsku", ""),
                record.get("item_name", ""),
                record.get("store_name", ""),
                record.get("country_name", ""),
                record.get("marketplace_id", ""),
                record.get("queue_type"),
                record.get("queue_type_name", ""),
                record.get("status"),
                record.get("status_name", ""),
                record.get("status_text", ""),
                record.get("failure_reason", ""),
                record.get("user_name", ""),
                _iso(record.get("operate_time")),
                _iso(record.get("finish_time")),
                record.get("asin_url", ""),
                record.get("main_image_url", ""),
                1 if str(record.get("is_archive") or 0) in ("1", "True", "true") else 0,
                revision,
                record.get("raw_json", ""),
            )
            if existing is None:
                inserted += 1
                conn.execute(
                    """
                    INSERT INTO publish_records (
                        parent_asin, child_asin, msku, fnsku, item_name, store_name,
                        country_name, marketplace_id, queue_type, queue_type_name,
                        status, status_name, status_text, failure_reason, user_name,
                        operate_time, finish_time, asin_url, main_image_url, is_archive,
                        revision, raw_json, queue_unique_id,
                        first_seen_at, last_seen_at, status_changed_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (*values, queue_id, observed_at, observed_at, observed_at),
                )
                conn.execute(
                    """
                    INSERT INTO record_status_history (
                        queue_unique_id, parent_asin, child_asin, status, status_name,
                        status_text, failure_reason, operate_time, finish_time,
                        change_kind, observed_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        queue_id,
                        record.get("parent_asin", ""),
                        record.get("child_asin", ""),
                        record.get("status"),
                        record.get("status_name", ""),
                        record.get("status_text", ""),
                        record.get("failure_reason", ""),
                        _iso(record.get("operate_time")),
                        _iso(record.get("finish_time")),
                        "new",
                        observed_at,
                    ),
                )
                asin = record.get("parent_asin", "")
                if asin and asin not in changed_asins:
                    changed_asins.append(asin)
                continue

            updated += 1
            status_changed = existing["revision"] != revision
            if status_changed:
                changed += 1
                new_status_changed_at = (
                    observed_at
                    if existing["status"] != record.get("status")
                    else None
                )
                conn.execute(
                    """
                    UPDATE publish_records SET
                        parent_asin=?, child_asin=?, msku=?, fnsku=?, item_name=?,
                        store_name=?, country_name=?, marketplace_id=?, queue_type=?,
                        queue_type_name=?, status=?, status_name=?, status_text=?,
                        failure_reason=?, user_name=?, operate_time=?, finish_time=?,
                        asin_url=?, main_image_url=?, is_archive=?, revision=?,
                        raw_json=?, last_seen_at=?,
                        status_changed_at=COALESCE(?, status_changed_at)
                     WHERE queue_unique_id=?
                    """,
                    (*values, observed_at, new_status_changed_at, queue_id),
                )
                conn.execute(
                    """
                    INSERT INTO record_status_history (
                        queue_unique_id, parent_asin, child_asin, status, status_name,
                        status_text, failure_reason, operate_time, finish_time,
                        change_kind, observed_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        queue_id,
                        record.get("parent_asin", ""),
                        record.get("child_asin", ""),
                        record.get("status"),
                        record.get("status_name", ""),
                        record.get("status_text", ""),
                        record.get("failure_reason", ""),
                        _iso(record.get("operate_time")),
                        _iso(record.get("finish_time")),
                        "status_change",
                        observed_at,
                    ),
                )
                asin = record.get("parent_asin", "")
                if asin and asin not in changed_asins:
                    changed_asins.append(asin)
            else:
                conn.execute(
                    "UPDATE publish_records SET last_seen_at=? WHERE queue_unique_id=?",
                    (observed_at, queue_id),
                )

    return {
        "inserted": inserted,
        "updated": updated,
        "changed": changed,
        "changed_asins": changed_asins,
        "observed_at": observed_at,
    }


# ────────────────────────── 查询 ──────────────────────────

LATEST_ORDER = (
    "ORDER BY LENGTH(queue_unique_id) DESC, queue_unique_id DESC, "
    "COALESCE(finish_time,'') DESC, COALESCE(operate_time,'') DESC"
)


def latest_records() -> Dict[str, Dict[str, Any]]:
    """每个 ASIN 的最新一条记录"""
    rows = db.fetchall(
        f"""
        SELECT * FROM (
            SELECT pr.*, ROW_NUMBER() OVER (
                PARTITION BY parent_asin
                {LATEST_ORDER}
            ) AS rn
              FROM publish_records pr
             WHERE parent_asin <> ''
        ) WHERE rn = 1
        """
    )
    return {row["parent_asin"]: row for row in rows}


def latest_record_for(asin: str) -> Optional[Dict[str, Any]]:
    return db.fetchone(
        f"SELECT * FROM publish_records WHERE parent_asin = ? {LATEST_ORDER} LIMIT 1",
        normalize_asin(asin),
    )


def records_for(asin: str, limit: int = 50) -> List[Dict[str, Any]]:
    return db.fetchall(
        f"SELECT * FROM publish_records WHERE parent_asin = ? {LATEST_ORDER} LIMIT ?",
        normalize_asin(asin),
        int(limit),
    )


def history_for(asin: str, limit: int = 100) -> List[Dict[str, Any]]:
    return db.fetchall(
        """
        SELECT * FROM record_status_history
         WHERE parent_asin = ?
         ORDER BY id DESC LIMIT ?
        """,
        normalize_asin(asin),
        int(limit),
    )


def query_records(
    *,
    asin: str = "",
    status_text: str = "",
    store_name: str = "",
    keyword: str = "",
    country_name: str = "",
    changed_from: str = "",
    changed_to: str = "",
    page: int = 1,
    page_size: int = 50,
    order: str = "operate_desc",
) -> Dict[str, Any]:
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 500)
    conds: List[str] = []
    params: List[Any] = []

    asin_text = normalize_asin(asin)
    if asin_text:
        conds.append("parent_asin LIKE ?")
        params.append(f"%{asin_text}%")
    if status_text:
        if status_text == STATUS_PROCESSING:
            conds.append("status_text NOT IN ('success','failed')")
        else:
            conds.append("status_text = ?")
            params.append(status_text)
    if store_name:
        conds.append("store_name LIKE ?")
        params.append(f"%{store_name}%")
    if country_name:
        conds.append("country_name LIKE ?")
        params.append(f"%{country_name}%")
    keyword_text = str(keyword or "").strip()
    if keyword_text:
        like = f"%{keyword_text}%"
        conds.append(
            "(item_name LIKE ? OR msku LIKE ? OR fnsku LIKE ? OR child_asin LIKE ? "
            "OR user_name LIKE ? OR failure_reason LIKE ? OR queue_unique_id LIKE ?)"
        )
        params.extend([like] * 7)
    if changed_from:
        conds.append("COALESCE(finish_time, operate_time, first_seen_at) >= ?")
        params.append(str(changed_from).strip().replace(" ", "T"))
    if changed_to:
        conds.append("COALESCE(finish_time, operate_time, first_seen_at) <= ?")
        params.append(str(changed_to).strip().replace(" ", "T"))

    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    order_sql = {
        "operate_desc": "ORDER BY COALESCE(finish_time, operate_time) DESC, queue_unique_id DESC",
        "operate_asc": "ORDER BY COALESCE(finish_time, operate_time) ASC, queue_unique_id ASC",
        "seen_desc": "ORDER BY last_seen_at DESC, queue_unique_id DESC",
        "status": "ORDER BY status ASC, COALESCE(finish_time, operate_time) DESC",
    }.get(order, "ORDER BY COALESCE(finish_time, operate_time) DESC, queue_unique_id DESC")

    total_row = db.fetchone(f"SELECT COUNT(*) AS n FROM publish_records {where}", params) or {}
    rows = db.fetchall(
        f"""
        SELECT * FROM publish_records
        {where}
        {order_sql}
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, (page - 1) * page_size],
    )
    return {
        "items": rows,
        "total": int(total_row.get("n") or 0),
        "page": page,
        "page_size": page_size,
    }


def asin_status(*asins: str) -> List[Dict[str, Any]]:
    """按 ASIN 查询最新状态（供第三方脚本查询）"""
    out: List[Dict[str, Any]] = []
    for raw in asins:
        asin = normalize_asin(raw)
        if not asin:
            continue
        record = latest_record_for(asin)
        session = db.fetchone(
            "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1",
            asin,
        )
        out.append(
            {
                "asin": asin,
                "found": bool(record),
                "status": (record or {}).get("status"),
                "status_name": (record or {}).get("status_name", ""),
                "status_text": (record or {}).get("status_text", STATUS_UNKNOWN),
                "status_text_cn": status_text_cn((record or {}).get("status_text")),
                "failure_reason": (record or {}).get("failure_reason", ""),
                "finish_time": (record or {}).get("finish_time"),
                "operate_time": (record or {}).get("operate_time"),
                "queue_unique_id": (record or {}).get("queue_unique_id"),
                "store_name": (record or {}).get("store_name", ""),
                "item_name": (record or {}).get("item_name", ""),
                "last_seen_at": (record or {}).get("last_seen_at"),
                "follow_state": (session or {}).get("state", ""),
                "follow_attempts": (session or {}).get("attempts", 0),
                "follow_owner": (session or {}).get("owner", ""),
            }
        )
    return out


def status_text_cn(text: str) -> str:
    return {
        STATUS_SUCCESS: "成功",
        STATUS_FAILED: "失败",
        STATUS_PROCESSING: "处理中",
        STATUS_UNKNOWN: "未知",
    }.get(text or "", "未知")


# ────────────────────────── 跟进会话 ──────────────────────────

def _event(session_id: Optional[int], asin: str, event: str, detail: str = "", actor: str = "") -> None:
    db.execute(
        "INSERT INTO follow_events (session_id, parent_asin, event, detail, actor, created_at) "
        "VALUES (?,?,?,?,?,?)",
        session_id,
        asin,
        event,
        detail[:2000],
        actor,
        now_iso(),
    )


def open_session(
    asin: str,
    *,
    source: str = "api_request",
    owner: str = "",
    request_ts: Any = None,
    payload: Any = None,
    notify: Optional[bool] = None,
    record: Optional[Dict[str, Any]] = None,
    actor: str = "system",
) -> Tuple[Dict[str, Any], bool]:
    """建立（或复用）跟进会话；返回 (会话, 是否新建)"""
    asin = validate_asin(asin)
    cfg = store.snapshot()
    round_minutes = int(cfg.get("follow_round_minutes", 30))
    interval = int(cfg.get("follow_interval_minutes", 10))
    now = now_dt()
    now_text = now.replace(microsecond=0).isoformat()
    # 第三方推送 / 人工建立的跟进：先等一会儿再第一次取数，
    # 避免「上传程序刚推完、领星还没更新状态」时误判成失败
    delay_minutes = (
        int(cfg.get("follow_request_delay_minutes", 2) or 0)
        if source in ("api_request", "manual")
        else 0
    )
    first_check = now + dt.timedelta(minutes=max(0, delay_minutes))
    first_check_text = first_check.replace(microsecond=0).isoformat()
    record = record if record is not None else latest_record_for(asin)
    revision = lingxing.revision_of(record) if record else ""
    if notify is None:
        notify = source in ("api_request", "manual")
    owner_text = str(owner or "").strip()
    if not owner_text and cfg.get("owner_fallback_to_operator"):
        # 默认关闭：负责人严格来自接口参数；开启后才用领星记录里的操作人补位
        owner_text = str((record or {}).get("user_name") or "").strip()

    existing = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? AND state='active'", asin
    )
    if existing:
        fields = {
            "updated_at": now_text,
            # 已有会话不受延迟影响；但若它还没做过第一次检查，按新的延迟顺延
            "next_tick_at": existing.get("next_tick_at") or first_check_text,
        }
        # 新的跟进请求带来的负责人优先（第三方每次推送的都是最新负责人）
        if owner_text:
            fields["owner"] = owner_text
        if request_ts:
            fields["request_ts"] = str(request_ts)
        if payload is not None:
            fields["request_payload"] = json.dumps(payload, ensure_ascii=False)[:2000]
        if notify:
            fields["notify_on_failure"] = 1
        _update_session(existing["id"], **fields)
        _event(existing["id"], asin, "request_repeat", "重复跟进请求，沿用现有会话", actor)
        return db.fetchone("SELECT * FROM follow_sessions WHERE id=?", existing["id"]), False

    # 自动会话数量上限，避免接口里一大堆历史「处理中」把会话挤爆
    if source.startswith("auto"):
        active_count = db.fetchone(
            "SELECT COUNT(*) AS n FROM follow_sessions WHERE state='active'"
        ) or {}
        if int(active_count.get("n") or 0) >= int(cfg.get("auto_follow_max_sessions", 200)):
            raise RuntimeError("自动跟进会话已达上限，跳过")

    deadline = first_check + dt.timedelta(minutes=round_minutes)
    session_id = db.insert(
        """
        INSERT INTO follow_sessions (
            parent_asin, source, state, owner, request_ts, request_payload,
            attempts, max_attempts, round_started_at, round_deadline_at, next_tick_at,
            baseline_revision, baseline_observed_at, last_revision, last_observed_at,
            last_status, last_status_name, last_status_text, last_failure_reason,
            observed_since_round, notify_on_failure, opened_at, created_at, updated_at
        ) VALUES (?,?,'active',?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?)
        """,
        asin,
        source,
        owner_text,
        str(request_ts) if request_ts else None,
        json.dumps(payload, ensure_ascii=False)[:2000] if payload is not None else "",
        int(cfg.get("follow_max_attempts", 5)),
        now_text,
        deadline.replace(microsecond=0).isoformat(),
        first_check_text,
        revision,
        now_text,
        revision,
        now_text,
        (record or {}).get("status"),
        (record or {}).get("status_name", ""),
        (record or {}).get("status_text", STATUS_UNKNOWN),
        (record or {}).get("failure_reason", ""),
        1 if notify else 0,
        now_text,
        now_text,
        now_text,
    )
    session = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", session_id)
    _event(
        session_id,
        asin,
        "session_open",
        f"来源={source} 负责人={owner_text or '-'} 当前状态={status_text_cn((record or {}).get('status_text', STATUS_UNKNOWN))}",
        actor,
    )
    return session or {}, True


SESSION_FIELDS = {
    "state",
    "owner",
    "request_ts",
    "request_payload",
    "attempts",
    "max_attempts",
    "round_started_at",
    "round_deadline_at",
    "next_tick_at",
    "last_tick_at",
    "baseline_revision",
    "baseline_observed_at",
    "action_revision",
    "last_revision",
    "last_observed_at",
    "last_status",
    "last_status_name",
    "last_status_text",
    "last_failure_reason",
    "observed_since_round",
    "notify_on_failure",
    "messages_sent",
    "success_at",
    "closed_at",
    "close_reason",
    "notes",
    "updated_at",
}


def _update_session(session_id: int, **fields) -> None:
    payload = {k: v for k, v in fields.items() if k in SESSION_FIELDS}
    if not payload:
        return
    payload.setdefault("updated_at", now_iso())
    columns = ", ".join(f"{key}=?" for key in payload)
    db.execute(
        f"UPDATE follow_sessions SET {columns} WHERE id=?",
        [*payload.values(), session_id],
    )


def close_session(session_id: int, *, state: str = "closed", reason: str = "", actor: str = "system") -> None:
    now_text = now_iso()
    fields = {
        "state": state,
        "closed_at": now_text,
        "close_reason": reason,
        "next_tick_at": None,
    }
    if state == "completed":
        fields["success_at"] = now_text
    _update_session(session_id, **fields)
    session = db.fetchone("SELECT parent_asin FROM follow_sessions WHERE id=?", session_id) or {}
    _event(session_id, session.get("parent_asin", ""), f"session_{state}", reason, actor)


def cancel_session(asin: str, *, actor: str = "ui") -> bool:
    asin = normalize_asin(asin)
    session = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? AND state='active'", asin
    )
    if not session:
        return False
    close_session(session["id"], state="cancelled", reason="人工结束跟进", actor=actor)
    return True


def list_sessions(
    *,
    state: str = "",
    source: str = "",
    asin: str = "",
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 500)
    conds: List[str] = []
    params: List[Any] = []
    if state:
        conds.append("state = ?")
        params.append(state)
    if source:
        conds.append("source = ?")
        params.append(source)
    if asin:
        conds.append("parent_asin LIKE ?")
        params.append(f"%{normalize_asin(asin)}%")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    total_row = db.fetchone(f"SELECT COUNT(*) AS n FROM follow_sessions {where}", params) or {}
    rows = db.fetchall(
        f"""
        SELECT * FROM follow_sessions
        {where}
        ORDER BY CASE state WHEN 'active' THEN 0 ELSE 1 END,
                 COALESCE(next_tick_at,'') ASC, id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, (page - 1) * page_size],
    )
    for row in rows:
        row["status_text_cn"] = status_text_cn(row.get("last_status_text"))
    return {
        "items": rows,
        "total": int(total_row.get("n") or 0),
        "page": page,
        "page_size": page_size,
    }


def session_events(session_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    return db.fetchall(
        "SELECT * FROM follow_events WHERE session_id=? ORDER BY id DESC LIMIT ?",
        int(session_id),
        int(limit),
    )


def recent_events(limit: int = 100) -> List[Dict[str, Any]]:
    return db.fetchall(
        "SELECT * FROM follow_events ORDER BY id DESC LIMIT ?", int(limit)
    )


def _active_sessions() -> List[Dict[str, Any]]:
    return db.fetchall(
        "SELECT * FROM follow_sessions WHERE state='active' "
        "ORDER BY COALESCE(next_tick_at,'') ASC, id ASC"
    )


def due_sessions(now: Optional[dt.datetime] = None) -> List[Dict[str, Any]]:
    now = now or now_dt()
    now_text = now.replace(microsecond=0).isoformat()
    out = []
    for session in _active_sessions():
        tick = session.get("next_tick_at")
        deadline = session.get("round_deadline_at")
        if not tick or str(tick) <= now_text or (deadline and str(deadline) <= now_text):
            out.append(session)
    return out


def next_session_tick() -> Optional[str]:
    row = db.fetchone(
        "SELECT MIN(COALESCE(next_tick_at,'')) AS t FROM follow_sessions WHERE state='active'"
    ) or {}
    return row.get("t") or None


def active_session_count() -> int:
    row = db.fetchone("SELECT COUNT(*) AS n FROM follow_sessions WHERE state='active'") or {}
    return int(row.get("n") or 0)


# ────────────────────────── 跟进状态机 ──────────────────────────

def evaluate_sessions(
    *,
    only_session_ids: Optional[List[int]] = None,
    now: Optional[dt.datetime] = None,
    actor: str = "scheduler",
    force_all: bool = False,
) -> Dict[str, Any]:
    """对到期的跟进会话做一次判定（发送飞书消息）

    加锁串行执行：避免调度线程与手动检查同时判定同一会话导致重复发消息。
    force_all=False 时只判定「到检查时间」的会话（next_tick_at / round_deadline_at 已到），
    这样全局取数不会抢跑（例如推送后的延迟检查）；force_all=True 用于界面「立即检查」。
    """
    if not _eval_lock.acquire(blocking=False):
        return {"checked": 0, "skipped": "已有判定在执行中"}
    try:
        return _evaluate_sessions_locked(
            only_session_ids=only_session_ids, now=now, actor=actor, force_all=force_all
        )
    finally:
        _eval_lock.release()


def _evaluate_sessions_locked(
    *,
    only_session_ids: Optional[List[int]] = None,
    now: Optional[dt.datetime] = None,
    actor: str = "scheduler",
    force_all: bool = False,
) -> Dict[str, Any]:
    cfg = store.snapshot()
    notifier = FeishuNotifier(cfg)
    now = now or now_dt()
    now_text = now.replace(microsecond=0).isoformat()
    round_minutes = int(cfg.get("follow_round_minutes", 30))
    interval = int(cfg.get("follow_interval_minutes", 10))
    fail_cutoff = _dt(cfg.get("follow_start_at"))

    sessions = _active_sessions()
    if only_session_ids is not None:
        wanted = set(only_session_ids)
        sessions = [s for s in sessions if s["id"] in wanted]

    result = {
        "checked": 0,
        "success": 0,
        "retry": 0,
        "final_failure": 0,
        "waiting": 0,
        "not_due": 0,
        "skipped": 0,
    }
    now_text_for_due = now.replace(microsecond=0).isoformat()
    for session in sessions:
        session_id = session["id"]
        asin = session["parent_asin"]

        if not force_all:
            tick = session.get("next_tick_at")
            deadline_at = session.get("round_deadline_at")
            due = (not tick or str(tick) <= now_text_for_due) or (
                deadline_at and str(deadline_at) <= now_text_for_due
            )
            if not due:
                result["not_due"] += 1
                continue
        result["checked"] += 1

        record = latest_record_for(asin)
        revision = lingxing.revision_of(record) if record else ""
        status_text = (record or {}).get("status_text", STATUS_UNKNOWN)
        activity = _activity_dt(record) if record else None

        # 历史失败清理：自动建立、还没发过重试消息的会话，若失败时间早于
        # 「失败自动跟进起始时间」，说明属于过去失败 → 直接结束，不跟进
        if (
            str(session.get("source") or "").startswith("auto")
            and status_text == STATUS_FAILED
            and int(session.get("attempts") or 0) == 0
            and fail_cutoff
            and (activity is None or activity < fail_cutoff)
        ):
            close_session(
                session_id,
                state="cancelled",
                reason="历史失败（早于失败自动跟进起始时间），不跟进",
                actor=actor,
            )
            _event(
                session_id, asin, "skip_history_failure",
                f"失败时间 {record.get('finish_time')} 早于起始时间 {cfg.get('follow_start_at')}，已结束跟进",
                actor,
            )
            result["skipped"] += 1
            continue

        # 记录最新观测
        observed_new = revision != (session.get("last_revision") or "")
        fields: Dict[str, Any] = {
            "last_tick_at": now_text,
            "last_revision": revision,
            "last_observed_at": now_text,
            "last_status": (record or {}).get("status"),
            "last_status_name": (record or {}).get("status_name", ""),
            "last_status_text": status_text,
            "last_failure_reason": (record or {}).get("failure_reason", ""),
        }
        if observed_new:
            fields["observed_since_round"] = 1
            _event(
                session_id,
                asin,
                "status_observed",
                f"status={status_text} name={(record or {}).get('status_name','')} "
                f"finish={(record or {}).get('finish_time')}",
                actor,
            )

        deadline = _dt(session.get("round_deadline_at"))
        expired = bool(deadline and now >= deadline)
        attempts = int(session.get("attempts") or 0)
        max_attempts = int(session.get("max_attempts") or cfg.get("follow_max_attempts", 5))
        notify = bool(session.get("notify_on_failure"))
        owner = session.get("owner") or (
            (record or {}).get("user_name", "") if cfg.get("owner_fallback_to_operator") else ""
        )
        baseline = session.get("baseline_revision") or ""
        action_revision = session.get("action_revision") or ""
        round_is_new = revision != baseline
        max_hours = int(cfg.get("follow_session_max_hours", 0) or 0)
        opened = _dt(session.get("opened_at"))
        session_too_long = bool(
            max_hours and opened and now >= opened + dt.timedelta(hours=max_hours)
        )

        def restart_round(**extra) -> None:
            payload = {
                "round_started_at": now_text,
                "round_deadline_at": _ceil_tick(now, round_minutes),
                "next_tick_at": _ceil_tick(now, interval),
                "observed_since_round": 0,
            }
            payload.update(extra)
            fields.update(payload)

        def send_final(msg_ref: str) -> None:
            """收尾：超过重试上限/长时间未结束 → 人工检查"""
            _update_session(session_id, **fields)
            if notify and cfg.get("notify_final_failure", True):
                if notifier.notify_final_failure(
                    asin=asin,
                    owner=owner,
                    record=record,
                    attempt=max(attempts, max_attempts),
                    max_attempts=max_attempts,
                    session_id=session_id,
                ):
                    db.execute(
                        "UPDATE follow_sessions SET messages_sent = messages_sent + 1 WHERE id=?",
                        session_id,
                    )
            close_session(session_id, state="failed", reason=msg_ref, actor=actor)
            _event(session_id, asin, "final_failure", msg_ref, actor)
            result["final_failure"] += 1

        # ① 成功 → 结束跟进 + 成功通知
        if status_text == STATUS_SUCCESS:
            _update_session(session_id, **fields)
            close_session(session_id, state="completed", reason="ASIN 上传成功", actor=actor)
            if notify and cfg.get("notify_success", True):
                if notifier.notify_success(
                    asin=asin, owner=owner, record=record, session_id=session_id
                ):
                    db.execute(
                        "UPDATE follow_sessions SET messages_sent = messages_sent + 1 WHERE id=?",
                        session_id,
                    )
            result["success"] += 1
            continue

        # ② 会话超时保护（长时间未成功/失败 → 人工检查）
        if session_too_long:
            send_final(f"跟进超过 {max_hours} 小时仍未得到成功结果")
            continue

        # ③ 观察到失败且该失败还没被重试过 → 马上发重试消息
        if status_text == STATUS_FAILED and action_revision != revision:
            if not notify:
                _update_session(session_id, **fields)
                close_session(
                    session_id, state="failed", reason="观察到失败（该会话未开启通知）", actor=actor
                )
                _event(session_id, asin, "failed_silent", "会话未开启通知，仅记录失败", actor)
                result["final_failure"] += 1
                continue
            if attempts >= max_attempts:
                send_final(f"已重试 {attempts} 次仍失败")
                continue
            sent = False
            if cfg.get("notify_retry", True):
                sent = notifier.notify_retry(
                    asin=asin,
                    owner=owner,
                    record=record,
                    attempt=attempts + 1,
                    max_attempts=max_attempts,
                    session_id=session_id,
                )
            if sent:
                restart_round(
                    attempts=attempts + 1,
                    baseline_revision=revision,
                    baseline_observed_at=now_text,
                    action_revision=revision,
                    messages_sent=int(session.get("messages_sent") or 0) + 1,
                )
                _event(
                    session_id,
                    asin,
                    "retry_sent",
                    f"第 {attempts + 1} 次重试消息已发送（{round_minutes} 分钟内等待新状态）",
                    actor,
                )
                result["retry"] += 1
            else:
                fields["next_tick_at"] = _ceil_tick(now, interval)
                _event(session_id, asin, "retry_send_failed", "重试消息发送失败，下一轮重试", actor)
            _update_session(session_id, **fields)
            continue

        # ④ 处理中出现更新的状态数据 → 视为进展，刷新窗口继续跟进
        if round_is_new and status_text != STATUS_FAILED:
            restart_round(
                baseline_revision=revision,
                baseline_observed_at=now_text,
            )
            _update_session(session_id, **fields)
            _event(
                session_id, asin, "round_refreshed",
                f"检测到新的状态数据（{status_text_cn(status_text)}），继续跟进", actor,
            )
            result["waiting"] += 1
            continue

        # ⑤ 窗口未到 → 等待下一次检查
        if not expired:
            candidates = [x for x in [session.get("round_deadline_at"), _ceil_tick(now, interval)] if x]
            fields["next_tick_at"] = min(candidates)
            _update_session(session_id, **fields)
            result["waiting"] += 1
            continue

        # ⑥ 窗口超时且没有更新的 ASIN 状态数据 → 判定重试消息失效，重发一次
        if not notify:
            _update_session(session_id, **fields)
            close_session(
                session_id, state="closed", reason="跟进窗口超时（该会话未开启通知）", actor=actor
            )
            result["skipped"] += 1
            continue
        if attempts >= max_attempts:
            send_final(f"重试 {attempts} 次后仍无成功/失败结果")
            continue
        sent = False
        if cfg.get("notify_retry", True):
            sent = notifier.notify_retry(
                asin=asin,
                owner=owner,
                record=record,
                attempt=attempts + 1,
                max_attempts=max_attempts,
                session_id=session_id,
                force=True,
            )
        if sent:
            restart_round(
                attempts=attempts + 1,
                action_revision=revision or action_revision,
                messages_sent=int(session.get("messages_sent") or 0) + 1,
            )
            _event(
                session_id, asin, "retry_resent",
                f"{round_minutes} 分钟内没有新的 ASIN 状态数据，判定重试消息失效，已重发第 {attempts + 1} 次",
                actor,
            )
            result["retry"] += 1
        else:
            fields["next_tick_at"] = _ceil_tick(now, interval)
            _event(session_id, asin, "retry_send_failed", "重试消息重发失败，下一轮再试", actor)
        _update_session(session_id, **fields)
    return result


# ────────────────────────── 自动跟进 ──────────────────────────

def _activity_dt(record: Dict[str, Any]) -> Optional[dt.datetime]:
    return _dt(record.get("finish_time")) or _dt(record.get("operate_time")) or _dt(
        record.get("last_seen_at")
    )


def auto_open_sessions(changed_asins: List[str], *, first_poll: bool = False) -> List[str]:
    """取数后自动为「处理中」的 ASIN 建立跟进会话；新出现的失败也可按配置跟进

    保护规则（避免骚扰/死循环）：
    1. 首次取数（系统刚上线）时把已有的失败当作历史数据，不自动跟进；之后新出现的失败才跟进；
    2. 失败记录还要满足「失败自动跟进起始时间 follow_start_at」：早于该时间的失败不跟进
       （留空=不限制）；
    3. 只有「最近 auto_follow_recent_hours 小时内有过动作」的记录才会发飞书消息，历史记录只静默跟踪；
    4. 已经用满重试次数转入人工检查的 ASIN，若之后没有新的上传动作（指纹未变化），不再自动重建会话；
       其余已结束的会话在 auto_follow_cooldown_hours 冷却期内也不重建。
    """
    cfg = store.snapshot()
    opened: List[str] = []
    recent_hours = int(cfg.get("auto_follow_recent_hours", 72))
    notify_cutoff = now_dt() - dt.timedelta(hours=recent_hours)
    # 首次取数的记录不算「新变化」，避免上线瞬间把历史失败全部跟进一遍
    changed = set() if first_poll else set(changed_asins or [])
    fail_cutoff = _dt(cfg.get("follow_start_at"))
    cooldown_hours = int(cfg.get("auto_follow_cooldown_hours", 6) or 0)
    last_sessions = {
        row["parent_asin"]: row
        for row in db.fetchall(
            """
            SELECT s.* FROM follow_sessions s
              JOIN (SELECT parent_asin, MAX(id) AS mid FROM follow_sessions GROUP BY parent_asin) t
                ON t.mid = s.id
            """
        )
    }

    if not cfg.get("auto_follow_processing") and not cfg.get("auto_follow_failed"):
        return opened

    latest = latest_records()
    for asin, record in latest.items():
        status_text = record.get("status_text")
        activity = _activity_dt(record)
        if status_text == STATUS_FAILED:
            if not (cfg.get("auto_follow_failed") or asin in changed):
                continue
            if fail_cutoff and (activity is None or activity < fail_cutoff):
                logger.info("跳过历史失败 %s（finish=%s 早于起始时间）", asin, record.get("finish_time"))
                continue
        elif status_text == STATUS_PROCESSING:
            if not cfg.get("auto_follow_processing"):
                continue
        else:
            continue

        previous = last_sessions.get(asin)
        if previous and previous.get("state") == "active":
            continue  # 已有进行中的会话
        if previous:
            revision_changed = (previous.get("last_revision") or "") != lingxing.revision_of(record)
            exhausted = (
                int(previous.get("attempts") or 0) > 0
                and int(previous.get("attempts") or 0) >= int(previous.get("max_attempts") or 0)
            )
            if exhausted:
                # 重试次数用尽 → 按约定转人工检查：一律不再自动重建会话，
                # 但把「人工检查期间又出现了新结果」记进事件，方便在页面上看到
                if revision_changed:
                    _event(
                        None,
                        asin,
                        "exhausted_new_result",
                        "重试次数已用尽（人工检查阶段），出现新的上传结果但不再自动跟进",
                        "auto",
                    )
                continue
            closed_at = _dt(previous.get("closed_at"))
            # 冷却期只用于「没有新动作」的情况；一旦出现新的上传结果（指纹变化，
            # 例如重试后又失败/成功），必须重新跟进，否则最新结果没人同步、没人通知。
            if (
                cooldown_hours
                and closed_at
                and not revision_changed
                and now_dt() - closed_at < dt.timedelta(hours=cooldown_hours)
            ):
                continue

        notify = bool(cfg.get("auto_follow_notify", True)) and (
            activity is not None and activity >= notify_cutoff
        )
        try:
            new_session, created = open_session(
                asin,
                source="auto_processing" if status_text == STATUS_PROCESSING else "auto_failed",
                owner=record.get("user_name", ""),
                payload={"reason": "auto", "status_text": status_text},
                notify=notify,
                record=record,
                actor="auto",
            )
        except (ValueError, RuntimeError) as exc:
            logger.info("自动跟进跳过 %s：%s", asin, exc)
            continue
        if created:
            opened.append(asin)
            if not notify:
                _event(
                    (new_session or {}).get("id"),
                    asin,
                    "notify_off",
                    "非近期记录，本次跟进不发送飞书消息（仅记录状态）",
                    "auto",
                )
    return opened


# ────────────────────────── 取数主流程 ──────────────────────────

def _run_insert(trigger: str, window_start: str, window_end: str) -> int:
    return db.insert(
        """
        INSERT INTO poll_runs (trigger_name, started_at, status, window_start, window_end)
        VALUES (?,?,?,?,?)
        """,
        trigger,
        now_iso(),
        "running",
        window_start,
        window_end,
    )


# ────────────────────── 取数节奏（正常 / 处理中 / 失败退避）──────────────────────

def poll_interval_seconds(cfg: Optional[Dict[str, Any]] = None) -> int:
    """下一次取数的间隔（秒）：
    - 有跟进任务或处理中记录 → 用 processing_poll_interval_minutes；
    - 否则用 poll_interval_minutes。

    取数失败（例如 auth-token 过期）不单独重试：失败也按同样节奏等下一次，
    避免不停打领星接口；更新 token 后系统会立刻重新取一次数。
    """
    cfg = cfg or store.snapshot()
    busy = active_session_count() > 0
    if not busy:
        row = db.fetchone(
            "SELECT COUNT(*) AS n FROM publish_records "
            "WHERE status_text NOT IN ('success','failed')"
        ) or {}
        busy = int(row.get("n") or 0) > 0
    minutes = int(
        cfg.get("processing_poll_interval_minutes" if busy else "poll_interval_minutes", 60)
    )
    return max(60, minutes * 60)


def poll_due(now: Optional[dt.datetime] = None) -> bool:
    """是否到了该取数的时间（按上一次「尝试」时间算，失败也会计入，避免空转）"""
    now = now or now_dt()
    if db.get_meta("poll_force_next") == "1":
        return True  # 例如刚更新了 auth-token：立刻重新取一次
    last = _dt(db.get_meta("last_poll_attempt_at")) or _dt(db.get_meta("last_poll_ok_at"))
    if not last:
        return True
    return (now - last).total_seconds() >= poll_interval_seconds()


def _run_finish(run_id: int, **fields) -> None:
    if not run_id:
        return
    allowed = {
        "status",
        "http_requests",
        "api_total",
        "fetched",
        "inserted",
        "updated",
        "changed",
        "processing",
        "success",
        "failed",
        "error",
        "detail",
    }
    payload = {k: v for k, v in fields.items() if k in allowed}
    payload["finished_at"] = now_iso()
    columns = ", ".join(f"{key}=?" for key in payload)
    db.execute(f"UPDATE poll_runs SET {columns} WHERE id=?", [*payload.values(), run_id])


def _notify_poll_error(error: str) -> None:
    cfg = store.snapshot()
    if not cfg.get("notify_poll_error", True):
        return
    # 每日上限：按自然日计数，避免故障期间反复刷屏（默认一天最多 3 次）
    max_per_day = int(cfg.get("poll_error_notify_max_per_day", 3) or 0)
    today = now_dt().strftime("%Y-%m-%d")
    if max_per_day > 0:
        if db.get_meta("poll_error_notify_day") != today:
            db.set_meta("poll_error_notify_day", today)
            db.set_meta("poll_error_notify_count", "0")
        if int(db.get_meta("poll_error_notify_count") or 0) >= max_per_day:
            return
    gap = int(cfg.get("poll_error_notify_minutes", 60)) * 60
    last = _dt(db.get_meta("last_poll_error_notify_at"))
    if last and (now_dt() - last).total_seconds() < gap:
        return
    notifier = FeishuNotifier(cfg)
    if notifier.enabled:
        notifier.notify_poll_error(error=error)
        db.set_meta("last_poll_error_notify_at", now_iso())
        if max_per_day > 0:
            count = int(db.get_meta("poll_error_notify_count") or 0) + 1
            db.set_meta("poll_error_notify_count", str(count))


def _notify_fallback(error: str) -> None:
    """主数据源失败但备用源成功：默认**不发**提示（只在页面/日志里体现数据来源）。

    如需提示，把「消息内容 → 走备用数据源时发提示」打开即可（同样受告警间隔限制）。
    """
    cfg = store.snapshot()
    if not cfg.get("notify_fallback"):
        return
    if not cfg.get("notify_poll_error", True):
        return
    gap = int(cfg.get("poll_error_notify_minutes", 60)) * 60
    last = _dt(db.get_meta("last_fallback_notify_at"))
    if last and (now_dt() - last).total_seconds() < gap:
        return
    notifier = FeishuNotifier(cfg)
    if notifier.enabled:
        from app.feishu import build_context

        notifier.send_kind(
            kind="fallback",
            template_key="fallback_message_template",
            asin="",
            context=build_context(error=error[:200]),
            force=True,
            targets=notifier.poll_error_targets(),
        )
        db.set_meta("last_fallback_notify_at", now_iso())


def poll_once(
    trigger: str = "schedule",
    *,
    force: bool = False,
    only_session_ids: Optional[List[int]] = None,
    force_all_sessions: bool = False,
) -> Dict[str, Any]:
    """取一次数：拉取 → 落库 → 自动跟进 → 判定跟进会话"""
    if not _poll_lock.acquire(blocking=False):
        return {"ok": False, "skipped": "已有一个取数任务在执行中"}
    try:
        cfg = store.snapshot()
        if not cfg.get("lingxing_auth_token"):
            error = "未配置 auth-token，请在「系统设置 → 领星接口」中填写"
            db.set_meta("last_poll_error", error)
            return {"ok": False, "error": error}

        window_start, window_end = lingxing.build_window(cfg)
        run_id = _run_insert(trigger, window_start, window_end)
        first_poll = not db.get_meta("bootstrap_at")
        db.set_meta("last_poll_attempt_at", now_iso())
        db.set_meta("poll_force_next", "")
        client = lingxing.LingxingClient(cfg)
        proxy = lingxing.LingxingProxyClient(cfg)
        # 主/备数据源：默认自建 API 站为主、领星直连为备（可在「系统设置 → 兜底接口」切换）
        if cfg.get("fallback_as_primary"):
            primary_client, primary_source = proxy, "fallback"
            backup_client, backup_source = client, "lingxing"
            primary_label, backup_label = "自建 API 站", "领星直连"
        else:
            primary_client, primary_source = client, "lingxing"
            backup_client, backup_source = proxy, "fallback"
            primary_label, backup_label = "领星直连", "自建 API 站"
        if isinstance(backup_client, lingxing.LingxingProxyClient):
            backup_ready = backup_client.ready
            proxy_is_backup = True
        else:
            backup_ready = bool(cfg.get("lingxing_auth_token"))
            proxy_is_backup = False

        data_source = primary_source
        primary_error = ""
        try:
            result = primary_client.fetch_all(window_start=window_start, window_end=window_end)
        except Exception as exc:  # 网络 / 业务错误 → 尝试备用数据源
            primary_error = str(exc)
            logger.warning("%s取数失败：%s", primary_label, primary_error)
            auth_error = any(
                word in primary_error
                for word in ("auth-token", "鉴权", "未授权", "-999", "token", "过期")
            )
            use_backup = backup_ready and (
                not proxy_is_backup
                or not cfg.get("fallback_only_on_auth")
                or auth_error
            )
            if use_backup:
                try:
                    logger.info("改用备用数据源（%s）取数", backup_label)
                    result = backup_client.fetch_all(
                        window_start=window_start, window_end=window_end
                    )
                    data_source = backup_source
                    _notify_fallback(primary_error)
                except Exception as exc2:
                    message = f"{primary_error}｜{backup_label}也失败：{exc2}"
                    logger.warning("取数失败（主备都失败）：%s", message)
                    _run_finish(run_id, status="error", error=message[:2000])
                    db.set_meta("last_poll_error", message[:2000])
                    db.set_meta("last_poll_error_at", now_iso())
                    db.set_meta("last_data_source", "error")
                    _notify_poll_error(message)
                    return {"ok": False, "error": message, "run_id": run_id}
            else:
                message = primary_error
                if proxy_is_backup and cfg.get("fallback_enabled") and not proxy.ready:
                    message += "（自建 API 站未配置完整：需要地址 + X-User-Id + X-User-Token）"
                _run_finish(run_id, status="error", error=message[:2000])
                db.set_meta("last_poll_error", message[:2000])
                db.set_meta("last_poll_error_at", now_iso())
                db.set_meta("last_data_source", "error")
                _notify_poll_error(message)
                return {"ok": False, "error": message, "run_id": run_id}

        counts = save_records(result.records)
        stats = db.fetchall(
            "SELECT status_text, COUNT(*) AS n FROM publish_records GROUP BY status_text"
        )
        stat_map = {row["status_text"]: int(row["n"]) for row in stats}
        _run_finish(
            run_id,
            status="ok",
            http_requests=result.http_requests,
            api_total=result.api_total,
            fetched=len(result.records),
            inserted=counts["inserted"],
            updated=counts["updated"],
            changed=counts["changed"],
            processing=stat_map.get(STATUS_PROCESSING, 0),
            success=stat_map.get(STATUS_SUCCESS, 0),
            failed=stat_map.get(STATUS_FAILED, 0),
            detail=(
                f"数据来源：{primary_label}"
                if data_source == primary_source
                else f"数据来源：{backup_label}（{primary_label}失败：{primary_error[:200]}）"
            ),
        )
        db.set_meta("last_poll_at", now_iso())
        db.set_meta("last_poll_ok_at", now_iso())
        db.set_meta("last_poll_error", "")
        db.set_meta("last_data_source", data_source)
        # 「最近一次走备用源」：仅在真正使用备用数据源时记录（主备关系可切换）
        db.set_meta(
            "last_fallback_at",
            now_iso() if data_source == backup_source else db.get_meta("last_fallback_at"),
        )
        db.set_meta("last_primary_error", primary_error[:2000])
        db.set_meta("last_window_start", window_start)
        db.set_meta("last_window_end", window_end)
        if not db.get_meta("bootstrap_at"):
            db.set_meta("bootstrap_at", now_iso())

        opened = auto_open_sessions(counts["changed_asins"], first_poll=first_poll)
        evaluation = evaluate_sessions(
            actor=f"poll:{trigger}",
            only_session_ids=only_session_ids,
            force_all=force_all_sessions,
        )
        return {
            "ok": True,
            "run_id": run_id,
            "api_total": result.api_total,
            "fetched": len(result.records),
            "inserted": counts["inserted"],
            "updated": counts["updated"],
            "changed": counts["changed"],
            "auto_followed": opened,
            "evaluation": evaluation,
            "counts": stat_map,
        }
    finally:
        _poll_lock.release()


def refresh_and_evaluate(trigger: str = "follow", *, max_age_seconds: int = 60) -> Dict[str, Any]:
    """需要最新状态时：若距上次取数不足 max_age_seconds 则直接判定，否则先取数"""
    cfg = store.snapshot()
    last = _dt(db.get_meta("last_poll_ok_at"))
    fresh = bool(last and (now_dt() - last).total_seconds() <= max(0, max_age_seconds))
    if fresh:
        return {
            "ok": True,
            "reused": True,
            "evaluation": evaluate_sessions(actor=f"tick:{trigger}"),
        }
    return poll_once(trigger, force=True)


# ────────────────────────── 概览统计 ──────────────────────────

def summary() -> Dict[str, Any]:
    cfg = store.snapshot()
    totals = db.fetchone("SELECT COUNT(*) AS n FROM publish_records") or {}
    asins = db.fetchone("SELECT COUNT(DISTINCT parent_asin) AS n FROM publish_records") or {}
    by_status = {
        row["status_text"]: int(row["n"])
        for row in db.fetchall(
            "SELECT status_text, COUNT(*) AS n FROM publish_records GROUP BY status_text"
        )
    }
    today = now_dt().strftime("%Y-%m-%d")
    today_changed = db.fetchone(
        "SELECT COUNT(*) AS n FROM record_status_history WHERE change_kind='status_change' "
        "AND substr(observed_at,1,10)=?",
        today,
    ) or {}
    active = db.fetchone(
        "SELECT COUNT(*) AS n FROM follow_sessions WHERE state='active'"
    ) or {}
    sessions_today = db.fetchone(
        "SELECT COUNT(*) AS n FROM follow_sessions WHERE substr(opened_at,1,10)=?", today
    ) or {}
    messages_today = db.fetchone(
        "SELECT COUNT(*) AS n FROM feishu_messages WHERE substr(created_at,1,10)=?", today
    ) or {}
    messages_failed = db.fetchone(
        "SELECT COUNT(*) AS n FROM feishu_messages WHERE ok=0"
    ) or {}
    last_run = db.fetchone("SELECT * FROM poll_runs ORDER BY id DESC LIMIT 1")
    error_runs = db.fetchone(
        "SELECT COUNT(*) AS n FROM poll_runs WHERE status='error' AND substr(started_at,1,10)=?",
        today,
    ) or {}
    return {
        "records_total": int(totals.get("n") or 0),
        "asins_total": int(asins.get("n") or 0),
        "status_counts": {
            "success": by_status.get(STATUS_SUCCESS, 0),
            "failed": by_status.get(STATUS_FAILED, 0),
            "processing": by_status.get(STATUS_PROCESSING, 0),
            "unknown": by_status.get(STATUS_UNKNOWN, 0),
        },
        "status_changes_today": int(today_changed.get("n") or 0),
        "follow_active": int(active.get("n") or 0),
        "follow_opened_today": int(sessions_today.get("n") or 0),
        "messages_today": int(messages_today.get("n") or 0),
        "messages_failed": int(messages_failed.get("n") or 0),
        "error_runs_today": int(error_runs.get("n") or 0),
        "last_poll_at": db.get_meta("last_poll_at"),
        "last_poll_ok_at": db.get_meta("last_poll_ok_at"),
        "last_poll_error": db.get_meta("last_poll_error"),
        "last_data_source": db.get_meta("last_data_source"),
        "last_data_source_cn": {"lingxing": "领星直连", "fallback": "自建 API 站"}.get(
            db.get_meta("last_data_source"), "-"
        ),
        "last_primary_error": db.get_meta("last_primary_error"),
        "last_fallback_at": db.get_meta("last_fallback_at"),
        "bootstrap_at": db.get_meta("bootstrap_at"),
        "window": {
            "start": db.get_meta("last_window_start"),
            "end": db.get_meta("last_window_end"),
        },
        "last_run": last_run,
        "next_poll_at": db.get_meta("next_poll_at"),
        "next_follow_tick": next_session_tick(),
        "config": {
            "lingxing_ready": store.lingxing_ready(cfg),
            "feishu_ready": store.feishu_ready(cfg),
            "feishu_chat_id": cfg.get("feishu_chat_id", ""),
            "feishu_bot_open_id": bool(cfg.get("feishu_bot_open_id")),
            "lingxing_proxy": cfg.get("lingxing_proxy", ""),
            "fallback_enabled": bool(cfg.get("fallback_enabled")),
            "fallback_ready": lingxing.LingxingProxyClient(cfg).ready,
            "fallback_base_url": cfg.get("fallback_base_url", ""),
            "fallback_account": cfg.get("fallback_account", ""),
            "poll_interval_minutes": cfg.get("poll_interval_minutes"),
            "processing_poll_interval_minutes": cfg.get("processing_poll_interval_minutes"),
            "follow_interval_minutes": cfg.get("follow_interval_minutes"),
            "follow_round_minutes": cfg.get("follow_round_minutes"),
            "follow_max_attempts": cfg.get("follow_max_attempts"),
            "follow_start_at": cfg.get("follow_start_at") or "",
        },
    }


def list_poll_runs(page: int = 1, page_size: int = 50) -> Dict[str, Any]:
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 200)
    total = db.fetchone("SELECT COUNT(*) AS n FROM poll_runs") or {}
    rows = db.fetchall(
        "SELECT * FROM poll_runs ORDER BY id DESC LIMIT ? OFFSET ?",
        page_size,
        (page - 1) * page_size,
    )
    return {
        "items": rows,
        "total": int(total.get("n") or 0),
        "page": page,
        "page_size": page_size,
    }


def list_messages(page: int = 1, page_size: int = 50, kind: str = "", asin: str = "", only_failed: bool = False) -> Dict[str, Any]:
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 200)
    conds: List[str] = []
    params: List[Any] = []
    if kind:
        conds.append("kind = ?")
        params.append(kind)
    if asin:
        conds.append("parent_asin LIKE ?")
        params.append(f"%{normalize_asin(asin)}%")
    if only_failed:
        conds.append("ok = 0")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    total = db.fetchone(f"SELECT COUNT(*) AS n FROM feishu_messages {where}", params) or {}
    rows = db.fetchall(
        f"SELECT * FROM feishu_messages {where} ORDER BY id DESC LIMIT ? OFFSET ?",
        [*params, page_size, (page - 1) * page_size],
    )
    return {
        "items": rows,
        "total": int(total.get("n") or 0),
        "page": page,
        "page_size": page_size,
    }


def resend_message(message_id: int) -> Dict[str, Any]:
    row = db.fetchone("SELECT * FROM feishu_messages WHERE id=?", int(message_id))
    if not row:
        return {"ok": False, "error": "消息记录不存在"}
    notifier = FeishuNotifier()
    # 重发时沿用原来的目标群（例如取数失败告警只发某个群）
    targets = [
        item.strip()
        for item in str(row.get("targets") or "").replace("，", ",").split(",")
        if item.strip().startswith("oc_")
    ]
    ok, details = notifier.notify_text(
        row.get("content", ""),
        kind="resend",
        asin=row.get("parent_asin", ""),
        targets=targets or None,
    )
    return {"ok": ok, "id": message_id, "details": details}


# 一次性通知（成功 / 人工检查）：发送失败会自动补发；重试消息由跟进状态机自己重试，不在这里补
_AUTO_RETRY_KINDS = ("success", "final_failure")


def retry_failed_notifications(
    *, max_age_hours: int = 6, limit: int = 5, min_gap_minutes: int = 5
) -> Dict[str, Any]:
    """补发发送失败的一次性飞书通知（需在「系统设置 → 消息内容」开启，默认关闭）"""
    cfg = store.snapshot()
    if not cfg.get("auto_resend_failed_notify"):
        return {"checked": 0, "sent": 0, "skipped": "未开启自动补发"}
    last = _dt(db.get_meta("last_notify_retry_at"))
    if last and (now_dt() - last).total_seconds() < max(1, min_gap_minutes) * 60:
        return {"checked": 0, "sent": 0, "skipped": "间隔未到"}
    cutoff = (now_dt() - dt.timedelta(hours=max(1, max_age_hours))).replace(
        microsecond=0
    ).isoformat()
    placeholders = ",".join("?" for _ in _AUTO_RETRY_KINDS)
    rows = db.fetchall(
        f"""
        SELECT * FROM feishu_messages
         WHERE ok = 0 AND COALESCE(retried_at,'') = '' AND content <> ''
           AND error NOT LIKE '演练模式%'
           AND kind IN ({placeholders}) AND created_at >= ?
         ORDER BY id ASC LIMIT ?
        """,
        [*_AUTO_RETRY_KINDS, cutoff, int(limit)],
    )
    if not rows:
        db.set_meta("last_notify_retry_at", now_iso())
        return {"checked": 0, "sent": 0}
    notifier = FeishuNotifier()
    sent = 0
    for row in rows:
        ok, _details = notifier.notify_text(
            row.get("content", ""), kind="resend", asin=row.get("parent_asin", "")
        )
        db.execute(
            "UPDATE feishu_messages SET retried_at = ? WHERE id = ?", now_iso(), row["id"]
        )
        if ok:
            sent += 1
            _event(
                row.get("session_id"),
                row.get("parent_asin", ""),
                "notify_resent",
                f"补发 {row.get('kind')} 消息（原发送失败：{row.get('error', '')[:60]}）",
                "system",
            )
    db.set_meta("last_notify_retry_at", now_iso())
    return {"checked": len(rows), "sent": sent}
