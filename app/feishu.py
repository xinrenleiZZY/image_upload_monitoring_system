"""飞书通知模块

发送方式（二选一，优先 Webhook）：
1) 群自定义机器人 Webhook：feishu_webhook_url（可选签名密钥）
2) 自建应用：feishu_app_id + feishu_app_secret，发往 feishu_chat_id 或 feishu_receiver_open_id

消息模板与目标群都在「系统设置」里维护；每条发送都会写入 feishu_messages 表便于排查。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, List, Optional

from app import db
from app import settings_store as store
from app.settings_store import DEFAULT_WIKI_URL
from app.config import now_dt, now_iso
from app.http_client import HttpError, request_json

logger = logging.getLogger("monitor.feishu")

FEISHU_API = "https://open.feishu.cn/open-apis"

# tenant_access_token 进程级缓存：{app_id: (token, 过期时间戳)}
# 每次发消息都新建 FeishuNotifier，必须用模块级缓存，否则每条消息都要重新换 token，
# 遇到代理抖动（SSL EOF）就会丢消息。
_TOKEN_CACHE: Dict[str, tuple] = {}


class _SafeDict(dict):
    """模板占位符缺失时返回空串，避免模板写错导致发送失败"""

    def __missing__(self, key):  # pragma: no cover - 触发即说明模板有误
        return ""


def render(template: str, context: Dict[str, Any]) -> str:
    try:
        return str(template).format_map(_SafeDict(context)).strip()
    except (KeyError, IndexError, ValueError):
        return str(template)


def build_context(
    *,
    asin: str = "",
    owner: str = "",
    status: Any = None,
    status_name: str = "",
    failure_reason: str = "",
    store_name: str = "",
    attempt: int = 0,
    max_attempts: int = 0,
    error: str = "",
    mention: str = "",
) -> Dict[str, Any]:
    return {
        "asin": asin,
        "owner": owner,
        "owner_part": f"，负责人{owner}" if owner else "",
        "status": "" if status is None else status,
        "status_name": status_name,
        "failure_reason": failure_reason,
        "store": store_name,
        "attempt": attempt,
        "max_attempts": max_attempts,
        "error": error,
        "mention": mention,
        "time": now_dt().strftime("%Y-%m-%d %H:%M:%S"),
        "url": DEFAULT_WIKI_URL,
    }


class FeishuNotifier:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or store.snapshot()

    # ── 配置 ──
    @property
    def chat_ids(self) -> List[str]:
        raw = str(self.cfg.get("feishu_chat_id") or "")
        ids = [x.strip() for x in raw.replace("，", ",").split(",") if x.strip()]
        return list(dict.fromkeys(ids))

    @property
    def receiver_open_id(self) -> str:
        return str(self.cfg.get("feishu_receiver_open_id") or "").strip()

    @property
    def webhook(self) -> str:
        return str(self.cfg.get("feishu_webhook_url") or "").strip()

    @property
    def enabled(self) -> bool:
        return store.feishu_ready(self.cfg)

    def poll_error_targets(self) -> List[str]:
        """取数失败告警的目标群（可在「系统设置 → 消息内容」里单独指定；留空=所有群）"""
        raw = str(self.cfg.get("poll_error_chat_id") or "")
        ids = [x.strip() for x in raw.replace("，", ",").split(",") if x.strip()]
        return ids or self.chat_ids

    def mention_text(self) -> str:
        """@ 二号虾掌柜：配置了 open_id 就是真正的 @，否则退化为纯文本"""
        open_id = str(self.cfg.get("feishu_bot_open_id") or "").strip()
        name = str(self.cfg.get("feishu_bot_name") or "二号虾掌柜").strip()
        if open_id:
            return f'<at user_id="{open_id}"></at> '
        return f"@{name} " if name else ""

    # ── 鉴权 ──
    def _app_token(self) -> Optional[str]:
        app_id = str(self.cfg.get("feishu_app_id") or "").strip()
        app_secret = str(self.cfg.get("feishu_app_secret") or "").strip()
        if not app_id or not app_secret:
            return None
        cache_key = f"{app_id}:{app_secret[:8]}"
        cached = _TOKEN_CACHE.get(cache_key)
        if cached and time.time() < cached[1]:
            return cached[0]
        try:
            _, data, text = request_json(
                f"{FEISHU_API}/auth/v3/tenant_access_token/internal",
                method="POST",
                payload={"app_id": app_id, "app_secret": app_secret},
                timeout=15,
                retries=2,
            )
        except HttpError as exc:
            logger.warning("飞书获取 token 失败：%s", exc)
            return None
        if not isinstance(data, dict) or data.get("code") != 0:
            logger.warning("飞书获取 token 失败：%s", str(data)[:200] if data else text[:200])
            return None
        token = data.get("tenant_access_token", "")
        expire = int(data.get("expire") or 7200)
        _TOKEN_CACHE[cache_key] = (token, time.time() + max(60, expire - 300))
        return token or None

    # ── 发送 ──
    def _post_message(self, token: str, receive_id: str, receive_id_type: str, text: str) -> tuple[bool, str]:
        try:
            _, data, raw = request_json(
                f"{FEISHU_API}/im/v1/messages?receive_id_type={receive_id_type}",
                method="POST",
                payload={
                    "receive_id": receive_id,
                    "msg_type": "text",
                    "content": json.dumps({"text": text}, ensure_ascii=False),
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=20,
                retries=1,
            )
        except HttpError as exc:
            logger.warning("飞书发送失败（%s）：%s", receive_id, exc)
            return False, str(exc)
        if not isinstance(data, dict) or data.get("code") != 0:
            message = str((data or {}).get("msg") or data)[:200]
            logger.warning("飞书发送失败（%s）：%s", receive_id, message)
            return False, message
        return True, ""

    def _post_webhook(self, text: str) -> tuple[bool, str]:
        payload: Dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
        secret = str(self.cfg.get("feishu_webhook_secret") or "").strip()
        if secret:
            timestamp = str(int(time.time()))
            string_to_sign = f"{timestamp}\n{secret}"
            digest = hmac.new(
                string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
            ).digest()
            payload["timestamp"] = timestamp
            payload["sign"] = base64.b64encode(digest).decode("utf-8")
        try:
            _, data, _ = request_json(
                self.webhook, method="POST", payload=payload, timeout=20, retries=1
            )
        except HttpError as exc:
            logger.warning("飞书 Webhook 发送失败：%s", exc)
            return False, str(exc)
        if isinstance(data, dict) and data.get("code") not in (0, None):
            message = str((data or {}).get("msg") or data)[:200]
            logger.warning("飞书 Webhook 发送失败：%s", message)
            return False, message
        return True, ""

    def send_text(
        self, text: str, *, targets: Optional[List[str]] = None
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """发送文本，返回 (是否有目标成功, 每个目标的回执明细)

        配置了群自定义机器人 Webhook 时优先走 Webhook（与自建应用二选一，避免重复推送）。
        开启「演练模式」时只返回明细、不真正发送（调用方会记为未发送）。
        targets 指定时只发这些群（用于「取数失败只发某个群」）。
        """
        if not self.enabled:
            logger.warning("飞书未配置完整，消息未发送：%s", text[:80])
            return False, []
        if self.cfg.get("notify_dry_run"):
            logger.info("演练模式：消息未发送（只记录）：%s", text[:80])
            return False, [{"target": "dry_run", "ok": False, "error": "演练模式：未实际发送"}]
        details: List[Dict[str, Any]] = []
        chat_targets = targets if targets else self.chat_ids
        if self.webhook:
            ok, error = self._post_webhook(text)
            details.append({"target": "webhook", "ok": ok, "error": error})
        elif str(self.cfg.get("feishu_app_id") or "").strip():
            token = self._app_token()
            if token:
                for chat_id in chat_targets:
                    ok, error = self._post_message(token, chat_id, "chat_id", text)
                    details.append({"target": chat_id, "ok": ok, "error": error})
                if self.receiver_open_id:
                    ok, error = self._post_message(
                        token, self.receiver_open_id, "open_id", text
                    )
                    details.append({"target": self.receiver_open_id, "ok": ok, "error": error})
            else:
                details.append(
                    {"target": "app", "ok": False, "error": "获取 tenant_access_token 失败"}
                )
        ok_any = any(item["ok"] for item in details)
        return ok_any, details

    # ── 业务消息 ──
    def _log(
        self,
        *,
        kind: str,
        content: str,
        ok: bool,
        error: str = "",
        asin: str = "",
        session_id: Optional[int] = None,
        dedupe_key: str = "",
        targets: str = "",
    ) -> None:
        target_text = targets or (
            "webhook" if self.webhook else (",".join(self.chat_ids) or self.receiver_open_id or "")
        )
        try:
            db.execute(
                """
                INSERT INTO feishu_messages
                    (kind, parent_asin, session_id, targets, content, ok, error, dedupe_key, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                kind,
                asin,
                session_id,
                target_text,
                content,
                int(ok),
                error,
                dedupe_key,
                now_iso(),
            )
        except Exception as exc:  # pragma: no cover - 记录失败不影响主流程
            logger.warning("飞书写入日志失败：%s", exc)

    def _recent_duplicate(self, kind: str, asin: str, content: str) -> bool:
        window = int(self.cfg.get("message_dedupe_seconds", 30) or 0)
        if window <= 0:
            return False
        from datetime import timedelta

        threshold = (now_dt() - timedelta(seconds=window)).replace(microsecond=0).isoformat()
        row = db.fetchone(
            """
            SELECT id FROM feishu_messages
             WHERE kind = ? AND parent_asin = ? AND content = ? AND ok = 1 AND created_at >= ?
             ORDER BY id DESC LIMIT 1
            """,
            kind,
            asin,
            content,
            threshold,
        )
        return bool(row)

    def _recent_failed(self, kind: str, asin: str, *, seconds: int = 3600) -> bool:
        """最近是否已经记过同类型失败（避免飞书未配置时刷屏写日志）"""
        from datetime import timedelta

        threshold = (now_dt() - timedelta(seconds=seconds)).replace(microsecond=0).isoformat()
        row = db.fetchone(
            """
            SELECT id FROM feishu_messages
             WHERE kind = ? AND parent_asin = ? AND ok = 0 AND created_at >= ?
             ORDER BY id DESC LIMIT 1
            """,
            kind,
            asin,
            threshold,
        )
        return bool(row)

    def _dry_run(self) -> bool:
        return bool(self.cfg.get("notify_dry_run"))

    def send_kind(
        self,
        *,
        kind: str,
        template_key: str,
        asin: str,
        context: Dict[str, Any],
        session_id: Optional[int] = None,
        force: bool = False,
        targets: Optional[List[str]] = None,
    ) -> bool:
        """按模板渲染并发送；返回是否发送成功"""
        template = str(self.cfg.get(template_key) or "")
        content = render(template, context).strip()
        if not content:
            logger.warning("消息内容为空，已跳过：kind=%s", kind)
            return False
        if not self.enabled:
            # 飞书还没配好：只记账（每小时最多记一次），不算「已发送」
            if not self._recent_failed(kind, asin):
                self._log(
                    kind=kind,
                    content=content,
                    ok=False,
                    error="飞书未配置完整，消息未发送",
                    asin=asin,
                    session_id=session_id,
                )
            logger.warning("飞书未配置完整，消息未发送：%s", content[:80])
            return False
        if self._dry_run():
            self._log(
                kind=kind,
                content=content,
                ok=False,
                error="演练模式：仅记录未发送",
                asin=asin,
                session_id=session_id,
            )
            logger.info("演练模式：%s 消息未发送（%s）", kind, asin or "-")
            return False
        if not force and self._recent_duplicate(kind, asin, content):
            logger.info("命中消息去重（%s/%s），跳过重复发送", kind, asin)
            return True
        ok, details = self.send_text(content, targets=targets)
        failed = [item for item in details if not item["ok"]]
        error = "" if ok else "发送失败（配置缺失或接口报错）"
        if failed:
            error = "部分目标失败：" + "；".join(
                f"{item['target']} → {item['error'] or '失败'}" for item in failed
            )
        self._log(
            kind=kind,
            content=content,
            ok=ok,
            error=error[:500],
            asin=asin,
            session_id=session_id,
            dedupe_key=f"{kind}:{asin}:{now_dt().strftime('%Y%m%d%H%M')}",
            targets=",".join(targets) if targets else "",
        )
        if ok:
            logger.info(
                "飞书已发送（%s，%s 个目标，其中失败 %s 个）：%s",
                kind,
                len(details),
                len(failed),
                content[:120],
            )
        return ok

    # 具体业务方法 ──
    def notify_retry(self, *, asin: str, owner: str = "", record: Optional[Dict[str, Any]] = None,
                     attempt: int = 0, max_attempts: int = 0, session_id: Optional[int] = None,
                     force: bool = False) -> bool:
        record = record or {}
        context = build_context(
            asin=asin,
            owner=owner,
            status=record.get("status"),
            status_name=record.get("status_name", ""),
            failure_reason=record.get("failure_reason", ""),
            store_name=record.get("store_name", ""),
            attempt=attempt,
            max_attempts=max_attempts,
            mention=self.mention_text(),
        )
        return self.send_kind(
            kind="retry",
            template_key="retry_message_template",
            asin=asin,
            context=context,
            session_id=session_id,
            force=force,
        )

    def notify_success(self, *, asin: str, owner: str = "", record: Optional[Dict[str, Any]] = None,
                       session_id: Optional[int] = None, force: bool = False) -> bool:
        record = record or {}
        context = build_context(
            asin=asin,
            owner=owner,
            status=record.get("status"),
            status_name=record.get("status_name", ""),
            store_name=record.get("store_name", ""),
        )
        return self.send_kind(
            kind="success",
            template_key="success_message_template",
            asin=asin,
            context=context,
            session_id=session_id,
            force=force,
        )

    def notify_final_failure(self, *, asin: str, owner: str = "",
                             record: Optional[Dict[str, Any]] = None,
                             attempt: int = 0, max_attempts: int = 0,
                             session_id: Optional[int] = None, force: bool = False) -> bool:
        record = record or {}
        context = build_context(
            asin=asin,
            owner=owner,
            status=record.get("status"),
            status_name=record.get("status_name", ""),
            failure_reason=record.get("failure_reason", ""),
            store_name=record.get("store_name", ""),
            attempt=attempt,
            max_attempts=max_attempts,
        )
        return self.send_kind(
            kind="final_failure",
            template_key="final_failure_message_template",
            asin=asin,
            context=context,
            session_id=session_id,
            force=force,
        )

    def notify_poll_error(self, *, error: str, force: bool = False) -> bool:
        context = build_context(error=error)
        return self.send_kind(
            kind="poll_error",
            template_key="poll_error_message_template",
            asin="",
            context=context,
            force=force,
            targets=self.poll_error_targets(),
        )

    def notify_text(
        self,
        text: str,
        *,
        kind: str = "test",
        asin: str = "",
        targets: Optional[List[str]] = None,
    ) -> tuple[bool, List[Dict[str, Any]]]:
        ok, details = self.send_text(text, targets=targets)
        failed = [item for item in details if not item["ok"]]
        error = ""
        if not ok:
            error = "发送失败（配置缺失或接口报错）"
        if failed:
            error = "部分目标失败：" + "；".join(
                f"{item['target']} → {item['error'] or '失败'}" for item in failed
            )
        self._log(
            kind=kind,
            content=text,
            ok=ok,
            error=error[:500],
            asin=asin,
            targets=",".join(targets) if targets else "",
        )
        return ok, details
