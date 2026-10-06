"""网页数据接口（/api/ui）"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import FileResponse

from app import db
from app import services
from app import settings_store as store
from app.config import now_iso, settings
from app.feishu import FeishuNotifier
from app.scheduler import scheduler

logger = logging.getLogger("monitor.ui")

router = APIRouter(prefix="/api/ui", tags=["web-ui"])


def _ok(data: Any, message: str = "ok") -> dict:
    return {"code": 0, "message": message, "data": data}


@router.get("/summary")
def ui_summary():
    data = services.summary()
    data["scheduler"] = scheduler.status()
    data["server_time"] = now_iso()
    return _ok(data)


@router.get("/records")
def ui_records(
    asin: str = Query(default=""),
    status_text: str = Query(default="", description="success / failed / processing"),
    store_name: str = Query(default=""),
    country_name: str = Query(default=""),
    keyword: str = Query(default=""),
    changed_from: str = Query(default=""),
    changed_to: str = Query(default=""),
    order: str = Query(default="operate_desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
):
    data = services.query_records(
        asin=asin,
        status_text=status_text,
        store_name=store_name,
        country_name=country_name,
        keyword=keyword,
        changed_from=changed_from,
        changed_to=changed_to,
        order=order,
        page=page,
        page_size=page_size,
    )
    return _ok(data)


@router.get("/records/{asin}")
def ui_record_detail(asin: str):
    asin = services.normalize_asin(asin)
    records = services.records_for(asin, limit=100)
    history = services.history_for(asin, limit=200)
    session = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin
    )
    events = services.session_events(session["id"]) if session else []
    messages = db.fetchall(
        "SELECT * FROM feishu_messages WHERE parent_asin=? ORDER BY id DESC LIMIT 50", asin
    )
    return _ok(
        {
            "asin": asin,
            "records": records,
            "history": history,
            "session": session,
            "events": events,
            "messages": messages,
        }
    )


@router.get("/status")
def ui_status(asins: str = Query(..., description="逗号分隔")):
    items = [
        x.strip()
        for x in str(asins or "").replace("，", ",").replace(";", ",").split(",")
        if x.strip()
    ][:500]
    if not items:
        raise HTTPException(status_code=400, detail="请至少提供一个 ASIN")
    return _ok({"items": services.asin_status(*items), "total": len(items)})


@router.get("/sessions")
def ui_sessions(
    state: str = Query(default=""),
    source: str = Query(default=""),
    asin: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
):
    return _ok(
        services.list_sessions(
            state=state, source=source, asin=asin, page=page, page_size=page_size
        )
    )


@router.get("/sessions/{session_id}/events")
def ui_session_events(session_id: int):
    return _ok({"items": services.session_events(session_id)})


@router.get("/events")
def ui_events(limit: int = Query(default=100, ge=1, le=500)):
    return _ok({"items": services.recent_events(limit)})


@router.post("/sessions/request")
def ui_request_follow(payload: Dict[str, Any] = Body(...)):
    """界面手工建立跟进（等价于第三方推送跟进请求）"""
    asins: List[str] = []
    raw = payload.get("asins") or payload.get("asin") or payload.get("parent_asin") or ""
    if isinstance(raw, str):
        raw = [x for x in raw.replace("，", ",").replace(";", ",").split(",") if x.strip()]
    for item in raw or []:
        text = services.normalize_asin(item)
        if text and text not in asins:
            asins.append(text)
    if not asins:
        raise HTTPException(status_code=400, detail="请填写 ASIN")
    owner = str(payload.get("owner") or "").strip()
    results = []
    for asin in asins[:200]:
        try:
            session, created = services.open_session(
                asin,
                source="manual",
                owner=owner,
                request_ts=payload.get("timestamp") or now_iso(),
                payload={"source": "ui", "owner": owner},
                notify=True,
                actor="ui",
            )
            results.append(
                {"asin": asin, "ok": True, "created": created, "session_id": session.get("id")}
            )
        except (ValueError, RuntimeError) as exc:
            results.append({"asin": asin, "ok": False, "error": str(exc)})
    scheduler.wake()
    return _ok({"items": results}, "已建立跟进任务")


@router.post("/sessions/{session_id}/close")
def ui_close_session(session_id: int):
    session = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", session_id)
    if not session:
        raise HTTPException(status_code=404, detail="跟进任务不存在")
    services.close_session(session_id, state="cancelled", reason="人工结束跟进", actor="ui")
    return _ok({"id": session_id, "state": "cancelled"}, "已结束跟进")


@router.post("/sessions/{session_id}/tick")
def ui_tick_session(session_id: int):
    """立即对该 ASIN 刷新一次状态并判定（不看检查时间，属于人工强制检查）"""
    session = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", session_id)
    if not session:
        raise HTTPException(status_code=404, detail="跟进任务不存在")
    result = services.poll_once(
        "ui_tick", force=True, only_session_ids=[session_id], force_all_sessions=True
    )
    return _ok({"session_id": session_id, "poll": result}, "已立即检查")


@router.get("/poll-runs")
def ui_poll_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    return _ok(services.list_poll_runs(page=page, page_size=page_size))


@router.post("/poll/run")
def ui_poll_run():
    result = services.poll_once("ui_manual", force=True)
    if not result.get("ok"):
        return {"code": 1, "message": str(result.get("error") or result.get("skipped") or "取数失败"), "data": result}
    return _ok(result, "取数完成")


@router.get("/feishu/messages")
def ui_messages(
    kind: str = Query(default=""),
    asin: str = Query(default=""),
    only_failed: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    return _ok(
        services.list_messages(
            page=page, page_size=page_size, kind=kind, asin=asin, only_failed=only_failed
        )
    )


@router.post("/feishu/test")
def ui_feishu_test(payload: Optional[Dict[str, Any]] = Body(default=None)):
    text = str((payload or {}).get("text") or "").strip()
    if not text:
        text = (
            "【图片上传监控系统】飞书通知测试消息，"
            f"如果收到说明配置正常（{now_iso()}）"
        )
    notifier = FeishuNotifier()
    ok, details = notifier.notify_text(text, kind="test")
    if ok:
        failed = [item for item in details if not item["ok"]]
        message = "发送成功" + (f"（{len(failed)} 个目标失败）" if failed else "")
    else:
        message = "发送失败（请检查飞书配置：app_id/app_secret/chat_id，以及机器人是否在群里）"
    return {
        "code": 0 if ok else 1,
        "message": message,
        "data": {"ok": ok, "details": details},
    }


@router.post("/feishu/resend/{message_id}")
def ui_feishu_resend(message_id: int):
    result = services.resend_message(message_id)
    return {"code": 0 if result.get("ok") else 1, "message": "已重发" if result.get("ok") else "重发失败", "data": result}


@router.get("/settings")
def ui_settings():
    return _ok(store.describe())


@router.post("/settings")
def ui_settings_save(payload: Dict[str, Any] = Body(...)):
    values = payload.get("values") if isinstance(payload.get("values"), dict) else payload
    result = store.update(values)
    # 改了领星相关配置（尤其是 auth-token）→ 清掉失败态，下一次调度立刻重新取数，
    # 不用干等到下一个整点
    lingxing_changed = any(str(key).startswith("lingxing_") for key in (values or {}))
    if lingxing_changed:
        db.set_meta("last_poll_error", "")
        db.set_meta("poll_force_next", "1")
    scheduler.wake()
    message = f"已保存 {len(result['saved'])} 项设置"
    if lingxing_changed:
        message += "（领星相关配置已更新，正在立即重新取数）"
    if result["rejected"]:
        message += f"，{len(result['rejected'])} 项被拒绝：" + "；".join(
            f"{key} {reason}" for key, reason in result["rejected"].items()
        )
    return {
        "code": 0 if not result["rejected"] else 1,
        "message": message,
        "data": result,
    }


@router.post("/settings/reset")
def ui_settings_reset(payload: Dict[str, Any] = Body(...)):
    keys = payload.get("keys") or []
    if not isinstance(keys, list):
        raise HTTPException(status_code=400, detail="keys 需要是数组")
    count = store.reset([str(k) for k in keys])
    return _ok({"reset": count}, "已恢复环境默认值")


@router.get("/config")
def ui_config():
    cfg = store.snapshot()
    return _ok(
        {
            "retry_template": cfg.get("retry_message_template"),
            "success_template": cfg.get("success_message_template"),
            "final_template": cfg.get("final_failure_message_template"),
            "mention_preview": FeishuNotifier(cfg).mention_text(),
            "feishu_ready": store.feishu_ready(cfg),
            "lingxing_ready": store.lingxing_ready(cfg),
            "server_time": now_iso(),
        }
    )


@router.post("/fallback/test")
def ui_fallback_test():
    """测试兜底数据源（自建领星 API 站）是否可用"""
    from app.lingxing import LingxingError, LingxingProxyClient

    client = LingxingProxyClient()
    if not client.ready:
        return {
            "code": 1,
            "message": "兜底接口未配置完整（需要地址 + X-User-Id + X-User-Token）",
            "data": {"ready": False},
        }
    try:
        result = client.probe()
    except LingxingError as exc:
        return {"code": 1, "message": f"兜底接口测试失败：{exc}", "data": {"ready": True}}
    except Exception as exc:  # pragma: no cover
        return {"code": 1, "message": f"兜底接口测试异常：{exc}", "data": {"ready": True}}
    return {
        "code": 0,
        "message": f"兜底接口可用：查询到 {result['total']} 条（样例 {result['returned']} 条）",
        "data": result,
    }


DOC_FILENAME = "图片上传监控系统接口文档.md"


@router.get("/api-info")
def ui_api_info():
    """接口文档页需要的信息（认证方式、接口清单、文档下载地址）"""
    cfg = store.snapshot()
    return _ok(
        {
            "api_token_required": bool(settings.api_token),
            "api_token_header": "X-API-Token",
            "version": "1.0",
            "server_time": now_iso(),
            "public_base_url": (cfg.get("public_base_url") or "").rstrip("/"),
            "swagger": "/docs",
            "openapi": "/openapi.json",
            "doc_download": "/api/ui/doc/download",
            "endpoints": [
                {"method": "POST", "path": "/api/v1/follow/request",
                 "desc": "上传完成后推送「请求跟进」（ASIN + 时间戳 + 负责人）", "auth": "可选"},
                {"method": "GET", "path": "/api/v1/follow/request?asin=B0XXXXXXXX",
                 "desc": "同上，GET 方式（最简调用）", "auth": "可选"},
                {"method": "GET", "path": "/api/v1/asin/status?asins=A,B",
                 "desc": "查询 ASIN 最新上传状态（含跟进状态）", "auth": "可选"},
                {"method": "POST", "path": "/api/v1/asin/status",
                 "desc": "同上，body 传 asins 数组", "auth": "可选"},
                {"method": "GET", "path": "/api/v1/follow/sessions?state=active",
                 "desc": "跟进任务列表", "auth": "可选"},
                {"method": "POST", "path": "/api/v1/follow/cancel",
                 "desc": "结束某个 ASIN 的跟进（body: {\"asin\":\"...\"}）", "auth": "可选"},
                {"method": "POST", "path": "/api/v1/poll/run",
                 "desc": "立即取数并判定跟进（同步，约 1-5 秒）", "auth": "可选"},
                {"method": "GET", "path": "/api/v1/summary",
                 "desc": "系统概览", "auth": "可选"},
                {"method": "GET", "path": "/api/v1/config-check",
                 "desc": "配置自检（token / 飞书 / 参数）", "auth": "可选"},
                {"method": "GET", "path": "/healthz",
                 "desc": "健康检查（无需令牌）", "auth": "无"},
            ],
            "current": {
                "poll_interval_minutes": cfg.get("poll_interval_minutes"),
                "processing_poll_interval_minutes": cfg.get("processing_poll_interval_minutes"),
                "follow_interval_minutes": cfg.get("follow_interval_minutes"),
                "follow_round_minutes": cfg.get("follow_round_minutes"),
                "follow_max_attempts": cfg.get("follow_max_attempts"),
                "status_success_value": cfg.get("status_success_value"),
                "status_failed_value": cfg.get("status_failed_value"),
                "feishu_ready": store.feishu_ready(cfg),
            },
        }
    )


@router.get("/doc/download")
def ui_doc_download():
    """下载完整接口文档（Markdown）"""
    path = Path(settings.base_dir) / DOC_FILENAME
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"文档文件不存在：{DOC_FILENAME}")
    return FileResponse(
        path, media_type="text/markdown; charset=utf-8", filename=DOC_FILENAME
    )
