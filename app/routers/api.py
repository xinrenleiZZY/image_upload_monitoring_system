"""开放 API（/api/v1）——给第三方「上传完成后请求跟进」脚本调用

主要接口：
  POST /api/v1/follow/request     接收跟进请求（ASIN + 时间戳 + 负责人[可选]）→ 马上进入跟进模式
  GET  /api/v1/asin/status        查询 ASIN 最新状态
  GET  /api/v1/follow/sessions    跟进任务列表
  POST /api/v1/follow/cancel      结束跟进
  GET  /api/v1/summary            概览
  POST /api/v1/poll/run           手动触发一次取数
"""

from __future__ import annotations

import hmac
import json
import logging
from urllib.parse import parse_qs
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import ValidationError

from app import services
from app import settings_store as store
from app.config import now_dt, now_iso, settings
from app.scheduler import scheduler
from app.schemas import AsinQuery, CancelRequest, FollowRequest

logger = logging.getLogger("monitor.api")

router = APIRouter(prefix="/api/v1", tags=["open-api"])


def _envelope(code: int, message: str, data=None, meta: Optional[dict] = None) -> dict:
    return {"code": code, "message": message, "data": data, "meta": meta or {}}


def api_token_guard(
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
) -> None:
    """配置了 API_TOKEN 才校验，方便第三方脚本先直接跑通"""
    token = settings.api_token
    if not token:
        return
    if not x_api_token or not hmac.compare_digest(x_api_token.strip(), token):
        raise HTTPException(status_code=401, detail="X-API-Token 无效或缺失")


async def _read_payload(request: Request) -> dict:
    """兼容三种传参方式：JSON body / 表单 / URL 查询参数（body 优先）"""
    payload: dict = {}
    content_type = (request.headers.get("content-type") or "").lower()
    raw_body = await request.body()
    text = raw_body.decode("utf-8", errors="replace").strip()
    if text:
        if "json" in content_type or text.startswith("{") or text.startswith("["):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    payload = parsed
                elif isinstance(parsed, list):
                    # 直接传 ASIN 数组：["B0AAA11111", "B0BBB22222"]
                    payload = {"asins": parsed}
                else:
                    # 直接传单个 ASIN 字符串："B0AAA11111"
                    payload = {"asin": parsed}
            except ValueError:
                payload = {}
        if not payload and ("form-urlencoded" in content_type or "=" in text):
            parsed_qs = parse_qs(text, keep_blank_values=True)
            payload = {k: (v[0] if len(v) == 1 else v) for k, v in parsed_qs.items()}
    if not payload and not text:
        try:
            form = await request.form()
            payload = {k: v for k, v in form.items()}
        except Exception:
            payload = {}
    query: dict = {}
    for key, value in request.query_params.items():
        query.setdefault(key, value)
    merged = {**query, **payload}
    return merged


async def _handle_follow_request(request: Request) -> dict:
    payload = await _read_payload(request)
    try:
        body = FollowRequest(**payload)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"请求参数不合法：{exc.errors()[:3]}")
    asins = body.asin_list()
    if not asins:
        raise HTTPException(
            status_code=400,
            detail="缺少 asin / parent_asin / asins（支持 JSON body、表单或 URL 参数）",
        )
    if len(asins) > 200:
        raise HTTPException(status_code=400, detail="单次最多 200 个 ASIN")

    ts_value = body.timestamp_value()
    payload_dump = body.model_dump() if hasattr(body, "model_dump") else body.dict()
    delay_minutes = store.get_int("follow_request_delay_minutes", 2)
    first_check_at = ""
    results = []
    created_count = 0
    reused_count = 0
    for asin in asins:
        try:
            session, created = services.open_session(
                asin,
                source="api_request",
                owner=body.owner_text(),
                request_ts=ts_value,
                payload=payload_dump,
                notify=True,
                actor="api",
            )
        except (ValueError, RuntimeError) as exc:
            results.append({"asin": asin, "ok": False, "error": str(exc)})
            continue
        if created:
            created_count += 1
        else:
            reused_count += 1
        first_check_at = session.get("next_tick_at") or first_check_at
        record = services.latest_record_for(asin)
        results.append(
            {
                "asin": asin,
                "ok": True,
                "session_id": session.get("id"),
                "created": created,
                "state": session.get("state"),
                "owner": session.get("owner") or "",
                "status": (record or {}).get("status"),
                "status_text": (record or {}).get("status_text", services.STATUS_UNKNOWN),
                "status_text_cn": services.status_text_cn(
                    (record or {}).get("status_text")
                ),
                "last_status_name": (record or {}).get("status_name", ""),
                "next_tick_at": session.get("next_tick_at"),
                "first_check_at": session.get("next_tick_at"),
                "next_note": "每 %s 分钟检查一次，失败自动发飞书重试消息"
                % (store.get_int("follow_interval_minutes", 10)),
            }
        )
    # 立刻唤醒调度线程：马上取一次数并判定（接口本身快速返回）
    scheduler.wake()
    accepted = sum(1 for item in results if item.get("ok"))
    if accepted == 0:
        raise HTTPException(
            status_code=400,
            detail="；".join(
                f"{item['asin']}: {item.get('error', '接收失败')}" for item in results
            )[:500],
        )
    return _envelope(
        0,
        f"已接收 {accepted}/{len(results)} 个 ASIN，逐个建立跟进任务（新建 {created_count} 个、"
        f"沿用进行中 {reused_count} 个）；"
        f"首次检查 {delay_minutes} 分钟后（{first_check_at or '-'}）",
        {
            "items": results,
            "accepted": accepted,
            "created": created_count,
            "reused": reused_count,
            "check_delay_minutes": delay_minutes,
            "first_check_at": first_check_at,
            "request_ts": str(ts_value) if ts_value is not None else "",
            "owner": body.owner_text(),
            "received_at": now_iso(),
        },
    )


@router.post("/follow/request", summary="接收跟进请求（产品图片上传完成 → 请求跟进）")
async def follow_request_post(request: Request, _ok: None = Depends(api_token_guard)):
    """第三方脚本推送 ASIN + 时间戳 + 负责人（可无）后，立即进入跟进模式：
    每 10 分钟查一次领星最新状态，失败发重试消息，成功结束跟进。
    """
    return await _handle_follow_request(request)


@router.get("/follow/request", summary="接收跟进请求（GET 方式，便于简单脚本调用）")
async def follow_request_get(request: Request, _ok: None = Depends(api_token_guard)):
    return await _handle_follow_request(request)


@router.get("/asin/status", summary="查询 ASIN 最新上传状态")
def query_status(
    asins: str = Query(..., description="ASIN 列表，逗号/分号分隔"),
    _ok: None = Depends(api_token_guard),
):
    raw = str(asins or "").replace("，", ",").replace(";", ",")
    items = [x.strip() for x in raw.split(",") if x.strip()]
    if not items:
        raise HTTPException(status_code=400, detail="请至少提供一个 ASIN")
    if len(items) > 500:
        raise HTTPException(status_code=400, detail="单次最多查询 500 个 ASIN")
    data = services.asin_status(*items)
    return _envelope(0, "ok", {"items": data, "total": len(data)})


@router.post("/asin/status", summary="查询 ASIN 最新上传状态（POST）")
def query_status_post(body: AsinQuery, _ok: None = Depends(api_token_guard)):
    asins = body.asin_list()
    if not asins:
        raise HTTPException(status_code=400, detail="缺少 asins")
    data = services.asin_status(*asins)
    return _envelope(0, "ok", {"items": data, "total": len(data)})


@router.get("/follow/sessions", summary="跟进任务列表")
def list_sessions(
    state: str = Query(default="", description="active / completed / failed / cancelled / closed"),
    source: str = Query(default=""),
    asin: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    _ok: None = Depends(api_token_guard),
):
    data = services.list_sessions(
        state=state, source=source, asin=asin, page=page, page_size=page_size
    )
    return _envelope(0, "ok", data)


@router.post("/follow/cancel", summary="结束某个 ASIN 的跟进")
def cancel(body: CancelRequest, _ok: None = Depends(api_token_guard)):
    asins = body.asin_list()
    if not asins:
        raise HTTPException(status_code=400, detail="缺少 asin")
    results = [
        {"asin": asin, "ok": services.cancel_session(asin, actor="api")} for asin in asins
    ]
    return _envelope(0, "ok", {"items": results})


@router.post("/poll/run", summary="手动触发一次取数（同步返回结果）")
def poll_run(_ok: None = Depends(api_token_guard)):
    result = services.poll_once("api_manual", force=True)
    code = 0 if result.get("ok") else 1
    return _envelope(code, "ok" if result.get("ok") else str(result.get("error", "取数失败")), result)


@router.get("/summary", summary="系统概览")
def get_summary(_ok: None = Depends(api_token_guard)):
    data = services.summary()
    data["scheduler"] = scheduler.status()
    data["server_time"] = now_iso()
    data["timezone"] = str(now_dt().tzinfo)
    return _envelope(0, "ok", data)


@router.get("/config-check", summary="配置自检（token / 飞书 / 参数）")
def config_check(_ok: None = Depends(api_token_guard)):
    cfg = store.snapshot()
    return _envelope(
        0,
        "ok",
        {
            "lingxing_ready": store.lingxing_ready(cfg),
            "lingxing_proxy": cfg.get("lingxing_proxy"),
            "lingxing_url": (cfg.get("lingxing_base_url") or "").rstrip("/")
            + (cfg.get("lingxing_path") or ""),
            "feishu_ready": store.feishu_ready(cfg),
            "feishu_targets": {
                "chat_id": bool(cfg.get("feishu_chat_id")),
                "open_id": bool(cfg.get("feishu_receiver_open_id")),
                "webhook": bool(cfg.get("feishu_webhook_url")),
                "bot_mention": bool(cfg.get("feishu_bot_open_id")),
            },
            "status_values": {
                "success": cfg.get("status_success_value"),
                "failed": cfg.get("status_failed_value"),
            },
            "intervals": {
                "poll_minutes": cfg.get("poll_interval_minutes"),
                "processing_minutes": cfg.get("processing_poll_interval_minutes"),
                "follow_minutes": cfg.get("follow_interval_minutes"),
                "round_minutes": cfg.get("follow_round_minutes"),
                "max_attempts": cfg.get("follow_max_attempts"),
            },
        },
    )
