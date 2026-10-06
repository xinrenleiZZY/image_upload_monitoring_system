"""领星「亚马逊上传队列」接口客户端

接口：POST {base_url}/listing-publish-api/api/AmazonPublishQueue/list
返回：{"code":1,"msg":"成功","data":{"list":[...],"count":147}}

取数策略：按 offset/length 翻页拉全量（接口不支持按 ASIN 过滤，实测传 asin/keyword 均被忽略），
翻页终止条件 = 本页返回 0 条 / 已取条数 >= count / 达到最大页数。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.config import get_timezone, now_dt
from app.http_client import HttpError, request_json
from app import settings_store as store

logger = logging.getLogger("monitor.lingxing")

# 记录里可能出现的字段别名（不同版本/不同队列类型字段名略有差异）
ALIASES: Dict[str, List[str]] = {
    "queue_unique_id": ["queue_unique_id", "queue_id", "unique_id", "id"],
    "parent_asin": ["parent_asin", "parentAsin", "parent_asin_code"],
    "child_asin": ["asin", "child_asin", "childAsin"],
    "msku": ["msku", "seller_sku", "msku_code"],
    "fnsku": ["fnsku"],
    "item_name": ["item_name", "title", "product_name"],
    "store_name": ["store_name", "store"],
    "country_name": ["country_name", "country"],
    "marketplace_id": ["marketplace_id", "marketplace"],
    "queue_type": ["queue_type"],
    "queue_type_name": ["queue_type_name"],
    "status": ["status"],
    "status_name": ["status_name"],
    "failure_reason": ["failure_reason", "fail_reason", "error_message"],
    "user_name": ["user_name", "operator", "user"],
    "operate_time": ["operate_time", "create_time", "gmt_create"],
    "finish_time": ["finish_time", "completed_time"],
    "asin_url": ["asin_url"],
    "main_image_url": ["main_image_url"],
    "parent_sku": ["parent_sku"],
}

# raw_json 中不落库的大字段（图片清单，体积大且无监控价值）
HEAVY_FIELDS = ("adjust_content",)


class LingxingError(Exception):
    """接口业务错误（code != 1 / 鉴权失效 等）"""

    def __init__(self, message: str, *, auth_invalid: bool = False):
        super().__init__(message)
        self.auth_invalid = auth_invalid


@dataclass
class FetchResult:
    records: List[Dict[str, Any]] = field(default_factory=list)
    api_total: int = 0
    http_requests: int = 0
    window_start: str = ""
    window_end: str = ""
    raw_items: int = 0


def _first(item: Dict[str, Any], keys: List[str]):
    for key in keys:
        if key in item and item[key] not in (None, ""):
            return item[key]
    return None


def _to_int(value) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def normalize_record(item: Dict[str, Any], *, success_value: int, failed_value: int) -> Dict[str, Any]:
    """把接口原始记录转为统一结构"""
    status = _to_int(_first(item, ALIASES["status"]))
    if status is None:
        status_text = "unknown"
    elif status == success_value:
        status_text = "success"
    elif status == failed_value:
        status_text = "failed"
    else:
        status_text = "processing"

    pruned = {k: v for k, v in item.items() if k not in HEAVY_FIELDS}
    return {
        "queue_unique_id": _to_text(_first(item, ALIASES["queue_unique_id"])),
        "parent_asin": _to_text(_first(item, ALIASES["parent_asin"])).upper(),
        "child_asin": _to_text(_first(item, ALIASES["child_asin"])).upper(),
        "msku": _to_text(_first(item, ALIASES["msku"])),
        "fnsku": _to_text(_first(item, ALIASES["fnsku"])),
        "item_name": _to_text(_first(item, ALIASES["item_name"])),
        "store_name": _to_text(_first(item, ALIASES["store_name"])),
        "country_name": _to_text(_first(item, ALIASES["country_name"])),
        "marketplace_id": _to_text(_first(item, ALIASES["marketplace_id"])),
        "queue_type": _to_int(_first(item, ALIASES["queue_type"])),
        "queue_type_name": _to_text(_first(item, ALIASES["queue_type_name"])),
        "status": status,
        "status_name": _to_text(_first(item, ALIASES["status_name"])),
        "status_text": status_text,
        "failure_reason": _to_text(_first(item, ALIASES["failure_reason"])),
        "user_name": _to_text(_first(item, ALIASES["user_name"])),
        "operate_time": _to_text(_first(item, ALIASES["operate_time"])),
        "finish_time": _to_text(_first(item, ALIASES["finish_time"])),
        "asin_url": _to_text(_first(item, ALIASES["asin_url"])),
        "main_image_url": _to_text(_first(item, ALIASES["main_image_url"])),
        "raw_json": json.dumps(pruned, ensure_ascii=False),
    }


def revision_of(record: Dict[str, Any]) -> str:
    """观测指纹：状态 + 结果时间 + 失败原因（用于判断是否有「最新的 ASIN 状态数据」）"""
    return "|".join(
        [
            str(record.get("queue_unique_id") or ""),
            str(record.get("status") if record.get("status") is not None else ""),
            str(record.get("status_name") or ""),
            str(record.get("finish_time") or ""),
            str(record.get("operate_time") or ""),
            str(record.get("failure_reason") or "")[:200],
        ]
    )


def _int_list(raw: str) -> List[int]:
    values: List[int] = []
    for part in str(raw or "").replace("，", ",").replace(";", ",").split(","):
        text = part.strip()
        if not text:
            continue
        try:
            values.append(int(float(text)))
        except ValueError:
            continue
    return values


def build_window(cfg: Dict[str, Any]) -> Tuple[str, str]:
    """计算 start_time / end_time"""
    end = now_dt().replace(microsecond=0)
    fixed_start = str(cfg.get("query_start_time") or "").strip()
    if fixed_start:
        start_text = fixed_start
    else:
        days = max(1, int(cfg.get("query_window_days", 45)))
        start = end - dt.timedelta(days=days)
        start_text = start.strftime("%Y-%m-%d 00:00:00")
    return start_text, end.strftime("%Y-%m-%d %H:%M:%S")


class LingxingClient:
    """按当前设置发起请求（每次取数都重新读取设置，改完参数立即生效）"""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or store.snapshot()

    # ── 请求头 ──
    def headers(self) -> Dict[str, str]:
        cfg = self.cfg
        origin = cfg.get("lingxing_origin") or "https://huizhixin.lingxing.com"
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "AK-Client-Type": cfg.get("lingxing_client_type") or "web",
            "AK-Origin": origin,
            "Origin": origin,
            "Referer": origin.rstrip("/") + "/",
            "X-AK-Company-Id": cfg.get("lingxing_company_id") or "",
            "X-AK-ENV-KEY": cfg.get("lingxing_env_key") or "",
            "X-AK-Language": cfg.get("lingxing_language") or "zh",
            "X-AK-PLATFORM": cfg.get("lingxing_platform") or "1",
            "X-AK-Request-Id": str(uuid.uuid4()),
            "X-AK-Request-Source": cfg.get("lingxing_request_source") or "erp",
            "X-AK-Uid": cfg.get("lingxing_uid") or "",
            "X-AK-Version": cfg.get("lingxing_version") or "3.9.1.3.0.114",
            "X-AK-Zid": cfg.get("lingxing_zid") or "1",
            "auth-token": cfg.get("lingxing_auth_token") or "",
        }

    def url(self) -> str:
        base = (self.cfg.get("lingxing_base_url") or "").rstrip("/")
        path = self.cfg.get("lingxing_path") or ""
        if not path.startswith("/"):
            path = "/" + path
        return base + path

    # ── 单页 ──
    def fetch_page(
        self,
        offset: int,
        length: int,
        *,
        window_start: str,
        window_end: str,
        seq: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        cfg = self.cfg
        path = str(cfg.get("lingxing_path") or "")
        payload: Dict[str, Any] = {
            "offset": int(offset),
            "length": int(length),
            "queue_type": _int_list(cfg.get("query_queue_type", "")),
            "status": _int_list(cfg.get("query_status", "")),
            "country_id": _int_list(cfg.get("query_country_id", "")),
            "store_id": _int_list(cfg.get("query_store_id", "")),
            "user_id": _int_list(cfg.get("query_user_id", "")),
            "start_time": window_start,
            "end_time": window_end,
            "is_archive": int(cfg.get("query_is_archive", 0) or 0),
            "req_time_sequence": f"{path}$${seq}",
        }
        try:
            status, data, text = request_json(
                self.url(),
                method="POST",
                payload=payload,
                headers=self.headers(),
                proxy=str(cfg.get("lingxing_proxy") or ""),
                timeout=int(cfg.get("lingxing_timeout_seconds", 30) or 30),
                retries=2,
            )
        except HttpError as exc:
            raise LingxingError(
                f"请求领星接口失败：{exc}"
                + (f"｜响应：{exc.body[:300]}" if getattr(exc, "body", "") else ""),
                auth_invalid=(exc.status in (401, 403)),
            ) from exc

        if status != 200:
            raise LingxingError(f"领星接口返回 HTTP {status}：{text[:200]}")
        if not isinstance(data, dict):
            raise LingxingError(f"领星接口响应格式异常：{text[:200]}")

        code = data.get("code")
        msg = str(data.get("msg") or data.get("message") or "")
        if code not in (1, 0, "1", "0", None):
            auth_invalid = any(
                word in msg for word in ("登录", "token", "Token", "鉴权", "未授权", "过期")
            )
            raise LingxingError(f"领星接口返回失败：code={code} msg={msg}", auth_invalid=auth_invalid)

        items, total = self._extract(data)
        return items, total

    @staticmethod
    def _extract(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
        """从响应中取 (记录列表, 总数)：标准结构 data.list + data.count，另兼容其他包裹层级"""
        payload = data.get("data", data)
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)], len(payload)
        if isinstance(payload, dict):
            for key in ("list", "items", "rows", "records", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    total = 0
                    for count_key in ("count", "total", "total_count", "totalCount"):
                        number = _to_int(payload.get(count_key))
                        if number is not None:
                            total = number
                            break
                    return [x for x in value if isinstance(x, dict)], total or len(value)
        return [], 0

    # ── 全量 ──
    def fetch_all(
        self,
        *,
        window_start: str = "",
        window_end: str = "",
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        progress=None,
    ) -> FetchResult:
        cfg = self.cfg
        if not cfg.get("lingxing_auth_token"):
            raise LingxingError("未配置 auth-token，无法取数（请在「系统设置」中填写）")

        if not window_start or not window_end:
            window_start, window_end = build_window(cfg)
        size = int(page_size or cfg.get("query_page_size", 100) or 100)
        size = max(1, min(size, 500))
        pages_limit = int(max_pages or cfg.get("query_max_pages", 50) or 50)
        seq_start = int(cfg.get("req_seq_start", 1) or 1)
        success_value = int(cfg.get("status_success_value", 3))
        failed_value = int(cfg.get("status_failed_value", 4))

        result = FetchResult(window_start=window_start, window_end=window_end)
        offset = 0
        page = 0
        while page < pages_limit:
            items, total = self.fetch_page(
                offset,
                size,
                window_start=window_start,
                window_end=window_end,
                seq=seq_start + page,
            )
            page += 1
            result.http_requests += 1
            if total:
                result.api_total = total
            if not items:
                break
            for item in items:
                normalized = normalize_record(
                    item, success_value=success_value, failed_value=failed_value
                )
                if not normalized["queue_unique_id"]:
                    continue
                result.records.append(normalized)
            if progress:
                progress(page=page, offset=offset, fetched=len(result.records), total=result.api_total)
            offset += len(items)
            if result.api_total and offset >= result.api_total:
                break
            if len(items) < size and not result.api_total:
                break

        # 同一 ASIN 可能多行；queue_unique_id 去重（保留最后出现的）
        deduped: Dict[str, Dict[str, Any]] = {}
        for record in result.records:
            deduped[record["queue_unique_id"]] = record
        result.records = list(deduped.values())
        result.raw_items = len(result.records)
        logger.info(
            "领星取数完成：%s 条（接口 count=%s，请求 %s 次，窗口 %s ~ %s）",
            len(result.records),
            result.api_total,
            result.http_requests,
            window_start,
            window_end,
        )
        return result


def latest_by_asin(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """按 parent_asin 归并出「最新一条」记录（queue_unique_id / 时间 取最大）"""
    latest: Dict[str, Dict[str, Any]] = {}
    for record in records:
        asin = record.get("parent_asin") or ""
        if not asin:
            continue
        current = latest.get(asin)
        if current is None or _sort_key(record) > _sort_key(current):
            latest[asin] = record
    return latest


def _sort_key(record: Dict[str, Any]):
    queue_id = str(record.get("queue_unique_id") or "")
    numeric = int(queue_id) if queue_id.isdigit() else 0
    times = " ".join(
        [str(record.get("finish_time") or ""), str(record.get("operate_time") or "")]
    )
    return (numeric, times)


def local_now_text() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


# ────────────────────── 兜底数据源：自建领星 API 站 ──────────────────────

class LingxingProxyClient:
    """自建 API 站客户端：默认作为**主数据源**（fallback_as_primary），也可作领星直连的兜底。

    请求体（参考接口库用法）：
        {"api_name": "Amazon刊登队列列表（AmazonPublishQueue/list）",
         "account": "xiazhanggui",
         "auto_pagination": true,
         "body": {"start_time": ..., "end_time": ..., "offset": 0, "length": 20, ...}}
    请求头：X-User-Id / X-User-Token（自建站鉴权）
    返回：{"success": true, "total": N, "data": [...]}（代理已归一化）

    自愈：当自建站返回「用户Token无效或已过期」（error=unauthorized）且配置了账号密码时，
    自动调 /api/user/login 重登、写回新的 X-User-Token 并重试一次。
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or store.snapshot()

    @property
    def base_url(self) -> str:
        return str(self.cfg.get("fallback_base_url") or "").rstrip("/")

    @property
    def path(self) -> str:
        path = str(self.cfg.get("fallback_path") or "/api/proxy")
        return path if path.startswith("/") else "/" + path

    @property
    def ready(self) -> bool:
        """是否配置齐（地址 + 自建站账号令牌）"""
        return bool(
            self.cfg.get("fallback_enabled")
            and self.base_url
            and self.cfg.get("fallback_user_id")
            and self.cfg.get("fallback_user_token")
        )

    def headers(self) -> Dict[str, str]:
        return {
            "X-User-Id": str(self.cfg.get("fallback_user_id") or ""),
            "X-User-Token": str(self.cfg.get("fallback_user_token") or ""),
            "Content-Type": "application/json;charset=UTF-8",
        }

    def _payload(self, body: Dict[str, Any], *, auto_pagination: Optional[bool] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        api_name = str(self.cfg.get("fallback_api_name") or "").strip()
        api_id = str(self.cfg.get("fallback_api_id") or "").strip()
        if api_name:
            payload["api_name"] = api_name
        elif api_id:
            payload["api_id"] = api_id
        account = str(self.cfg.get("fallback_account") or "").strip()
        if account:
            payload["account"] = account
        paginate = (
            bool(self.cfg.get("fallback_auto_pagination"))
            if auto_pagination is None
            else auto_pagination
        )
        if paginate:
            payload["auto_pagination"] = True
        payload["body"] = body
        return payload

    def _can_auto_login(self) -> bool:
        """是否具备自动重登条件（开关 + 账号 + 密码）"""
        return bool(
            self.cfg.get("fallback_auto_login")
            and self.cfg.get("fallback_user_id")
            and self.cfg.get("fallback_user_password")
        )

    @staticmethod
    def _user_token_invalid(data: Any) -> bool:
        """是否为「自建站账号令牌失效」（区别于领星 Token 过期，后者不在此处理）"""
        if not isinstance(data, dict):
            return False
        if str(data.get("error") or "").strip() == "unauthorized":
            return True
        message = str(data.get("message") or data.get("msg") or "")
        return "用户Token" in message or "user_token" in message.lower()

    def _login(self) -> str:
        """用已保存的账号密码重新登录自建站，取回新的 X-User-Token 并写回设置"""
        cfg = self.cfg
        user_id = str(cfg.get("fallback_user_id") or "")
        password = str(cfg.get("fallback_user_password") or "")
        if not user_id or not password:
            raise LingxingError("自动重登失败：未配置 X-User-Id 或账号密码")
        logger.info("自建站账号令牌失效，尝试自动重登：user_id=%s", user_id)
        try:
            status, data, text = request_json(
                self.base_url + "/api/user/login",
                method="POST",
                payload={"user_id": user_id, "password": password},
                headers={"Content-Type": "application/json;charset=UTF-8"},
                proxy="",
                timeout=30,
                retries=0,
            )
        except HttpError as exc:
            raise LingxingError(f"自动重登请求失败：{exc}") from exc
        if not isinstance(data, dict):
            raise LingxingError(f"自动重登响应格式异常：{text[:200]}")
        token = str(data.get("user_token") or (data.get("data") or {}).get("user_token") or "")
        if not token:
            raise LingxingError(
                "自动重登失败："
                + str(data.get("message") or data.get("detail") or data.get("error") or text[:200])
            )
        store.update({"fallback_user_id": user_id, "fallback_user_token": token})
        self.cfg = store.snapshot()
        logger.info("自建站自动重登成功，已更新 X-User-Token（%s…）", token[:8])
        return token

    def _request(self, body: Dict[str, Any], *, auto_pagination: Optional[bool] = None,
                 _auth_retry: bool = True) -> Tuple[List[Dict[str, Any]], int]:
        try:
            status, data, text = request_json(
                self.base_url + self.path,
                method="POST",
                payload=self._payload(body, auto_pagination=auto_pagination),
                headers=self.headers(),
                proxy="",  # 内网自建站直连，不走代理
                timeout=int(self.cfg.get("fallback_timeout_seconds", 120) or 120),
                retries=2,  # 抗 TUN 劫持等瞬时断连（RemoteDisconnected），避免误报兜底失败
            )
        except HttpError as exc:
            if _auth_retry and exc.status in (401, 403) and self._can_auto_login():
                self._login()
                return self._request(body, auto_pagination=auto_pagination, _auth_retry=False)
            raise LingxingError(
                f"兜底接口请求失败：{exc}"
                + (f"｜响应：{exc.body[:300]}" if getattr(exc, "body", "") else "")
            ) from exc
        if status != 200:
            raise LingxingError(f"兜底接口返回 HTTP {status}：{text[:200]}")
        if not isinstance(data, dict):
            raise LingxingError(f"兜底接口响应格式异常：{text[:200]}")
        if data.get("success") is False:
            if _auth_retry and self._can_auto_login() and self._user_token_invalid(data):
                self._login()
                return self._request(body, auto_pagination=auto_pagination, _auth_retry=False)
            raise LingxingError(
                f"兜底接口返回失败：{data.get('message') or data.get('msg') or str(data)[:200]}"
            )
        items, total = self._extract(data)
        return items, total

    @staticmethod
    def _extract(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
        """兼容两种结构：{success,total,data:[...]}（归一化）与 {code,msg,data:{list,count}}（原始）"""
        payload = data.get("data", data)
        if isinstance(payload, list):
            total = _to_int(data.get("total") or data.get("count")) or len(payload)
            return [x for x in payload if isinstance(x, dict)], total
        if isinstance(payload, dict):
            for key in ("list", "data", "items", "rows", "records"):
                value = payload.get(key)
                if isinstance(value, list):
                    total = 0
                    for count_key in ("total", "count", "total_count", "totalCount"):
                        number = _to_int(payload.get(count_key) or data.get(count_key))
                        if number is not None:
                            total = number
                            break
                    return [x for x in value if isinstance(x, dict)], total or len(value)
        return [], 0

    def fetch_all(
        self,
        *,
        window_start: str = "",
        window_end: str = "",
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        progress=None,
    ) -> FetchResult:
        cfg = self.cfg
        if not self.ready:
            raise LingxingError("兜底接口未配置完整（需要地址 + X-User-Id + X-User-Token）")
        if not window_start or not window_end:
            window_start, window_end = build_window(cfg)
        size = int(page_size or cfg.get("query_page_size", 100) or 100)
        size = max(1, min(size, 500))
        success_value = int(cfg.get("status_success_value", 3))
        failed_value = int(cfg.get("status_failed_value", 4))
        base_body: Dict[str, Any] = {
            "start_time": window_start,
            "end_time": window_end,
            "offset": 0,
            "length": size,
            "queue_type": _int_list(cfg.get("query_queue_type", "")),
            "status": _int_list(cfg.get("query_status", "")),
            "country_id": _int_list(cfg.get("query_country_id", "")),
            "store_id": _int_list(cfg.get("query_store_id", "")),
            "user_id": _int_list(cfg.get("query_user_id", "")),
            "is_archive": int(cfg.get("query_is_archive", 0) or 0),
        }
        result = FetchResult(window_start=window_start, window_end=window_end)

        if cfg.get("fallback_auto_pagination"):
            # 由自建站按 total 自动翻页合并，一次拿全量
            items, total = self._request(base_body, auto_pagination=True)
            result.http_requests += 1
            result.api_total = total
            for item in items:
                normalized = normalize_record(
                    item, success_value=success_value, failed_value=failed_value
                )
                if normalized["queue_unique_id"]:
                    result.records.append(normalized)
        else:
            pages_limit = int(max_pages or cfg.get("query_max_pages", 50) or 50)
            offset = 0
            page = 0
            while page < pages_limit:
                body = dict(base_body, offset=offset, length=size)
                items, total = self._request(body, auto_pagination=False)
                page += 1
                result.http_requests += 1
                if total:
                    result.api_total = total
                if not items:
                    break
                for item in items:
                    normalized = normalize_record(
                        item, success_value=success_value, failed_value=failed_value
                    )
                    if normalized["queue_unique_id"]:
                        result.records.append(normalized)
                offset += len(items)
                if result.api_total and offset >= result.api_total:
                    break

        deduped: Dict[str, Dict[str, Any]] = {}
        for record in result.records:
            deduped[record["queue_unique_id"]] = record
        result.records = list(deduped.values())
        result.raw_items = len(result.records)
        logger.info(
            "兜底取数完成：%s 条（total=%s，请求 %s 次）",
            len(result.records),
            result.api_total,
            result.http_requests,
        )
        return result

    def probe(self, *, window_start: str = "", window_end: str = "") -> Dict[str, Any]:
        """配置自检：取 2 条样本，验证账号/账号令牌/接口名是否可用"""
        if not window_start or not window_end:
            window_start, window_end = build_window(self.cfg)
        body = {
            "start_time": window_start,
            "end_time": window_end,
            "offset": 0,
            "length": 2,
        }
        items, total = self._request(body, auto_pagination=False)
        sample = items[0] if items else {}
        return {
            "ok": True,
            "total": total,
            "returned": len(items),
            "sample": {
                "msku": sample.get("msku") or sample.get("seller_sku") or "",
                "asin": sample.get("asin") or sample.get("parent_asin") or "",
                "status": sample.get("status"),
                "status_name": sample.get("status_name") or "",
                "store_name": sample.get("store_name") or "",
            },
            "window": {"start": window_start, "end": window_end},
        }


def tz_name() -> str:
    zone = get_timezone()
    return getattr(zone, "key", str(zone))
