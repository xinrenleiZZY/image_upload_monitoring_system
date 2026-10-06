"""极简 HTTP 客户端（标准库实现，无第三方依赖）

- 支持 JSON 请求 / 响应
- 支持 gzip / deflate 响应解压
- 支持 HTTP 代理（容器内可能需要走宿主机代理访问领星）
- 超时 + 失败重试
"""

from __future__ import annotations

import gzip
import json
import logging
import ssl
import time
import zlib
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

logger = logging.getLogger("monitor.http")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0"
)


class HttpError(Exception):
    """网络层 / HTTP 层错误"""

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


def _decode_body(raw: bytes, encoding: str) -> bytes:
    encoding = (encoding or "").lower()
    if not raw:
        return raw
    if "gzip" in encoding:
        try:
            return gzip.decompress(raw)
        except OSError:
            return raw
    if "deflate" in encoding:
        try:
            return zlib.decompress(raw)
        except zlib.error:
            try:
                return zlib.decompress(raw, -zlib.MAX_WBITS)
            except zlib.error:
                return raw
    return raw


def _build_opener(proxy: str = ""):
    handlers = []
    if proxy:
        handlers.append(request.ProxyHandler({"http": proxy, "https": proxy}))
    else:
        # 显式禁用环境变量代理，避免容器内 HTTP_PROXY 干扰
        handlers.append(request.ProxyHandler({}))
    context = ssl.create_default_context()
    handlers.append(request.HTTPSHandler(context=context))
    return request.build_opener(*handlers)


def request_json(
    url: str,
    *,
    method: str = "POST",
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    proxy: str = "",
    timeout: int = 30,
    retries: int = 2,
    retry_backoff: float = 2.0,
) -> Tuple[int, Any, str]:
    """发起请求并解析 JSON。

    Returns: (status_code, json_data, raw_text)
    Raises: HttpError（网络错误 / 非 2xx / 非 JSON 响应）
    """
    body = None
    final_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    if headers:
        final_headers.update({k: v for k, v in headers.items() if v is not None})
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        final_headers.setdefault("Content-Type", "application/json;charset=UTF-8")

    opener = _build_opener(proxy)
    last_error: Optional[Exception] = None
    for attempt in range(max(1, retries + 1)):
        req = request.Request(url, data=body, headers=final_headers, method=method.upper())
        try:
            with opener.open(req, timeout=timeout) as resp:
                raw = _decode_body(
                    resp.read(), resp.headers.get("Content-Encoding", "")
                )
                text = raw.decode("utf-8", errors="replace")
                status = getattr(resp, "status", resp.getcode())
            try:
                return status, json.loads(text), text
            except ValueError as exc:
                raise HttpError(
                    f"响应不是合法 JSON：{text[:200]}", status=status, body=text[:2000]
                ) from exc
        except error.HTTPError as exc:
            raw = b""
            try:
                raw = _decode_body(exc.read(), exc.headers.get("Content-Encoding", ""))
            except Exception:  # pragma: no cover - 读取失败不影响主流程
                pass
            text = raw.decode("utf-8", errors="replace")
            if exc.code in (401, 403):
                raise HttpError(
                    f"HTTP {exc.code} 鉴权失败（token 可能已失效）", status=exc.code, body=text
                )
            last_error = HttpError(f"HTTP {exc.code}", status=exc.code, body=text[:2000])
        except (error.URLError, TimeoutError, OSError) as exc:
            last_error = HttpError(f"网络请求失败：{exc}")

        if attempt < retries:
            wait = retry_backoff * (attempt + 1)
            logger.warning("请求失败，%s 秒后重试（第 %s 次）：%s", wait, attempt + 1, last_error)
            time.sleep(wait)

    raise last_error or HttpError("请求失败")


def post_json(url: str, payload: Dict[str, Any], **kwargs) -> Tuple[int, Any, str]:
    return request_json(url, method="POST", payload=payload, **kwargs)


def get_json(url: str, **kwargs) -> Tuple[int, Any, str]:
    return request_json(url, method="GET", **kwargs)
