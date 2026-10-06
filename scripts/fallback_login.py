"""自建领星 API 站：注册 / 登录 / 保存凭证 / 测试代理接口

用法（在本机或容器里执行都行，配置写入同一个数据库）：
    python scripts/fallback_login.py --user-id apq7609 --password 'AmazonPublishQueue7609'
    python scripts/fallback_login.py --user-id apq7609 --password '...' --no-register   # 已注册则跳过注册
    python scripts/fallback_login.py --test-only                                        # 只用已保存的凭证测一次

流程：POST /api/user/register（已存在会忽略）→ POST /api/user/login 拿 user_token
      → 保存到「系统设置 → 兜底接口」的 X-User-Id / X-User-Token
      → POST /api/proxy 调一次「Amazon刊登队列列表」验证是否可用
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from app import db  # noqa: E402
from app import settings_store as store  # noqa: E402
from app.http_client import HttpError, request_json  # noqa: E402


def _post(url: str, payload: Dict[str, Any], headers: Dict[str, str] | None = None,
          timeout: int = 30) -> Dict[str, Any]:
    status, data, text = request_json(
        url, method="POST", payload=payload, headers=headers or {}, proxy="",
        timeout=timeout, retries=0,
    )
    if not isinstance(data, dict):
        raise RuntimeError(f"响应不是 JSON（HTTP {status}）：{text[:200]}")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="自建领星 API 站登录与自检")
    parser.add_argument("--user-id", default="", help="代理账号 user_id")
    parser.add_argument("--password", default="", help="代理账号密码")
    parser.add_argument("--no-register", action="store_true", help="跳过注册，直接登录")
    parser.add_argument("--test-only", action="store_true", help="只用已保存的凭证测试代理接口")
    parser.add_argument("--server", default="", help="覆盖自建站地址（默认取系统设置）")
    args = parser.parse_args()

    db.init_schema()
    cfg = store.snapshot()
    server = (args.server or str(cfg.get("fallback_base_url") or "")).rstrip("/")
    if not server:
        print("未配置自建站地址（系统设置 → 兜底接口 → 自建 API 站地址）")
        return 2
    path = str(cfg.get("fallback_path") or "/api/proxy")

    user_id = args.user_id or str(cfg.get("fallback_user_id") or "")
    password = args.password

    if not args.test_only:
        if not user_id or not password:
            print("缺少 --user-id / --password")
            return 2
        if not args.no_register:
            data = _post(f"{server}/api/user/register",
                         {"user_id": user_id, "password": password})
            print("注册：", data.get("message") or data.get("detail") or data.get("error") or data)
        data = _post(f"{server}/api/user/login",
                     {"user_id": user_id, "password": password})
        token = data.get("user_token") or (data.get("data") or {}).get("user_token")
        if not token:
            print("登录失败：", json.dumps(data, ensure_ascii=False)[:300])
            return 1
        print(f"登录成功：user_id={user_id} user_token={token[:8]}…（共 {len(token)} 位）")
        saved = store.update({"fallback_user_id": user_id, "fallback_user_token": token,
                              "fallback_enabled": True})
        print("已保存到系统设置：", saved["saved"])
        headers = {"X-User-Id": user_id, "X-User-Token": token}
    else:
        token = str(cfg.get("fallback_user_token") or "")
        if not user_id or not token:
            print("未保存过凭证，请先用 --user-id/--password 登录一次")
            return 2
        headers = {"X-User-Id": user_id, "X-User-Token": token}

    # 测一次代理接口（只取 2 条样例）
    payload: Dict[str, Any] = {"body": {"offset": 0, "length": 2}}
    api_name = str(cfg.get("fallback_api_name") or "").strip()
    api_id = str(cfg.get("fallback_api_id") or "").strip()
    if api_name:
        payload["api_name"] = api_name
    elif api_id:
        payload["api_id"] = api_id
    account = str(cfg.get("fallback_account") or "").strip()
    if account:
        payload["account"] = account
    try:
        data = _post(f"{server}{path}", payload, headers=headers, timeout=120)
    except HttpError as exc:
        print(f"代理接口请求失败：{exc}")
        return 1
    if data.get("success") is False:
        print("代理接口返回失败：", json.dumps(data, ensure_ascii=False)[:400])
        print("提示：如果提示未绑定领星账号，需要在自建站上先完成 /api/auth/login 绑定")
        return 1
    items = data.get("data") or []
    sample = items[0] if items else {}
    print(f"代理接口可用：total={data.get('total')}，返回 {len(items)} 条")
    if sample:
        print("样例：", json.dumps({
            "msku": sample.get("msku"), "asin": sample.get("asin"),
            "status_name": sample.get("status_name"), "store_name": sample.get("store_name"),
            "finish_time": sample.get("finish_time"),
        }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
