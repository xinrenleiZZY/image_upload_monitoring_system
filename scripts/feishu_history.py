"""查看飞书群里最近的真实消息（用于核对「系统到底发了什么」）

用法：
    python scripts/feishu_history.py            # 最近 1 小时
    python scripts/feishu_history.py --hours 6  # 最近 6 小时
    python scripts/feishu_history.py --limit 50

说明：需要应用具备「获取群组中消息」权限（im:message 只读）；没有权限时会提示查询失败。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from app.feishu import FEISHU_API, FeishuNotifier  # noqa: E402
from app.http_client import request_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="查看飞书群最近消息")
    parser.add_argument("--hours", type=float, default=1.0, help="查询最近多少小时（默认 1）")
    parser.add_argument("--limit", type=int, default=50, help="每个群最多取多少条")
    args = parser.parse_args()

    notifier = FeishuNotifier()
    if not notifier.enabled:
        print("飞书未配置完整，请先在网页「系统设置 → 飞书通知」里配置")
        return 2
    token = notifier._app_token()
    if not token:
        print("获取 tenant_access_token 失败（网络或凭证问题）")
        return 1

    start = int(time.time() - max(0.1, args.hours) * 3600)
    for chat in notifier.chat_ids:
        url = (
            f"{FEISHU_API}/im/v1/messages?container_id_type=chat&container_id={chat}"
            f"&start_time={start}&page_size={min(50, max(1, args.limit))}&sort_type=ByCreateTimeDesc"
        )
        try:
            _, data, _ = request_json(
                url,
                method="GET",
                headers={"Authorization": f"Bearer {token}"},
                timeout=20,
                retries=0,
            )
        except Exception as exc:
            print(f"群 {chat} 查询失败：{str(exc)[:200]}")
            print("  提示：应用需要「获取群组中消息」权限（im:message:readonly）")
            continue
        items = ((data or {}).get("data") or {}).get("items") or []
        print(f"=== 群 {chat} 最近 {len(items)} 条 ===")
        for item in items:
            raw = (item.get("body") or {}).get("content", "")
            try:
                text = json.loads(raw).get("text", raw)
            except ValueError:
                text = raw
            when = dt.datetime.fromtimestamp(
                int(item.get("create_time") or 0) / 1000
            ).strftime("%m-%d %H:%M:%S")
            sender = (item.get("sender") or {}).get("id", "")
            print(f"  {when} [{item.get('msg_type')}] {sender[-6:]}：{text[:160]}")
        if not items:
            print("  （该时间段内没有消息，或应用缺少读取权限）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
