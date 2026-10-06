"""手动发送飞书消息（重试消息 / 成功消息 / 自定义文本）

⚠️ 默认「演练模式」：只打印将要发送的内容，不会真的发到群里。
   确认要发时加 --live：

用法：
    python scripts/send_retry_message.py --asin B0HCVJ6HLY --owner 张三            # 只预览
    python scripts/send_retry_message.py --asin B0HCVJ6HLY --owner 张三 --live     # 真的发送
    python scripts/send_retry_message.py --text "自定义消息" --live
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import db, services  # noqa: E402
from app.feishu import FeishuNotifier  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="手动发送飞书消息")
    parser.add_argument("--asin", default="", help="ASIN")
    parser.add_argument("--owner", default="", help="负责人（可空）")
    parser.add_argument(
        "--kind",
        default="retry",
        choices=["retry", "success", "final_failure", "text"],
        help="消息类型",
    )
    parser.add_argument("--text", default="", help="kind=text 时发送的自定义内容")
    parser.add_argument("--live", action="store_true", help="确认真的发送（不加则只预览）")
    args = parser.parse_args()

    db.init_schema()
    notifier = FeishuNotifier()
    if not notifier.enabled:
        print("飞书未配置完整：请先在网页「系统设置 → 飞书通知」填写 app_id/app_secret/chat_id，"
              "或填写自定义机器人 Webhook。")
        return 2

    asin = services.normalize_asin(args.asin)
    record = services.latest_record_for(asin) if asin else None
    if not args.live:
        # 预览：本次进程内开启演练模式，只记录不发送（不改数据库设置）
        from app import settings_store as store

        store.set_runtime_override("notify_dry_run", True)
        notifier.cfg = store.snapshot()
        print("【演练模式】以下消息只会记录到日志，不会发到群里；确认发送请加 --live")
    if args.kind == "text":
        ok, details = notifier.notify_text(args.text or "测试消息", kind="test", asin=asin)
        for item in details:
            print(f"  {item['target']} → {'成功' if item['ok'] else '失败 ' + (item['error'] or '')}")
    elif args.kind == "success":
        ok = notifier.notify_success(asin=asin, owner=args.owner, record=record, force=True)
    elif args.kind == "final_failure":
        ok = notifier.notify_final_failure(
            asin=asin, owner=args.owner, record=record, attempt=5, max_attempts=5, force=True
        )
    else:
        ok = notifier.notify_retry(asin=asin, owner=args.owner, record=record, force=True)
    print("发送成功" if ok else "发送失败")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
