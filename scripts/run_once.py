"""命令行手动取数 / 手动判定跟进（可挂计划任务，也可 docker exec 调用）

⚠️ 注意：取数时若发现新的失败，会按规则发送飞书重试消息。
   只想同步数据、不想发消息时加 --dry-run（演练模式，只记录不发送）。

用法：
    python scripts/run_once.py            # 取一次数 + 判定跟进
    python scripts/run_once.py --tick     # 只判定跟进（不重新取数）
    python scripts/run_once.py --dry-run  # 演练模式：不发飞书消息
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import db, services  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="图片上传监控系统：手动取数")
    parser.add_argument("--tick", action="store_true", help="只判定跟进会话，不重新取数")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    parser.add_argument("--dry-run", action="store_true", help="演练模式：飞书消息只记录不发送")
    args = parser.parse_args()

    db.init_schema()
    if args.dry_run:
        from app import settings_store as store

        # 只影响本次进程，不改数据库设置
        store.set_runtime_override("notify_dry_run", True)
        print("【演练模式】本次进程内「只记录不发送」，不会往群里发消息")
    if args.tick:
        result = {"tick": services.evaluate_sessions(actor="cli")}
    else:
        result = services.poll_once("cli", force=True)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
