"""排查工具：打印当前库内关键信息（配置自检 / 状态分布 / 跟进会话 / 异常记录）

用法：python scripts/diagnose.py        （容器内：docker exec image-upload-monitor python scripts/diagnose.py）
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from app import db, services  # noqa: E402
from app import settings_store as store  # noqa: E402


def main() -> int:
    db.init_schema()
    cfg = store.snapshot()
    print("== 配置 ==")
    print(f"  领星 token：{'已配置' if store.lingxing_ready(cfg) else '未配置'}"
          f"｜代理：{cfg.get('lingxing_proxy') or '直连'}")
    print(f"  飞书：{'已配置' if store.feishu_ready(cfg) else '未配置'}"
          f"｜群：{cfg.get('feishu_chat_id') or '-'}"
          f"｜二号虾掌柜 open_id：{'已配置' if cfg.get('feishu_bot_open_id') else '未配置'}")
    print(f"  间隔：常规 {cfg.get('poll_interval_minutes')} 分钟 / 处理中 {cfg.get('processing_poll_interval_minutes')} 分钟"
          f"｜跟进 {cfg.get('follow_interval_minutes')} 分钟，单轮 {cfg.get('follow_round_minutes')} 分钟，最多 {cfg.get('follow_max_attempts')} 次")

    print("== 数据 ==")
    print(f"  取数时间：{db.get_meta('last_poll_at') or '-'}｜窗口：{db.get_meta('last_window_start')} ~ {db.get_meta('last_window_end')}")
    if db.get_meta("last_poll_error"):
        print(f"  最近取数错误：{db.get_meta('last_poll_error')}")
    for row in db.fetchall("SELECT status_text, COUNT(*) AS n FROM publish_records GROUP BY status_text"):
        print(f"  {services.status_text_cn(row['status_text'])}：{row['n']}")
    total = db.fetchone("SELECT COUNT(*) AS n, COUNT(DISTINCT parent_asin) AS a FROM publish_records") or {}
    print(f"  记录 {total.get('n')} 条 / ASIN {total.get('a')} 个")

    empty = db.fetchone("SELECT COUNT(*) AS n FROM publish_records WHERE parent_asin = ''") or {}
    if int(empty.get("n") or 0):
        print(f"  注意：{empty['n']} 条记录的 parent_asin 为空，示例：")
        for row in db.fetchall(
            "SELECT queue_unique_id, child_asin, msku, status_name, store_name, raw_json "
            "FROM publish_records WHERE parent_asin = '' ORDER BY queue_unique_id DESC LIMIT 2"
        ):
            print(f"    #{row['queue_unique_id']} asin={row['child_asin'] or '-'} msku={row['msku'] or '-'} "
                  f"{row['status_name']} {row['store_name']}")
            print(f"      原始字段：{row['raw_json'][:300]}")

    print("== 跟进会话 ==")
    sessions = db.fetchall(
        "SELECT * FROM follow_sessions ORDER BY CASE state WHEN 'active' THEN 0 ELSE 1 END, id DESC LIMIT 10"
    )
    for s in sessions:
        print(f"  #{s['id']} {s['parent_asin']} {s['state']}/{s['source']} 重试 {s['attempts']}/{s['max_attempts']}"
              f" 通知={'开' if s['notify_on_failure'] else '关'} 下次={s['next_tick_at'] or '-'} {s['close_reason'] or ''}")

    print("== 最近事件 ==")
    for e in db.fetchall("SELECT * FROM follow_events ORDER BY id DESC LIMIT 8"):
        print(f"  {e['created_at']} {e['parent_asin']} {e['event']}：{e['detail'][:80]}")

    print("== 最近飞书消息 ==")
    for m in db.fetchall("SELECT * FROM feishu_messages ORDER BY id DESC LIMIT 5"):
        print(f"  {m['created_at']} [{m['kind']}] {m['parent_asin']} ok={m['ok']} {m['error'][:40]}")
        print(f"    {m['content'][:160]}")

    print("== 参数快照（可改项）==")
    print(json.dumps({k: v for k, v in cfg.items() if not k.endswith("__source") and "token" not in k
                      and "secret" not in k}, ensure_ascii=False, indent=2)[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
