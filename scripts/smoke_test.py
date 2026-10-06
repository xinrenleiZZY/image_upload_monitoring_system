"""本地端到端自测（不依赖外网、不污染正式数据）

做法：
1. 用标准库起一个「假领星接口 + 假飞书 Webhook」服务
2. 用独立的临时 SQLite 库启动真实服务进程（禁用后台调度，保证结果可预期）
3. 依次验证：取数落库 → 状态分析 → 自动跟进 → 失败重试 → 成功通知 →
   5 次上限 → 30 分钟窗口重发 → 第三方推送跟进请求 → 开放 API / 页面接口

运行：python scripts/smoke_test.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Windows 控制台默认 GBK，强制 UTF-8 输出，避免中文/符号报错
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

FAKE_PORT = 7798
APP_PORT = 7796
TMP_DIR = Path(tempfile.mkdtemp(prefix="image-monitor-smoke-"))
os.environ["MONITOR_DB_PATH"] = str(TMP_DIR / "monitor.db")
os.environ["APP_PORT"] = str(APP_PORT)
os.environ["APP_HOST"] = "127.0.0.1"
os.environ["SCHEDULER_ENABLED"] = "false"
os.environ["POLL_ON_STARTUP"] = "false"
os.environ["LOG_LEVEL"] = "WARNING"

CST = timezone(timedelta(hours=8))
STATE = {"records": [], "feishu": [], "api_calls": 0, "fail_count": 0,
         "proxy_calls": [], "proxy_fail": False, "direct_fail_count": 0,
         "login_calls": 0, "valid_token": "smoke-token"}
CHECKS: list = []


# ───────────── 假接口服务 ─────────────
class FakeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json;charset=UTF-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        if self.path.startswith("/listing-publish-api"):
            if STATE["fail_count"] > 0:
                STATE["fail_count"] -= 1
                self._send(500, {"code": -1, "msg": "服务异常"})
                return
            try:
                payload = json.loads(raw.decode("utf-8"))
            except ValueError:
                payload = {}
            offset = int(payload.get("offset") or 0)
            size = int(payload.get("length") or 100)
            records = STATE["records"]
            STATE["api_calls"] += 1
            self._send(
                200,
                {
                    "code": 1,
                    "msg": "成功",
                    "data": {"list": records[offset : offset + size], "count": len(records)},
                },
            )
            return
        if self.path.startswith("/feishu-webhook"):
            STATE["feishu"].append(json.loads(raw.decode("utf-8") or "{}"))
            self._send(200, {"code": 0, "msg": "success"})
            return
        if self.path.startswith("/api/user/login"):
            # 模拟自建站「账号密码登录」：登录成功即签发新的 user_token
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except ValueError:
                payload = {}
            STATE["login_calls"] += 1
            if payload.get("password") != "smoke-pass":
                self._send(200, {"success": False, "message": "账号或密码不正确"})
                return
            STATE["valid_token"] = "smoke-token-2"
            self._send(200, {"success": True, "user_token": STATE["valid_token"]})
            return
        if self.path.startswith("/api/proxy"):
            # 模拟自建领星 API 站：归一化返回 {success,total,data}
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except ValueError:
                payload = {}
            STATE["proxy_calls"].append(payload)
            if self.headers.get("X-User-Token") != STATE["valid_token"]:
                # 站点用户令牌失效（区别于领星 Token 过期，后者不触发重登）
                self._send(200, {"success": False, "error": "unauthorized",
                                 "message": "用户Token无效或已过期，请重新登录"})
                return
            if STATE["proxy_fail"]:
                self._send(200, {"success": False, "message": "模拟兜底接口失败"})
                return
            body = payload.get("body") or {}
            offset = int(body.get("offset") or 0)
            size = int(body.get("length") or 20)
            records = STATE["records"]
            items = records if payload.get("auto_pagination") else records[offset : offset + size]
            self._send(200, {"success": True, "total": len(records), "data": items})
            return
        self._send(404, {"code": -1, "msg": "not found"})

    def log_message(self, *args):  # 静默
        return


def now_text(offset_minutes: int = 0) -> str:
    return (datetime.now(CST) + timedelta(minutes=offset_minutes)).strftime("%Y-%m-%d %H:%M:%S")


def make_record(asin: str, status: int, queue_id: int, *, finish: str = "", reason: str = "",
                owner: str = "运营-测试", store: str = "Geefuun-US") -> dict:
    status_name = {3: "提交成功", 4: "失败"}.get(status, "处理中")
    return {
        "queue_unique_id": str(queue_id),
        "parent_asin": asin,
        "asin": asin,
        "msku": "msku-" + asin.lower(),
        "fnsku": "X00" + asin[-4:],
        "item_name": f"测试产品 {asin}",
        "marketplace_id": "ATVPDKIKX0DER",
        "queue_type": 4,
        "queue_type_name": "编辑商品",
        "status": status,
        "status_name": status_name,
        "failure_reason": reason,
        "operate_time": finish or now_text(),
        "finish_time": finish or now_text(),
        "user_name": owner,
        "store_name": store,
        "country_name": "美国",
        "asin_url": f"https://www.amazon.com/dp/{asin}",
    }


def set_records(records: list) -> None:
    STATE["records"] = records


def add_record(record: dict) -> None:
    STATE["records"].append(record)


# ───────────── HTTP 小工具 ─────────────
def http(method: str, path: str, body=None, headers=None, timeout: int = 30, form: bool = False):
    url = f"http://127.0.0.1:{APP_PORT}{path}"
    data = None
    hdrs = {"Content-Type": "application/json;charset=UTF-8"}
    if body is not None and form:
        data = urllib.parse.urlencode(body).encode("utf-8")
        hdrs = {"Content-Type": "application/x-www-form-urlencoded"}
    elif body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw.strip().startswith(("{", "[")) else raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, raw


def check(name: str, condition: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(condition), detail))
    flag = "[OK]  " if condition else "[FAIL]"
    print(f"{flag} {name}" + (f"｜{detail}" if detail else ""))


# ───────────── 主流程 ─────────────
def main() -> int:
    fake = ThreadingHTTPServer(("127.0.0.1", FAKE_PORT), FakeHandler)
    threading.Thread(target=fake.serve_forever, daemon=True).start()

    from app import db, services
    from app import settings_store as store
    import uvicorn
    import main as app_main

    db.init_schema()
    # 测试期指向假接口；代理必须显式置空（避免走 .env 里的宿主代理）
    store.update(
        {
            "lingxing_base_url": f"http://127.0.0.1:{FAKE_PORT}",
            "lingxing_path": "/listing-publish-api/api/AmazonPublishQueue/list",
            "lingxing_auth_token": "test-token",
            "lingxing_proxy": "",
            "feishu_webhook_url": f"http://127.0.0.1:{FAKE_PORT}/feishu-webhook",
            "feishu_webhook_secret": "",
            "feishu_app_id": "",
            "feishu_app_secret": "",
            "feishu_chat_id": "",
            "feishu_bot_open_id": "",
            # 兜底数据源指向测试用的假 API 站
            "fallback_enabled": True,
            "fallback_base_url": f"http://127.0.0.1:{FAKE_PORT}",
            "fallback_path": "/api/proxy",
            "fallback_user_id": "smoke-user",
            "fallback_user_token": "smoke-token",
            "fallback_user_password": "smoke-pass",
            "fallback_auto_login": True,
            # 场景 1-8 保持「领星直连为主」的既有行为；主备翻转在后续场景单独验证
            "fallback_as_primary": False,
            "fallback_account": "xiazhanggui",
            "follow_round_minutes": 30,
            "follow_interval_minutes": 10,
            # 自测要即时判定，延迟检查单独在「场景 16」验证
            "follow_request_delay_minutes": 0,
            "follow_max_attempts": 5,
            "message_dedupe_seconds": 0,
            "query_page_size": 50,
        },
        wipe_empty=True,
    )

    config = uvicorn.Config(app_main.app, host="127.0.0.1", port=APP_PORT, log_level="error")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(60):
        try:
            status, _body = http("GET", "/healthz", timeout=2)
            if status == 200:
                break
        except Exception:
            time.sleep(0.3)
    else:
        check("服务启动", False, "健康检查超时")
        return 1
    check("服务启动", True, f"http://127.0.0.1:{APP_PORT}")

    # 自测里把「取数 + 判定」统一按「检查时间到点」驱动：
    # 真实环境由调度线程按 next_tick_at 到点触发，脚本里手动模拟到点，保证断言稳定。
    def poll_all(trigger: str = "smoke"):
        return services.poll_once(trigger, force=True, force_all_sessions=True)

    # 不强制：严格按 next_tick_at 判断（用于验证「推送后延迟检查」）
    def noforce_poll(trigger: str = "smoke"):
        return services.poll_once(trigger, force=True)

    # ── 场景数据：1 成功 / 1 处理中 / 1 新失败 / 1 历史失败（9 天前）──
    asin_ok = "B0AAAAA111"
    asin_proc = "B0BBBBB222"
    asin_fail = "B0CCCCC333"
    asin_old = "B0DDDDD444"
    asin_fail_new = "B0CCCCC444"
    set_records(
        [
            make_record(asin_ok, 3, 1000000000000000001),
            make_record(asin_proc, 2, 1000000000000000002),
            make_record(asin_fail, 4, 1000000000000000003, reason='[{"code":"99022","message":"图片尺寸不符"}]'),
            make_record(asin_old, 4, 1000000000000000004, finish=now_text(-9 * 24 * 60),
                        reason="历史失败"),
        ]
    )

    # ── 1. 取数落库 + 状态分析 + 自动跟进 ──
    status, body = http("POST", "/api/ui/poll/run", timeout=60)
    data = (body or {}).get("data") or {}
    check("取数接口返回成功", status == 200 and (body or {}).get("code") == 0, str(body)[:160])
    check("全量落库 4 条", data.get("fetched") == 4, f"fetched={data.get('fetched')}")
    counts = db.fetchone("SELECT COUNT(*) AS n FROM publish_records") or {}
    check("快照表记录数=4", int(counts.get("n") or 0) == 4, f"n={counts.get('n')}")
    by_status = {
        row["status_text"]: int(row["n"])
        for row in db.fetchall(
            "SELECT status_text, COUNT(*) AS n FROM publish_records GROUP BY status_text"
        )
    }
    check(
        "状态分析正确（成功/失败/处理中）",
        by_status.get("success") == 1 and by_status.get("failed") == 2 and by_status.get("processing") == 1,
        json.dumps(by_status),
    )
    check(
        "处理中 ASIN 自动进入跟进",
        bool(
            db.fetchone(
                "SELECT 1 FROM follow_sessions WHERE parent_asin=? AND state='active'", asin_proc
            )
        ),
    )
    check(
        "历史失败(9天前)不发送通知（静默跟进/已收尾）",
        not any(asin_old in json.dumps(m) for m in STATE["feishu"]),
        f"飞书消息数={len(STATE['feishu'])}",
    )
    check(
        "首次取数不跟进历史失败（上线前已有失败）",
        not db.fetchone("SELECT 1 FROM follow_sessions WHERE parent_asin=?", asin_fail),
        "历史失败未建立会话",
    )

    # ── 2. 上线后新失败 → 发送重试消息 ──
    add_record(
        make_record(asin_fail_new, 4, 1000000000000000009, reason='[{"code":"99022","message":"图片尺寸不符"}]')
    )
    poll_all()
    retry_msgs = [m for m in STATE["feishu"] if "重试" in json.dumps(m, ensure_ascii=False)]
    check("失败立即发送重试消息", len(retry_msgs) == 1, f"n={len(retry_msgs)}")
    text = retry_msgs[0]["content"]["text"] if retry_msgs else ""
    check("重试消息内容符合要求", all(s in text for s in [
        "@二号虾掌柜", "读取文档", "亚马逊产品图片上传",
        "https://qcn07e5a5sag.feishu.cn/wiki/Giwlw0pLfifm9ekHfn7c4Ulenzh",
        f"重试{asin_fail_new} ASIN", "执行上传产品图片自动化任务",
    ]), text[:200])
    session = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_fail_new
    )
    check("该会话记录第 1 次重试", (session or {}).get("attempts") == 1, f"attempts={(session or {}).get('attempts')}")

    # ── 3. 处理中 → 成功：结束跟进 + 成功通知 ──
    set_records(
        [
            make_record(asin_ok, 3, 1000000000000000001),
            make_record(asin_proc, 3, 1000000000000000002),
            make_record(asin_fail, 4, 1000000000000000003, reason="图片尺寸不符"),
            make_record(asin_fail_new, 4, 1000000000000000009, reason="图片尺寸不符"),
            make_record(asin_old, 4, 1000000000000000004, finish=now_text(-9 * 24 * 60), reason="历史失败"),
        ]
    )
    poll_all()
    session = db.fetchone("SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_proc)
    check("成功后跟进结束", (session or {}).get("state") == "completed",
          f"state={(session or {}).get('state')} reason={(session or {}).get('close_reason')}")
    ok_msgs = [m for m in STATE["feishu"] if f"{asin_proc}成功上传产品图片" in json.dumps(m, ensure_ascii=False)]
    check("发送成功消息", len(ok_msgs) == 1, f"n={len(ok_msgs)}")

    # ── 4. 反复失败：重试 5 次后提示人工检查 ──
    for i in range(1, 6):
        add_record(make_record(asin_fail_new, 4, 1000000000000000009 + i * 100, reason=f"第{i}次重试仍失败"))
        poll_all()
    retry_msgs = [m for m in STATE["feishu"] if f"重试{asin_fail_new}" in json.dumps(m, ensure_ascii=False)]
    final_msgs = [m for m in STATE["feishu"] if f"{asin_fail_new}上传失败，人工检查图片" in json.dumps(m, ensure_ascii=False)]
    session = db.fetchone("SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_fail_new)
    check("重试消息最多 5 次", len(retry_msgs) == 5, f"n={len(retry_msgs)}")
    check("超过 5 次发送人工检查消息", len(final_msgs) == 1, f"n={len(final_msgs)}")
    check("会话以失败收尾", (session or {}).get("state") == "failed",
          f"reason={(session or {}).get('close_reason')}")

    # ── 5. 30 分钟窗口无新数据 → 重试消息失效 → 重发一次（计入 5 次）──
    asin_window = "B0EEEEE555"
    add_record(make_record(asin_window, 2, 1000000000000000100))
    status, body = http(
        "POST",
        "/api/v1/follow/request",
        {"asin": asin_window, "timestamp": now_text(), "owner": "张三"},
    )
    check("第三方推送跟进请求成功", status == 200 and (body or {}).get("code") == 0, str(body)[:160])
    poll_all()
    session = db.fetchone("SELECT * FROM follow_sessions WHERE parent_asin=? AND state='active'", asin_window)
    check("请求后立刻建立跟进会话", bool(session), f"session={(session or {}).get('id')}")
    before = len(STATE["feishu"])
    # 模拟 30 分钟到期且无新状态数据
    past = (datetime.now(CST) - timedelta(minutes=1)).isoformat(timespec="seconds")
    db.execute(
        "UPDATE follow_sessions SET next_tick_at=?, round_deadline_at=? WHERE id=?",
        past, past, (session or {}).get("id"),
    )
    services.evaluate_sessions(actor="smoke")
    session = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", (session or {}).get("id"))
    resent = [m for m in STATE["feishu"][before:] if f"重试{asin_window}" in json.dumps(m, ensure_ascii=False)]
    check("窗口超时判定重试消息失效并重发一次", len(resent) == 1 and (session or {}).get("attempts") == 1,
          f"resent={len(resent)} attempts={(session or {}).get('attempts')}")
    events = [e["event"] for e in services.session_events((session or {}).get("id"))]
    check("记录 retry_resent 事件", "retry_resent" in events, ",".join(events[:6]))

    # 该 ASIN 成功 → 结束跟进
    add_record(make_record(asin_window, 3, 1000000000000000101))
    poll_all()
    session = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", (session or {}).get("id"))
    check("窗口重发后成功后正常收尾", (session or {}).get("state") == "completed",
          f"state={(session or {}).get('state')}")

    # ── 6. 无任何记录的新 ASIN + 不同入参方式 ──
    asin_new = "B0FFFFF666"
    status, body = http(
        "GET",
        "/api/v1/follow/request?" + urllib.parse.urlencode(
            {"asin": asin_new, "timestamp": now_text(), "owner": "李四"}
        ),
    )
    check("GET 方式推送跟进请求", status == 200 and (body or {}).get("code") == 0, str(body)[:120])
    add_record(make_record(asin_new, 3, 1000000000000000200))
    poll_all()
    session = db.fetchone("SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_new)
    check("无记录 ASIN 后续成功可收尾", (session or {}).get("state") == "completed",
          f"state={(session or {}).get('state')}")
    msgs = [m for m in STATE["feishu"] if f"{asin_new}成功上传产品图片" in json.dumps(m, ensure_ascii=False)]
    check("无记录 ASIN 成功也发送通知", len(msgs) == 1, f"n={len(msgs)}")

    # 表单方式（第三方脚本可能用 requests.post(data=...)）
    status, form_body = http(
        "POST", "/api/v1/follow/request", {"asin": asin_new, "owner": "表单"}, form=True
    )
    check("表单方式推送跟进请求", status == 200 and (form_body or {}).get("code") == 0,
          str(form_body)[:120])

    # 非法 ASIN
    status, body = http("POST", "/api/v1/follow/request", {"asin": "abc"})
    check("非法 ASIN 返回 400", status == 400, f"status={status}")

    # ── 7. 查询接口 / 页面 ──
    status, body = http("GET", f"/api/v1/asin/status?asins={asin_fail},{asin_ok}")
    items = ((body or {}).get("data") or {}).get("items") or []
    check(
        "ASIN 状态查询",
        status == 200 and len(items) == 2 and items[0]["status_text"] in ("success", "failed"),
        json.dumps(items[:1], ensure_ascii=False)[:160],
    )
    status, body = http("GET", "/api/v1/follow/sessions?state=active")
    check("开放接口跟进列表", status == 200 and isinstance((body or {}).get("data"), dict))
    status, body = http("GET", "/healthz")
    check("健康检查", status == 200 and (body or {}).get("status") == "ok", str(body)[:120])
    status, html = http("GET", "/")
    check("前端页面可访问", status == 200 and isinstance(html, str) and "图片上传监控系统" in html)
    status, body = http("GET", "/api/ui/settings")
    groups = ((body or {}).get("data") or {}).get("groups") or []
    check("设置接口返回分组参数", status == 200 and len(groups) >= 6, f"groups={len(groups)}")
    status, body = http("GET", "/api/ui/records?page=1&page_size=10")
    check("上传记录接口", status == 200 and ((body or {}).get("data") or {}).get("total", 0) >= 5,
          f"total={((body or {}).get('data') or {}).get('total')}")
    today = datetime.now(CST).strftime("%Y-%m-%d")
    qs = urllib.parse.urlencode(
        {"changed_from": f"{today} 00:00:00", "changed_to": f"{today} 23:59:59", "page_size": 200}
    )
    status, body = http("GET", "/api/ui/records?" + qs)
    check("同一天时间范围筛选包含当天记录",
          status == 200 and ((body or {}).get("data") or {}).get("total", 0) >= 5,
          f"total={((body or {}).get('data') or {}).get('total')}")
    status, body = http("GET", "/api/ui/poll-runs?page=1&page_size=10")
    check("轮询日志接口", status == 200 and ((body or {}).get("data") or {}).get("total", 0) >= 5)
    status, body = http("GET", "/api/ui/feishu/messages?page=1&page_size=20")
    check("飞书消息日志接口", status == 200 and ((body or {}).get("data") or {}).get("total", 0) >= 6,
          f"total={((body or {}).get('data') or {}).get('total')}")
    status, body = http("GET", "/api/ui/summary")
    cfg = ((body or {}).get("data") or {}).get("config") or {}
    check("概览接口显示配置自检", status == 200 and cfg.get("lingxing_ready") is True and cfg.get("feishu_ready") is True,
          json.dumps(cfg, ensure_ascii=False)[:160])

    # ── 8. 异常与幂等 ──
    # 先关掉兜底，验证「直连失败且无兜底」时的行为
    store.update({"fallback_enabled": False})
    # 连续 5 次失败（覆盖「重试 2 次」的客户端重试策略），确保真正走到失败分支
    STATE["fail_count"] = 5
    status, body = http("POST", "/api/ui/poll/run", timeout=60)
    check("接口异常时返回失败且不中断", status in (200, 500) and (body or {}).get("code") == 1, str(body)[:120])
    run = db.fetchone("SELECT * FROM poll_runs WHERE status='error' ORDER BY id DESC LIMIT 1")
    check("异常写入轮询日志", bool(run), (run or {}).get("error", "")[:80])
    err_msgs = [m for m in STATE["feishu"] if "取数失败" in json.dumps(m, ensure_ascii=False)]
    check("取数失败发送告警", len(err_msgs) >= 1, f"n={len(err_msgs)}")
    # 失败告警只发指定的群；失败后不再空转刷接口
    err_row = db.fetchone(
        "SELECT * FROM feishu_messages WHERE kind='poll_error' ORDER BY id DESC LIMIT 1"
    )
    check(
        "取数失败告警只发指定群",
        (err_row or {}).get("targets") == "oc_f57489fe7119d84bcb45567432bb8ae7",
        f"targets={(err_row or {}).get('targets')}",
    )
    check(
        "取数失败不额外重试（按正常节奏等下一次）",
        bool(db.get_meta("last_poll_attempt_at")) and services.poll_due() is False
        and services.poll_interval_seconds() >= 10 * 60,
        f"attempt={db.get_meta('last_poll_attempt_at')} due={services.poll_due()} "
        f"interval={services.poll_interval_seconds()}",
    )
    # 更新 token 后应立刻可以重新取数（不用干等一个整点）
    status, body = http(
        "POST", "/api/ui/settings", {"values": {"lingxing_auth_token": "new-token"}}
    )
    check(
        "保存新 token 后立刻恢复取数",
        status == 200 and services.poll_due() is True
        and "重新取数" in str((body or {}).get("message")),
        f"force={db.get_meta('poll_force_next')} due={services.poll_due()}",
    )
    # 恢复：清掉失败态，让后续场景照常取数
    db.set_meta("last_poll_error", "")
    db.set_meta("poll_force_next", "")

    # ── 8b. 失败告警每日上限：一天最多 3 次 ──
    store.update({"notify_poll_error": True, "poll_error_notify_max_per_day": 3})
    today = services.now_dt().strftime("%Y-%m-%d")
    db.set_meta("poll_error_notify_day", today)
    db.set_meta("poll_error_notify_count", "3")
    db.set_meta("last_poll_error_notify_at", "")
    before = len(STATE["feishu"])
    services._notify_poll_error("每日上限测试")
    check(
        "失败告警达到每日上限后不再发送",
        len(STATE["feishu"]) == before,
        f"新增={len(STATE['feishu']) - before}",
    )
    db.set_meta("poll_error_notify_count", "2")
    db.set_meta("last_poll_error_notify_at", "")
    services._notify_poll_error("每日上限测试")
    check(
        "未达每日上限时正常发送",
        len(STATE["feishu"]) == before + 1,
        f"新增={len(STATE['feishu']) - before}",
    )
    db.set_meta("poll_error_notify_day", "")
    db.set_meta("poll_error_notify_count", "0")
    db.set_meta("last_poll_error_notify_at", "")

    # ── 9. 兜底数据源：直连失败 → 自动改用自建 API 站 ──
    store.update({"fallback_enabled": True})
    STATE["proxy_calls"].clear()
    STATE["fail_count"] = 5  # 直连持续失败
    set_records(
        [
            make_record(asin_ok, 3, 1000000000000000001),
            make_record("B0FB000001", 4, 1000000000000002001, reason="兜底测试失败"),
        ]
    )
    result = poll_all("fallback_test")
    proxy_body = STATE["proxy_calls"][-1] if STATE["proxy_calls"] else {}
    inner = (proxy_body.get("body") or {})
    check(
        "直连失败自动走兜底接口取数",
        result.get("ok") is True and result.get("fetched") == 2
        and db.get_meta("last_data_source") == "fallback",
        f"ok={result.get('ok')} fetched={result.get('fetched')} 来源={db.get_meta('last_data_source')}",
    )
    check(
        "兜底请求参数正确（api_name/account/auto_pagination/时间区间）",
        proxy_body.get("api_name") == "Amazon刊登队列列表（AmazonPublishQueue/list）"
        and proxy_body.get("account") == "xiazhanggui"
        and proxy_body.get("auto_pagination") is True
        and bool(inner.get("start_time")) and bool(inner.get("end_time"))
        and "country_id" in inner,
        json.dumps(proxy_body, ensure_ascii=False)[:220],
    )
    fallback_msgs = [
        m for m in STATE["feishu"] if "已自动改用备用数据源" in json.dumps(m, ensure_ascii=False)
    ]
    check("走兜底时默认不发提示", len(fallback_msgs) == 0, f"n={len(fallback_msgs)}")
    # 打开开关才会提示，且只提示一次
    store.update({"notify_fallback": True})
    db.set_meta("last_fallback_notify_at", "")
    STATE["fail_count"] = 5
    poll_all("fallback_notify")
    fallback_msgs = [
        m for m in STATE["feishu"] if "已自动改用备用数据源" in json.dumps(m, ensure_ascii=False)
    ]
    check("打开开关后走兜底提示一次", len(fallback_msgs) == 1, f"n={len(fallback_msgs)}")
    store.update({"notify_fallback": False})
    STATE["fail_count"] = 0
    check(
        "直连失败原因被记录（便于更新 token）",
        "auth-token" in (db.get_meta("last_primary_error") or "")
        or "HTTP 500" in (db.get_meta("last_primary_error") or ""),
        (db.get_meta("last_primary_error") or "")[:80],
    )
    # 直连恢复后不再走兜底
    STATE["fail_count"] = 0
    STATE["proxy_calls"].clear()
    result = poll_all("direct_test")
    check(
        "直连恢复后回到直连取数",
        result.get("ok") is True and db.get_meta("last_data_source") == "lingxing"
        and not STATE["proxy_calls"],
        f"来源={db.get_meta('last_data_source')} 兜底调用={len(STATE['proxy_calls'])}",
    )
    # 兜底也失败时如实报错
    store.update({"fallback_enabled": True})
    STATE["fail_count"] = 5
    STATE["proxy_fail"] = True
    result = poll_all("fallback_fail")
    run_err = db.fetchone("SELECT * FROM poll_runs ORDER BY id DESC LIMIT 1") or {}
    check(
        "直连与兜底都失败时如实报错",
        result.get("ok") is False and "自建 API 站也失败" in (run_err.get("error") or ""),
        (run_err.get("error") or "")[:120],
    )
    STATE["fail_count"] = 0
    STATE["proxy_fail"] = False
    db.set_meta("last_poll_error", "")
    db.set_meta("poll_force_next", "")

    # ── 9b. 主备切换：自建 API 站为主数据源、领星直连为备 ──
    store.update({"fallback_enabled": True, "fallback_as_primary": True})
    STATE["fail_count"] = 0
    STATE["proxy_fail"] = False
    STATE["proxy_calls"].clear()
    STATE["api_calls"] = 0
    set_records(
        [
            make_record(asin_ok, 3, 1000000000000000001),
            make_record("B0PRIMARY01", 4, 1000000000000003001, reason="主源测试失败"),
        ]
    )
    result = poll_all("proxy_primary")
    check(
        "自建站为主时优先走自建站取数（不碰领星直连）",
        result.get("ok") is True and result.get("fetched") == 2
        and db.get_meta("last_data_source") == "fallback"
        and len(STATE["proxy_calls"]) == 1 and STATE["api_calls"] == 0,
        f"ok={result.get('ok')} 来源={db.get_meta('last_data_source')} "
        f"自建站调用={len(STATE['proxy_calls'])} 直连调用={STATE['api_calls']}",
    )
    # 自建站失败 → 自动回退领星直连
    STATE["proxy_fail"] = True
    STATE["api_calls"] = 0
    result = poll_all("proxy_primary_fail")
    check(
        "自建站失败时回退领星直连",
        result.get("ok") is True and db.get_meta("last_data_source") == "lingxing"
        and STATE["api_calls"] >= 1,
        f"ok={result.get('ok')} 来源={db.get_meta('last_data_source')} 直连调用={STATE['api_calls']}",
    )
    STATE["proxy_fail"] = False
    db.set_meta("last_poll_error", "")
    db.set_meta("poll_force_next", "")

    # ── 9c. 自建站站点用户令牌失效 → 自动重登并重试一次 ──
    store.update({
        "fallback_as_primary": True,
        "fallback_auto_login": True,
        "fallback_user_id": "smoke-user",
        "fallback_user_password": "smoke-pass",
    })
    STATE["valid_token"] = "expired-token"   # 让自建站认为我们保存的令牌已失效
    STATE["login_calls"] = 0
    STATE["proxy_calls"].clear()
    set_records([make_record(asin_ok, 3, 1000000000000000001)])
    result = poll_all("auto_login")
    check(
        "自建站令牌失效自动重登并重试成功",
        result.get("ok") is True and STATE["login_calls"] == 1
        and result.get("fetched") == 1 and db.get_meta("last_data_source") == "fallback",
        f"ok={result.get('ok')} 重登次数={STATE['login_calls']} "
        f"来源={db.get_meta('last_data_source')}",
    )
    check(
        "重登后新的 X-User-Token 已写回设置",
        store.snapshot().get("fallback_user_token") == "smoke-token-2"
        and STATE["valid_token"] == "smoke-token-2",
        f"token={store.snapshot().get('fallback_user_token')}",
    )
    # 恢复「领星直连为主」，后续场景沿用原有断言
    store.update({"fallback_as_primary": False})
    db.set_meta("last_poll_error", "")
    db.set_meta("poll_force_next", "")

    status, body = http("POST", "/api/ui/sessions/request", {"asins": [asin_ok], "owner": "张三"})
    first = ((body or {}).get("data") or {}).get("items") or []
    status, body = http("POST", "/api/ui/sessions/request", {"asins": [asin_ok], "owner": "张三"})
    second = ((body or {}).get("data") or {}).get("items") or []
    check(
        "重复跟进请求复用同一会话（幂等）",
        bool(first) and second and first[0]["session_id"] == second[0]["session_id"],
        f"{first[:1]} / {second[:1]}",
    )

    # ── 9. 状态机细节：不重复发送、会话超时保护 ──
    asin_dup = "B0GGGGG777"
    add_record(make_record(asin_dup, 4, 1000000000000000300, reason="测试重复发送"))
    poll_all()
    session = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_dup
    )
    before = len(STATE["feishu"])
    poll_all()   # 同一失败状态，再取一次数
    services.evaluate_sessions(actor="smoke")
    after = len(STATE["feishu"])
    check(
        "同一失败状态不重复发送重试消息",
        after == before and (session or {}).get("attempts") == 1,
        f"新增消息={after - before} attempts={(session or {}).get('attempts')}",
    )
    # 会话超时保护：把 open 时间改到 8 小时前（默认 6 小时上限）
    old_opened = (datetime.now(CST) - timedelta(hours=8)).isoformat(timespec="seconds")
    db.execute("UPDATE follow_sessions SET opened_at=? WHERE id=?", old_opened, (session or {}).get("id"))
    poll_all()
    session = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", (session or {}).get("id"))
    check(
        "长时间未结束的会话自动收尾",
        (session or {}).get("state") == "failed" and "跟进超过" in ((session or {}).get("close_reason") or ""),
        f"state={(session or {}).get('state')} reason={(session or {}).get('close_reason')}",
    )
    closed_msgs = [m for m in STATE["feishu"] if f"{asin_dup}上传失败，人工检查图片" in json.dumps(m, ensure_ascii=False)]
    check("超时收尾同时提示人工检查", len(closed_msgs) == 1, f"n={len(closed_msgs)}")

    # ── 10. 自动跟进冷却：结束后短时间内不再自动重建 ──
    poll_all()
    active = db.fetchone(
        "SELECT 1 FROM follow_sessions WHERE parent_asin=? AND state='active'", asin_dup
    )
    check("自动跟进冷却期内不重建会话", not active)

    # ── 11. 失败自动跟进起始时间：过去失败不跟进 ──
    asin_cut = "B0HHHHH888"
    future = (datetime.now(CST) + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    past = (datetime.now(CST) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    store.update({"follow_start_at": future})
    add_record(make_record(asin_cut, 4, 1000000000000000600, reason="起始时间测试-过去失败"))
    poll_all()
    check(
        "早于起始时间的失败不自动跟进",
        not db.fetchone("SELECT 1 FROM follow_sessions WHERE parent_asin=?", asin_cut),
    )
    # 起始时间回退到过去 → 新的失败变化可以正常跟进
    store.update({"follow_start_at": past})
    add_record(make_record(asin_cut, 4, 1000000000000000601, reason="起始时间测试-恢复跟进"))
    poll_all()
    session = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_cut
    )
    check(
        "起始时间调整后新失败可以跟进",
        bool(session) and (session or {}).get("state") == "active",
        f"state={(session or {}).get('state')}",
    )
    # 把起始时间调回「未来」→ 进行中且未发过重试消息的历史失败会话应自动结束
    store.update({"feishu_webhook_url": f"http://127.0.0.1:{FAKE_PORT}/feishu-disabled"})
    add_record(make_record("B0IIIII999", 4, 1000000000000000700, reason="清理测试"))
    poll_all()
    target = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? AND state='active'", "B0IIIII999"
    )
    store.update({"follow_start_at": future})
    poll_all()
    target_after = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", (target or {}).get("id"))
    check(
        "调整起始时间后，未发过重试消息的历史失败会话自动结束",
        bool(target) and (target_after or {}).get("state") == "cancelled",
        f"before={(target or {}).get('state')} after={(target_after or {}).get('state')} "
        f"reason={(target_after or {}).get('close_reason')}",
    )
    store.update({"follow_start_at": ""}, wipe_empty=True)

    # ── 12. 批量请求跟进：一次多个 ASIN，按 ASIN 逐个建会话 ──
    batch = ["B0BATCH0001", "B0BATCH0002", "B0BATCH0003"]
    set_records(
        [
            make_record(batch[0], 3, 1000000000000000801),
            make_record(batch[1], 2, 1000000000000000802),
            make_record(batch[2], 4, 1000000000000000803, reason="批量测试失败"),
        ]
    )
    status, body = http(
        "POST",
        "/api/v1/follow/request",
        {"asins": batch, "timestamp": now_text(), "owner": "批量负责人"},
    )
    data = (body or {}).get("data") or {}
    # 生产环境里调度线程会在收到请求后立刻取数判定，这里手动触发同样动作
    poll_all()
    sessions = {
        row["parent_asin"]: row
        for row in db.fetchall(
            "SELECT * FROM follow_sessions WHERE parent_asin LIKE 'B0BATCH%'"
        )
    }
    check(
        "一次传 3 个 ASIN → 逐个建立跟进会话",
        status == 200 and data.get("accepted") == 3 and len(sessions) == 3,
        f"accepted={data.get('accepted')} created={data.get('created')} 会话数={len(sessions)}",
    )
    check(
        "每个 ASIN 独立跟进（负责人一致、状态各自记录）",
        all(s.get("owner") == "批量负责人" for s in sessions.values())
        and sessions.get(batch[1], {}).get("last_status_text") == "processing"
        and sessions.get(batch[2], {}).get("last_status_text") == "failed",
        json.dumps({k: (v.get("last_status_text"), v.get("owner")) for k, v in sessions.items()},
                   ensure_ascii=False),
    )
    # 逗号分隔字符串写法 + 部分非法
    status, body = http(
        "POST",
        "/api/v1/follow/request",
        {"asin": f"{batch[0]},{batch[1]},bad", "owner": "逗号写法"},
    )
    data = (body or {}).get("data") or {}
    items = {item["asin"]: item for item in (data.get("items") or [])}
    check(
        "逗号分隔多个 ASIN 且非法项只影响自己",
        status == 200 and data.get("accepted") == 2
        and items.get("BAD", {}).get("ok") is False
        and items.get(batch[0], {}).get("ok") is True,
        f"accepted={data.get('accepted')} reused={data.get('reused')} bad_ok={items.get('BAD', {}).get('ok')}",
    )
    # 直接传数组 body
    status, body = http("POST", "/api/v1/follow/request", batch)
    data = (body or {}).get("data") or {}
    check("直接传 JSON 数组 body", status == 200 and data.get("accepted") == 3,
          f"accepted={data.get('accepted')}")
    # 批量查状态
    status, body = http("GET", "/api/v1/asin/status?asins=" + ",".join(batch))
    check("批量查状态接口",
          status == 200 and len(((body or {}).get("data") or {}).get("items") or []) == 3)
    # 批量取消
    status, body = http("POST", "/api/v1/follow/cancel", {"asins": batch})
    active = db.fetchall(
        "SELECT 1 FROM follow_sessions WHERE parent_asin LIKE 'B0BATCH%' AND state='active'"
    )
    check("批量取消跟进", not active, f"剩余进行中={len(active)}")

    # ── 13. 通知失败与补发：默认不补发，手动开启后补发一次且不重复 ──
    # 恢复正常的飞书通道（场景 11 里故意改坏过）
    store.update({"feishu_webhook_url": f"http://127.0.0.1:{FAKE_PORT}/feishu-webhook"})
    bad_id = db.insert(
        "INSERT INTO feishu_messages (kind, parent_asin, targets, content, ok, error, created_at) "
        "VALUES ('success', 'B0RESEND001', 'webhook', 'B0RESEND001成功上传产品图片', 0, '模拟网络失败', ?)",
        datetime.now(CST).replace(microsecond=0).isoformat(),
    )
    before = len(STATE["feishu"])
    out = services.retry_failed_notifications(min_gap_minutes=0)
    check(
        "默认不自动补发（避免未经允许往群里发消息）",
        out.get("checked") == 0 and len(STATE["feishu"]) == before,
        json.dumps(out, ensure_ascii=False),
    )
    store.update({"auto_resend_failed_notify": True})
    out = services.retry_failed_notifications(min_gap_minutes=0)
    delivered = [
        m for m in STATE["feishu"] if "B0RESEND001成功上传产品图片" in json.dumps(m, ensure_ascii=False)
    ]
    row = db.fetchone("SELECT retried_at FROM feishu_messages WHERE id=?", bad_id)
    out2 = services.retry_failed_notifications(min_gap_minutes=0)
    delivered2 = [
        m for m in STATE["feishu"] if "B0RESEND001成功上传产品图片" in json.dumps(m, ensure_ascii=False)
    ]
    check(
        "开启后补发一次且不会重复补发",
        len(delivered) == 1 and len(delivered2) == 1 and bool((row or {}).get("retried_at")),
        f"补发={len(delivered)} 二次调用后={len(delivered2)} retried_at={bool((row or {}).get('retried_at'))}",
    )
    store.update({"auto_resend_failed_notify": False})

    # ── 14. 演练模式：只记录不发送 ──
    store.update({"notify_dry_run": True})
    before = len(STATE["feishu"])
    status, body = http("POST", "/api/ui/feishu/test", {"text": "演练模式测试消息"})
    after = len(STATE["feishu"])
    check(
        "演练模式不会真正发送到飞书",
        after == before and (body or {}).get("code") == 1,
        f"新增实际发送={after - before} code={(body or {}).get('code')}",
    )
    store.update({"notify_dry_run": False})

    # ── 15. 会话结束后出现「新的上传结果」→ 必须重新跟进并通知 ──
    asin_rearm = "B0REARM0001"
    add_record(make_record(asin_rearm, 4, 1000000000000000900, reason="第一次失败"))
    poll_all()
    first = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_rearm
    )
    services.cancel_session(asin_rearm, actor="test")   # 模拟人工结束
    before_msgs = len(STATE["feishu"])
    add_record(make_record(asin_rearm, 4, 1000000000000000901, reason="重试后再次失败"))
    poll_all()
    second = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? ORDER BY id DESC LIMIT 1", asin_rearm
    )
    new_msgs = len(STATE["feishu"]) - before_msgs
    check(
        "会话结束后出现新结果会重新跟进并通知",
        bool(first) and bool(second) and second["id"] != first["id"]
        and second["state"] == "active" and new_msgs >= 1,
        f"新会话={second['id'] if second else None} 状态={(second or {}).get('state')} 新消息={new_msgs}",
    )

    # ── 16. 推送后延迟检查：等 follow_request_delay_minutes 分钟再取数判定 ──
    store.update({"follow_request_delay_minutes": 2})
    asin_delay = "B0DELAY0001"
    # 直接落库（不走 auto_open_sessions），确保只有「推送」这一个来源的会话
    raw = make_record(asin_delay, 4, 1000000000000001000, reason="延迟检查测试")
    services.save_records([services.lingxing.normalize_record(raw, success_value=3, failed_value=4)])

    def msgs_for(target: str):
        return [m for m in STATE["feishu"] if target in json.dumps(m, ensure_ascii=False)]

    before = len(STATE["feishu"])
    status, body = http(
        "POST",
        "/api/v1/follow/request",
        {"asin": asin_delay, "timestamp": now_text(), "owner": "延迟测试"},
    )
    data = (body or {}).get("data") or {}
    session = db.fetchone(
        "SELECT * FROM follow_sessions WHERE parent_asin=? AND source='api_request' "
        "ORDER BY id DESC LIMIT 1",
        asin_delay,
    )
    first_check = (session or {}).get("next_tick_at") or ""
    eager = noforce_poll()                           # 立刻再取一次数：不应判定（未到检查时间）
    after_early = len(STATE["feishu"])
    check(
        "推送后先等待，不立即判定",
        session is not None and data.get("check_delay_minutes") == 2
        and (session or {}).get("attempts") == 0 and after_early == before
        and not eager.get("evaluation", {}).get("checked") and not msgs_for(asin_delay),
        f"延迟={data.get('check_delay_minutes')}分钟 首次检查={first_check} "
        f"期间该ASIN消息={len(msgs_for(asin_delay))} 提前检查次数={eager.get('evaluation', {}).get('checked')}",
    )
    # 模拟 2 分钟到点
    past = (datetime.now(CST) - timedelta(seconds=5)).isoformat(timespec="seconds")
    db.execute("UPDATE follow_sessions SET next_tick_at=? WHERE id=?", past, (session or {}).get("id"))
    noforce_poll()
    session_after = db.fetchone("SELECT * FROM follow_sessions WHERE id=?", (session or {}).get("id"))
    check(
        "到点后正常判定并发重试消息",
        (session_after or {}).get("attempts") == 1 and len(msgs_for(asin_delay)) == 1,
        f"attempts={(session_after or {}).get('attempts')} 该ASIN消息={len(msgs_for(asin_delay))}",
    )
    store.update({"follow_request_delay_minutes": 0})

    print("\n──────── 汇总 ────────")
    passed = sum(1 for _, ok, _ in CHECKS if ok)
    failed = [name for name, ok, _ in CHECKS if not ok]
    print(f"通过 {passed}/{len(CHECKS)}")
    if failed:
        print("未通过：" + "、".join(failed))
    server.should_exit = True
    fake.shutdown()
    return 0 if not failed else 1


if __name__ == "__main__":
    code = 1
    try:
        code = main()
    finally:
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    raise SystemExit(code)
