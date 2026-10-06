"""后台调度线程

职责（与 FastAPI 同进程，单线程串行执行，避免并发取数）：
1. 到期的跟进会话 → 先保证数据新鲜，再判定状态、发消息
2. 常规取数到点 → 每 poll_interval_minutes 一次（默认 60 分钟）
3. 只要存在跟进会话或「处理中」记录 → 自动切换为 processing_poll_interval_minutes（默认 10 分钟）
4. 维护 meta.next_poll_at 供界面展示
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
from typing import Optional

from app import db
from app import services
from app import settings_store as store
from app.config import now_dt, parse_dt, settings

logger = logging.getLogger("monitor.scheduler")


class Scheduler:
    def __init__(self, tick_seconds: int = 15):
        self.tick_seconds = max(5, int(tick_seconds))
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_tick_at = ""
        self._last_action = ""

    # ── 生命周期 ──
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="monitor-scheduler", daemon=True)
        self._thread.start()
        logger.info("调度线程已启动（tick=%ss）", self.tick_seconds)

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        logger.info("调度线程已停止")

    def wake(self) -> None:
        """外部（如收到跟进请求）立刻触发一次调度"""
        self._wake.set()

    # ── 内部 ──
    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:  # 调度线程不能因单次异常退出
                logger.exception("调度轮询异常：%s", exc)
            self._wake.wait(timeout=self.tick_seconds)
            self._wake.clear()

    def _tick(self) -> None:
        now = now_dt()
        self._last_tick_at = now.replace(microsecond=0).isoformat()
        cfg = store.snapshot()
        gap = int(cfg.get("min_fetch_gap_seconds", 60))

        if services.due_sessions(now):
            logger.info("存在到期的跟进任务，刷新数据并判定")
            self._last_action = "follow"
            services.refresh_and_evaluate("follow_tick", max_age_seconds=gap)
        else:
            interval = services.poll_interval_seconds(cfg)
            due = services.poll_due(now)
            if due:
                logger.info("常规取数到点（间隔 %s 秒）", interval)
                self._last_action = "poll"
                services.poll_once("schedule")
            else:
                self._last_action = "idle"
        # 顺手补发因网络抖动丢失的一次性通知（成功/人工检查），有 5 分钟间隔保护
        try:
            services.retry_failed_notifications()
        except Exception as exc:  # 补发失败不影响主流程
            logger.warning("补发通知失败：%s", exc)
        self._write_next_poll(now, cfg)

    @staticmethod
    def _effective_interval(cfg) -> int:
        """取数间隔（正常 / 处理中 / 失败退避），统一由 services 计算"""
        return services.poll_interval_seconds(cfg)

    @staticmethod
    def _write_next_poll(now: dt.datetime, cfg) -> None:
        interval = Scheduler._effective_interval(cfg)
        last = (
            parse_dt(db.get_meta("last_poll_attempt_at"))
            or parse_dt(db.get_meta("last_poll_ok_at"))
            or now
        )
        next_at = last + dt.timedelta(seconds=interval)
        if next_at < now:
            next_at = now
        db.set_meta("next_poll_at", next_at.replace(microsecond=0).isoformat())

    def status(self) -> dict:
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "tick_seconds": self.tick_seconds,
            "last_tick_at": self._last_tick_at,
            "last_action": self._last_action,
        }


scheduler = Scheduler(settings.scheduler_tick_seconds)
