"""图片上传监控系统 —— FastAPI 入口

功能：
1. 每小时（默认）调用领星 AmazonPublishQueue/list 接口，全量更新落库
2. 分析每个 ASIN 的上传状态（3=成功，4=失败，其余=处理中）
3. 出现「处理中」或收到第三方「请求跟进」→ 立即进入跟进模式（每 10 分钟一次）
4. 跟进中失败 → 飞书发送重试消息；成功 → 发送成功消息；超过 5 次 → 提示人工检查
5. 开放 API + 前端展示页面（http://<本机IP>:7701/）
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

from app import db
from app import services
from app.config import now_iso, settings
from app.routers import api, ui
from app.scheduler import scheduler

STATIC_DIR: Path = settings.static_dir

logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("monitor.main")


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.init_schema()
    logger.info("数据库已就绪：%s", settings.db_path)
    if settings.scheduler_enabled:
        scheduler.start()
        if settings.poll_on_startup:
            scheduler.wake()  # 启动后立刻执行一次调度（含首轮取数）
    else:
        logger.warning("后台调度线程已禁用（SCHEDULER_ENABLED=false），仅提供接口与手动取数")
    logger.info("服务已启动：http://%s:%s", settings.app_host, settings.app_port)
    try:
        yield
    finally:
        scheduler.stop()


app = FastAPI(
    title="图片上传监控系统",
    description=(
        "领星亚马逊产品图片上传队列监控：定时取数落库 → 分析 ASIN 上传状态 → "
        "处理中/请求跟进自动进入跟进模式 → 失败发送飞书重试消息 → 成功/超限通知。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api.router)
app.include_router(ui.router)

FALLBACK_HTML = """<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<title>图片上传监控系统</title><body><h1>图片上传监控系统</h1>
<p>首页静态文件缺失，接口调试请访问 <a href="/docs">/docs</a>。</p></body></html>"""


@app.get("/", include_in_schema=False)
def index():
    page = STATIC_DIR / "index.html"
    if page.exists():
        return FileResponse(page)
    return HTMLResponse(FALLBACK_HTML)


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return HTMLResponse(status_code=204)


@app.get("/healthz", include_in_schema=False)
def healthz():
    summary = services.summary()
    return {
        "status": "ok",
        "time": now_iso(),
        "records_total": summary["records_total"],
        "follow_active": summary["follow_active"],
        "last_poll_ok_at": summary["last_poll_ok_at"],
        "last_poll_error": summary["last_poll_error"],
        "scheduler": scheduler.status(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.app_host, port=settings.app_port, reload=False)
