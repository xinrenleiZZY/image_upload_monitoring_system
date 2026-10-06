"""环境配置加载

优先级：界面设置（SQLite settings 表） > 环境变量 / .env > 代码默认值。
本文件只负责「环境变量 / .env / 默认值」这一层；界面可改的参数见 app/settings_store.py。
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """极简 .env 解析：KEY=VALUE，支持 # 注释与引号"""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(BASE_DIR / ".env")


def env_str(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return default if value is None else value.strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name) or default)
    except (TypeError, ValueError):
        return default


def env_bool(name: str, default: bool) -> bool:
    value = env_str(name).lower()
    if value in ("1", "true", "yes", "y", "on"):
        return True
    if value in ("0", "false", "no", "n", "off"):
        return False
    return default


class Settings:
    """进程级配置（部署时通过 .env / docker-compose 覆盖）"""

    base_dir: Path = BASE_DIR
    data_dir: Path = BASE_DIR / "data"
    static_dir: Path = BASE_DIR / "app" / "static"

    app_host: str = env_str("APP_HOST", "0.0.0.0")
    app_port: int = env_int("APP_PORT", 7701)

    db_path: str = (
        env_str("MONITOR_DB_PATH")
        or str(BASE_DIR / "data" / "monitor.db")
    )

    # 时区：优先 Asia/Shanghai，取不到时退化为固定 +08:00
    timezone_name: str = env_str("TIMEZONE", "Asia/Shanghai")

    # 开放 API 令牌（配置后 /api/v1 需要 X-API-Token；留空=完全开放，方便第三方脚本先跑通）
    api_token: str = env_str("API_TOKEN", "")

    # 后台调度线程轮询间隔（秒），仅影响调度灵敏度，不影响业务取数频率
    scheduler_tick_seconds: int = max(5, env_int("SCHEDULER_TICK_SECONDS", 15))

    # 是否启用后台调度线程（单测 / 只跑接口时可以关掉）
    scheduler_enabled: bool = env_bool("SCHEDULER_ENABLED", True)

    # 启动时是否执行一次取数
    poll_on_startup: bool = env_bool("POLL_ON_STARTUP", True)

    # 日志级别
    log_level: str = env_str("LOG_LEVEL", "INFO").upper()

    # 界面默认是否开启自动刷新
    ui_refresh_seconds: int = max(0, env_int("UI_REFRESH_SECONDS", 20))


settings = Settings()


def get_timezone():
    """返回 tzinfo；Windows 无 tzdata 时退化为固定 +08:00"""
    from zoneinfo import ZoneInfo

    try:
        return ZoneInfo(settings.timezone_name)
    except Exception:
        try:
            import tzdata  # noqa: F401  仅用于让 ZoneInfo 能找到数据库

            return ZoneInfo(settings.timezone_name)
        except Exception:
            return dt.timezone(dt.timedelta(hours=8), name="UTC+08:00")


def now_dt() -> dt.datetime:
    return dt.datetime.now(get_timezone())


def now_iso() -> str:
    """本地时区 ISO 字符串（秒级），全库统一使用该格式"""
    return now_dt().replace(microsecond=0).isoformat()


def parse_dt(value, default: Optional[dt.datetime] = None) -> Optional[dt.datetime]:
    """宽松解析时间字符串 / 时间戳（秒、毫秒）"""
    if value is None or value == "":
        return default
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=get_timezone())
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 1e11:  # 毫秒
            number /= 1000.0
        try:
            return dt.datetime.fromtimestamp(number, get_timezone())
        except (OverflowError, OSError, ValueError):
            return default
    text = str(value).strip()
    if not text:
        return default
    if text.isdigit():
        return parse_dt(int(text), default)
    normalized = text.replace("/", "-").replace("T", " ").strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S%z",
    ):
        try:
            parsed = dt.datetime.strptime(normalized, fmt)
        except ValueError:
            continue
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=get_timezone())
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return default
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=get_timezone())


def iso(value) -> Optional[str]:
    parsed = parse_dt(value)
    return parsed.replace(microsecond=0).isoformat() if parsed else None
