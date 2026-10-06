"""运行参数存储：界面可改的参数（SQLite settings 表）

取值优先级：界面保存值（settings 表） > 环境变量 / .env > 代码默认值。
界面上「恢复环境默认」= 删除该键的记录，重新回落到 .env。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app import db
from app.config import env_bool, env_int, env_str, now_iso

DEFAULT_WIKI_URL = "https://qcn07e5a5sag.feishu.cn/wiki/Giwlw0pLfifm9ekHfn7c4Ulenzh"

DEFAULT_RETRY_TEMPLATE = (
    "{mention}读取文档，亚马逊产品图片上传(" + DEFAULT_WIKI_URL + ")"
    "{owner_part}，下载到本地，重试{asin} ASIN。执行上传产品图片自动化任务。"
)

# 每一项：key / 分组 / 类型 / 默认值 / 环境变量名 / 是否敏感 / 说明
SPEC: List[Dict[str, Any]] = [
    # ── 领星接口 ──
    {"key": "lingxing_base_url", "group": "领星接口", "type": "str",
     "default": "https://gw.lingxingerp.com", "env": "LINGXING_BASE_URL",
     "label": "接口域名", "help": "领星网关地址"},
    {"key": "lingxing_path", "group": "领星接口", "type": "str",
     "default": "/listing-publish-api/api/AmazonPublishQueue/list", "env": "LINGXING_PATH",
     "label": "接口路径", "help": "上传队列列表接口路径"},
    {"key": "lingxing_auth_token", "group": "领星接口", "type": "str",
     "default": "", "env": "LINGXING_AUTH_TOKEN", "secret": True,
     "label": "auth-token", "help": "浏览器登录令牌，失效后需在此更新（重新从开发者工具复制）"},
    {"key": "lingxing_company_id", "group": "领星接口", "type": "str",
     "default": "90136117059997696", "env": "LINGXING_COMPANY_ID",
     "label": "X-AK-Company-Id"},
    {"key": "lingxing_env_key", "group": "领星接口", "type": "str",
     "default": "huizhixin", "env": "LINGXING_ENV_KEY", "label": "X-AK-ENV-KEY"},
    {"key": "lingxing_uid", "group": "领星接口", "type": "str",
     "default": "11054904", "env": "LINGXING_UID", "label": "X-AK-Uid"},
    {"key": "lingxing_zid", "group": "领星接口", "type": "str",
     "default": "1", "env": "LINGXING_ZID", "label": "X-AK-Zid"},
    {"key": "lingxing_version", "group": "领星接口", "type": "str",
     "default": "3.9.1.3.0.114", "env": "LINGXING_VERSION", "label": "X-AK-Version"},
    {"key": "lingxing_language", "group": "领星接口", "type": "str",
     "default": "zh", "env": "LINGXING_LANGUAGE", "label": "X-AK-Language"},
    {"key": "lingxing_origin", "group": "领星接口", "type": "str",
     "default": "https://huizhixin.lingxing.com", "env": "LINGXING_ORIGIN",
     "label": "Origin / Referer"},
    {"key": "lingxing_platform", "group": "领星接口", "type": "str",
     "default": "1", "env": "LINGXING_PLATFORM", "label": "X-AK-PLATFORM"},
    {"key": "lingxing_client_type", "group": "领星接口", "type": "str",
     "default": "web", "env": "LINGXING_CLIENT_TYPE", "label": "AK-Client-Type"},
    {"key": "lingxing_request_source", "group": "领星接口", "type": "str",
     "default": "erp", "env": "LINGXING_REQUEST_SOURCE", "label": "X-AK-Request-Source"},
    {"key": "lingxing_proxy", "group": "领星接口", "type": "str",
     "default": "", "env": "LINGXING_PROXY",
     "label": "代理地址", "help": "Docker 内访问需要走本机代理时填 http://host.docker.internal:7897；留空=直连"},
    {"key": "lingxing_timeout_seconds", "group": "领星接口", "type": "int",
     "default": 30, "env": "LINGXING_TIMEOUT", "min": 5, "max": 300,
     "label": "单请求超时(秒)"},

    # ── 兜底接口（自建领星 API 站）──
    {"key": "fallback_enabled", "group": "兜底接口", "type": "bool",
     "default": True, "env": "FALLBACK_ENABLED",
     "label": "启用自建 API 站",
     "help": "启用自建领星 API 站作为数据源；默认它是主数据源（见「作为主数据源」），失败时回退领星直连"},
    {"key": "fallback_as_primary", "group": "兜底接口", "type": "bool",
     "default": True, "env": "FALLBACK_AS_PRIMARY",
     "label": "作为主数据源",
     "help": "开启（推荐）：优先从自建 API 站取数，失败时才回退领星直连；"
             "关闭：仍按「领星直连为主、自建站为兜底」"},
    {"key": "fallback_base_url", "group": "兜底接口", "type": "str",
     "default": "http://192.168.40.194:7788", "env": "FALLBACK_BASE_URL",
     "label": "自建 API 站地址"},
    {"key": "fallback_path", "group": "兜底接口", "type": "str",
     "default": "/api/proxy", "env": "FALLBACK_PATH", "label": "代理接口路径"},
    {"key": "fallback_user_id", "group": "兜底接口", "type": "str",
     "default": "", "env": "FALLBACK_USER_ID", "label": "X-User-Id"},
    {"key": "fallback_user_token", "group": "兜底接口", "type": "str",
     "default": "", "env": "FALLBACK_USER_TOKEN", "secret": True,
     "label": "X-User-Token", "help": "自建 API 站登录返回的 user_token"},
    {"key": "fallback_user_password", "group": "兜底接口", "type": "str",
     "default": "", "env": "FALLBACK_USER_PASSWORD", "secret": True,
     "label": "账号密码",
     "help": "自动重登用：配合 X-User-Id 调 /api/user/login。留空=不自动重登"},
    {"key": "fallback_auto_login", "group": "兜底接口", "type": "bool",
     "default": True, "env": "FALLBACK_AUTO_LOGIN",
     "label": "令牌失效自动重登",
     "help": "自建站返回「用户Token无效或已过期」时，自动用账号密码重新登录并重试一次"},
    {"key": "fallback_account", "group": "兜底接口", "type": "str",
     "default": "xiazhanggui", "env": "FALLBACK_ACCOUNT",
     "label": "指定领星账号", "help": "用该账号的登录态取数；留空=用接口库默认账号"},
    {"key": "fallback_api_name", "group": "兜底接口", "type": "str",
     "default": "Amazon刊登队列列表（AmazonPublishQueue/list）", "env": "FALLBACK_API_NAME",
     "label": "接口库 api_name"},
    {"key": "fallback_api_id", "group": "兜底接口", "type": "str",
     "default": "4833dfb271cb", "env": "FALLBACK_API_ID",
     "label": "接口库 api_id", "help": "与 api_name 二选一；优先用 api_name"},
    {"key": "fallback_auto_pagination", "group": "兜底接口", "type": "bool",
     "default": True, "env": "FALLBACK_AUTO_PAGINATION",
     "label": "让服务端自动翻页", "help": "开启后由自建 API 站按 total 自动翻页并合并（推荐）"},
    {"key": "fallback_only_on_auth", "group": "兜底接口", "type": "bool",
     "default": False, "env": "FALLBACK_ONLY_ON_AUTH",
     "label": "只在鉴权失败时兜底", "help": "开启后仅 token/鉴权类错误才走兜底；" "关闭则任何直连失败都兜底"},
    {"key": "fallback_timeout_seconds", "group": "兜底接口", "type": "int",
     "default": 120, "env": "FALLBACK_TIMEOUT", "min": 5, "max": 600,
     "label": "兜底请求超时(秒)", "help": "自动翻页取全量会比较慢，建议 ≥60 秒"},

    # ── 取数参数 ──
    {"key": "query_country_id", "group": "取数参数", "type": "str",
     "default": "1", "env": "QUERY_COUNTRY_ID",
     "label": "country_id", "help": "逗号分隔，如 1 或 1,2；留空=不限"},
    {"key": "query_queue_type", "group": "取数参数", "type": "str",
     "default": "", "env": "QUERY_QUEUE_TYPE", "label": "queue_type", "help": "留空=全部"},
    {"key": "query_status", "group": "取数参数", "type": "str",
     "default": "", "env": "QUERY_STATUS", "label": "status", "help": "留空=全部（建议留空，便于统计成功/失败）"},
    {"key": "query_store_id", "group": "取数参数", "type": "str",
     "default": "", "env": "QUERY_STORE_ID", "label": "store_id", "help": "留空=全部"},
    {"key": "query_user_id", "group": "取数参数", "type": "str",
     "default": "", "env": "QUERY_USER_ID", "label": "user_id", "help": "留空=全部"},
    {"key": "query_is_archive", "group": "取数参数", "type": "int",
     "default": 0, "env": "QUERY_IS_ARCHIVE", "min": 0, "max": 1,
     "label": "is_archive", "help": "0=未归档（默认），1=已归档"},
    {"key": "query_window_days", "group": "取数参数", "type": "int",
     "default": 45, "env": "QUERY_WINDOW_DAYS", "min": 1, "max": 365,
     "label": "时间窗口(天)", "help": "start_time = 当前时间 - 该天数"},
    {"key": "query_start_time", "group": "取数参数", "type": "str",
     "default": "", "env": "QUERY_START_TIME",
     "label": "固定 start_time", "help": "填写后固定使用，如 2026-08-03 00:00:00；留空=滚动窗口"},
    {"key": "query_page_size", "group": "取数参数", "type": "int",
     "default": 100, "env": "QUERY_PAGE_SIZE", "min": 1, "max": 500,
     "label": "每页条数(length)"},
    {"key": "query_max_pages", "group": "取数参数", "type": "int",
     "default": 50, "env": "QUERY_MAX_PAGES", "min": 1, "max": 1000,
     "label": "最大页数", "help": "防止接口异常导致死循环"},
    {"key": "req_seq_start", "group": "取数参数", "type": "int",
     "default": 1, "env": "REQ_SEQ_START", "min": 0, "max": 999999,
     "label": "req_time_sequence 起始序号"},

    # ── 调度与跟进 ──
    {"key": "poll_interval_minutes", "group": "调度与跟进", "type": "int",
     "default": 60, "env": "POLL_INTERVAL_MINUTES", "min": 1, "max": 1440,
     "label": "常规取数间隔(分钟)", "help": "默认每小时一次"},
    {"key": "processing_poll_interval_minutes", "group": "调度与跟进", "type": "int",
     "default": 10, "env": "PROCESSING_POLL_INTERVAL_MINUTES", "min": 1, "max": 1440,
     "label": "处理中/跟进时取数间隔(分钟)", "help": "出现处理中或存在跟进任务时自动切换"},
    {"key": "follow_interval_minutes", "group": "调度与跟进", "type": "int",
     "default": 10, "env": "FOLLOW_INTERVAL_MINUTES", "min": 1, "max": 1440,
     "label": "跟进检查间隔(分钟)", "help": "每个跟进任务每 10 分钟查一次最新状态"},
    {"key": "follow_request_delay_minutes", "group": "调度与跟进", "type": "int",
     "default": 2, "env": "FOLLOW_REQUEST_DELAY_MINUTES", "min": 0, "max": 60,
     "label": "收到推送后延迟检查(分钟)",
     "help": "第三方推送「请求跟进」后，先等这么久再取数判定，给领星留出更新状态的时间；"
             "0=收到就立刻判定"},
    {"key": "follow_round_minutes", "group": "调度与跟进", "type": "int",
     "default": 30, "env": "FOLLOW_ROUND_MINUTES", "min": 1, "max": 1440,
     "label": "单次跟进最长(分钟)", "help": "超时且无新状态视为重试消息失效，重发一次"},
    {"key": "follow_max_attempts", "group": "调度与跟进", "type": "int",
     "default": 5, "env": "FOLLOW_MAX_ATTEMPTS", "min": 1, "max": 50,
     "label": "最大跟进次数", "help": "重试消息最多发送次数，超过后提示人工检查"},
    {"key": "follow_session_max_hours", "group": "调度与跟进", "type": "int",
     "default": 6, "env": "FOLLOW_SESSION_MAX_HOURS", "min": 0, "max": 240,
     "label": "跟进任务最长存活(小时)", "help": "0=不限制；防止会话长期挂着"},
    {"key": "min_fetch_gap_seconds", "group": "调度与跟进", "type": "int",
     "default": 60, "env": "MIN_FETCH_GAP_SECONDS", "min": 5, "max": 3600,
     "label": "两次取数最小间隔(秒)", "help": "避免多个触发源重复请求接口"},
    {"key": "auto_follow_processing", "group": "调度与跟进", "type": "bool",
     "default": True, "env": "AUTO_FOLLOW_PROCESSING",
     "label": "发现处理中自动跟进", "help": "取数时对「处理中」的 ASIN 自动建立跟进任务"},
    {"key": "auto_follow_failed", "group": "调度与跟进", "type": "bool",
     "default": False, "env": "AUTO_FOLLOW_FAILED",
     "label": "发现失败自动跟进", "help": "开启后对新出现的失败记录也会自动跟进并发重试消息"},
    {"key": "auto_follow_recent_hours", "group": "调度与跟进", "type": "int",
     "default": 72, "env": "AUTO_FOLLOW_RECENT_HOURS", "min": 1, "max": 8760,
     "label": "自动跟进只覆盖最近(小时)"},
    {"key": "auto_follow_notify", "group": "调度与跟进", "type": "bool",
     "default": True, "env": "AUTO_FOLLOW_NOTIFY",
     "label": "自动跟进发送通知",
     "help": "自动建立的会话若命中近期记录也发送飞书消息；命中不到则只记录状态"},
    {"key": "auto_follow_cooldown_hours", "group": "调度与跟进", "type": "int",
     "default": 6, "env": "AUTO_FOLLOW_COOLDOWN_HOURS", "min": 0, "max": 8760,
     "label": "自动跟进冷却(小时)",
     "help": "同一 ASIN 的会话结束后多久内不再自动重建（第三方推送的跟进请求不受限）"},
    {"key": "follow_start_at", "group": "调度与跟进", "type": "str",
     "default": "", "env": "FOLLOW_START_AT",
     "label": "失败自动跟进起始时间",
     "help": "只自动跟进该时间之后完成/变化的失败记录（如 2026-09-16 17:00:00）；"
             "早于该时间的历史失败不再跟进。留空=不限制"},
    {"key": "auto_follow_max_sessions", "group": "调度与跟进", "type": "int",
     "default": 200, "env": "AUTO_FOLLOW_MAX_SESSIONS", "min": 1, "max": 5000,
     "label": "自动跟进最大并发数"},

    # ── 状态判定 ──
    {"key": "status_success_value", "group": "状态判定", "type": "int",
     "default": 3, "env": "STATUS_SUCCESS_VALUE", "min": -100, "max": 1000,
     "label": "成功状态值", "help": "默认 3=成功"},
    {"key": "status_failed_value", "group": "状态判定", "type": "int",
     "default": 4, "env": "STATUS_FAILED_VALUE", "min": -100, "max": 1000,
     "label": "失败状态值", "help": "默认 4=失败；其余均为处理中"},

    # ── 飞书通知 ──
    {"key": "feishu_app_id", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_APP_ID", "label": "app_id"},
    {"key": "feishu_app_secret", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_APP_SECRET", "secret": True, "label": "app_secret"},
    {"key": "feishu_chat_id", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_CHAT_ID",
     "label": "群 chat_id", "help": "多个群用英文逗号分隔（稍后补上）"},
    {"key": "feishu_receiver_open_id", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_RECEIVER_OPEN_ID",
     "label": "私聊接收人 open_id", "help": "可选：直接私聊某人时填写"},
    {"key": "feishu_bot_open_id", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_BOT_OPEN_ID",
     "label": "二号虾掌柜 open_id", "help": "用于消息中真正 @ 到二号虾掌柜（稍后补上）"},
    {"key": "feishu_bot_name", "group": "飞书通知", "type": "str",
     "default": "二号虾掌柜", "env": "FEISHU_BOT_NAME",
     "label": "被 @ 的机器人名称", "help": "未配置 open_id 时，消息里以纯文本 @ 名称"},
    {"key": "feishu_webhook_url", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_WEBHOOK_URL", "secret": True,
     "label": "自定义机器人 Webhook", "help": "可选：配置后改用群自定义机器人发送（与 app 二选一）"},
    {"key": "feishu_webhook_secret", "group": "飞书通知", "type": "str",
     "default": "", "env": "FEISHU_WEBHOOK_SECRET", "secret": True,
     "label": "Webhook 签名密钥", "help": "可选：自定义机器人开启签名校验时填写"},

    # ── 消息内容 ──
    {"key": "public_base_url", "group": "接口文档", "type": "str",
     "default": "", "env": "PUBLIC_BASE_URL",
     "label": "对外访问地址",
     "help": "第三方脚本看到的接口地址，如 http://192.168.0.193:7701；"
             "留空=用浏览器当前访问的地址（页面示例地址会跟着变）"},
    {"key": "notify_retry", "group": "消息内容", "type": "bool",
     "default": True, "env": "NOTIFY_RETRY", "label": "失败时发送重试消息"},
    {"key": "notify_success", "group": "消息内容", "type": "bool",
     "default": True, "env": "NOTIFY_SUCCESS", "label": "成功时发送成功消息"},
    {"key": "notify_final_failure", "group": "消息内容", "type": "bool",
     "default": True, "env": "NOTIFY_FINAL_FAILURE", "label": "跟进超限发送人工检查消息"},
    {"key": "notify_poll_error", "group": "消息内容", "type": "bool",
     "default": True, "env": "NOTIFY_POLL_ERROR", "label": "接口取数失败时告警"},
    {"key": "poll_error_chat_id", "group": "消息内容", "type": "str",
     "default": "oc_f57489fe7119d84bcb45567432bb8ae7", "env": "POLL_ERROR_CHAT_ID",
     "label": "取数失败告警只发这个群",
     "help": "多个群用英文逗号分隔；留空=发往「飞书通知」里配置的所有群"},
    {"key": "notify_dry_run", "group": "消息内容", "type": "bool",
     "default": False, "env": "NOTIFY_DRY_RUN",
     "label": "演练模式（只记录不发送）",
     "help": "开启后所有飞书消息只写日志、不真正发到群里，用于验证流程时不打扰群"},
    {"key": "auto_resend_failed_notify", "group": "消息内容", "type": "bool",
     "default": False, "env": "AUTO_RESEND_FAILED_NOTIFY",
     "label": "自动补发发送失败的通知",
     "help": "开启后：成功/人工检查这类一次性通知若因网络抖动没发出去，会自动补发一次；"
             "关闭则只在「飞书通知」页显示失败，由人工点「重发」"},
    {"key": "owner_fallback_to_operator", "group": "消息内容", "type": "bool",
     "default": False, "env": "OWNER_FALLBACK_TO_OPERATOR",
     "label": "负责人缺省时用领星操作人",
     "help": "接口没传 owner 时：否（默认）=消息里不出现「负责人XXX」，严格按接口参数；"
             "是=用领星记录里的操作人（user_name）补上"},
    {"key": "poll_error_notify_minutes", "group": "消息内容", "type": "int",
     "default": 60, "env": "POLL_ERROR_NOTIFY_MINUTES", "min": 1, "max": 1440,
     "label": "取数失败告警间隔(分钟)"},
    {"key": "poll_error_notify_max_per_day", "group": "消息内容", "type": "int",
     "default": 3, "env": "POLL_ERROR_NOTIFY_MAX_PER_DAY", "min": 0, "max": 24,
     "label": "取数失败告警每天最多(次)",
     "help": "与「告警间隔」同时生效：按自然日计数，达到上限后当天不再发送；0=不限制"},
    {"key": "message_dedupe_seconds", "group": "消息内容", "type": "int",
     "default": 30, "env": "MESSAGE_DEDUPE_SECONDS", "min": 0, "max": 3600,
     "label": "相同消息去重窗口(秒)"},
    {"key": "retry_message_template", "group": "消息内容", "type": "text",
     "default": DEFAULT_RETRY_TEMPLATE, "env": "RETRY_MESSAGE_TEMPLATE",
     "label": "重试消息模板",
     "help": "可用占位符：{mention} {owner} {owner_part} {asin} {status} {status_name} {failure_reason} {store} {time} {attempt} {max_attempts}"},
    {"key": "success_message_template", "group": "消息内容", "type": "text",
     "default": "{asin}成功上传产品图片", "env": "SUCCESS_MESSAGE_TEMPLATE",
     "label": "成功消息模板",
     "help": "可用占位符：{asin} {status} {status_name} {store} {owner} {time}"},
    {"key": "final_failure_message_template", "group": "消息内容", "type": "text",
     "default": "{asin}上传失败，人工检查图片", "env": "FINAL_FAILURE_MESSAGE_TEMPLATE",
     "label": "人工检查消息模板",
     "help": "可用占位符：{asin} {attempt} {max_attempts} {owner} {failure_reason} {time}"},
    {"key": "poll_error_message_template", "group": "消息内容", "type": "text",
     "default": "【图片上传监控】领星接口取数失败：{error}（{time}）",
     "env": "POLL_ERROR_MESSAGE_TEMPLATE", "label": "取数失败告警模板",
     "help": "可用占位符：{error} {time} {url}"},
    {"key": "fallback_message_template", "group": "消息内容", "type": "text",
     "default": "【图片上传监控】主数据源取数失败（{error}），已自动改用备用数据源取数成功（{time}）",
     "env": "FALLBACK_MESSAGE_TEMPLATE", "label": "备用数据源提示模板",
     "help": "主数据源失败、改用备用源成功时，在告警群里提示一次（同样受告警间隔限制）；可用 {error} {time} {asin}"},
    {"key": "notify_fallback", "group": "消息内容", "type": "bool",
     "default": False, "env": "NOTIFY_FALLBACK",
     "label": "走备用数据源时发提示",
     "help": "默认关闭：主数据源失败自动改用备用源取数时**不发**群消息；"
             "数据来源在「概览 → 数据来源」和「轮询日志」里都能看到"},
]

SPEC_BY_KEY: Dict[str, Dict[str, Any]] = {item["key"]: item for item in SPEC}

_SECRET_MASK_KEEP_HEAD = 6
_SECRET_MASK_KEEP_TAIL = 4

# 进程内临时覆盖（只影响当前进程，不写数据库）：给脚本的 --dry-run 这类场景用
_RUNTIME_OVERRIDES: Dict[str, str] = {}


def set_runtime_override(key: str, value: Any) -> None:
    if _spec(key):
        _RUNTIME_OVERRIDES[key] = "" if value is None else str(value)


def clear_runtime_overrides() -> None:
    _RUNTIME_OVERRIDES.clear()


def _spec(key: str) -> Optional[Dict[str, Any]]:
    return SPEC_BY_KEY.get(key)


def _env_value(item: Dict[str, Any]) -> Optional[str]:
    name = item.get("env")
    if not name:
        return None
    raw = env_str(name, "__MISSING__")
    return None if raw == "__MISSING__" else raw


def effective(key: str) -> tuple[str, str]:
    """返回 (值, 来源)，来源：ui / env / default"""
    item = _spec(key)
    if item is None:
        return "", "unknown"
    if key in _RUNTIME_OVERRIDES:
        return _RUNTIME_OVERRIDES[key], "runtime"
    row = db.fetchone("SELECT value FROM settings WHERE key = ?", key)
    # 只要界面保存过（哪怕保存成空串），就以界面值为准
    if row is not None:
        return ("" if row.get("value") is None else str(row.get("value"))), "ui"
    env_value = _env_value(item)
    if env_value is not None and str(env_value).strip() != "":
        return str(env_value), "env"
    return str(item.get("default", "")), "default"


def _coerce(key: str, raw: str):
    item = _spec(key) or {}
    kind = item.get("type", "str")
    text = "" if raw is None else str(raw).strip()
    if kind == "int":
        if text == "":
            return int(item.get("default", 0) or 0)
        try:
            number = int(float(text))
        except ValueError:
            raise ValueError(f"{item.get('label', key)} 需要整数")
        low, high = item.get("min"), item.get("max")
        if low is not None and number < low:
            raise ValueError(f"{item.get('label', key)} 不能小于 {low}")
        if high is not None and number > high:
            raise ValueError(f"{item.get('label', key)} 不能大于 {high}")
        return number
    if kind == "bool":
        if text == "":
            return bool(item.get("default", False))
        lowered = text.lower()
        if lowered in ("1", "true", "yes", "y", "on"):
            return True
        if lowered in ("0", "false", "no", "n", "off"):
            return False
        raise ValueError(f"{item.get('label', key)} 需要 true/false")
    return text


def get_str(key: str, default: str = "") -> str:
    value, _ = effective(key)
    return value if value != "" else default


def get_int(key: str, default: int = 0) -> int:
    try:
        return int(_coerce(key, effective(key)[0]))
    except (ValueError, TypeError):
        return default


def get_bool(key: str, default: bool = False) -> bool:
    try:
        return bool(_coerce(key, effective(key)[0]))
    except (ValueError, TypeError):
        return default


def get_json(key: str, default):
    raw = get_str(key, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except ValueError:
        return default


def update(values: Dict[str, Any], *, wipe_empty: bool = False) -> Dict[str, Any]:
    """保存界面设置；secret 字段传空表示「保持原值不变」

    返回 {"saved": [...], "rejected": {key: 原因}}
    """
    saved: List[str] = []
    rejected: Dict[str, str] = {}
    now = now_iso()
    with db.transaction() as conn:
        for key, raw in values.items():
            item = _spec(key)
            if item is None:
                continue
            try:
                value = _coerce(key, raw)
            except ValueError as exc:
                rejected[key] = str(exc)
                continue
            if item.get("secret") and str(raw).strip() == "":
                continue
            if str(value) == "" and not wipe_empty:
                # 空值 = 回落到 .env / 默认值（删除记录）
                conn.execute("DELETE FROM settings WHERE key = ?", (key,))
                saved.append(key)
                continue
            conn.execute(
                "INSERT INTO settings(key, value, updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, str(value), now),
            )
            saved.append(key)
    return {"saved": saved, "rejected": rejected}


def reset(keys: List[str]) -> int:
    count = 0
    for key in keys:
        if _spec(key):
            count += db.execute("DELETE FROM settings WHERE key = ?", key)
    return count


def _mask(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= _SECRET_MASK_KEEP_HEAD + _SECRET_MASK_KEEP_TAIL:
        return "*" * len(text)
    return (
        text[:_SECRET_MASK_KEEP_HEAD]
        + "*" * (len(text) - _SECRET_MASK_KEEP_HEAD - _SECRET_MASK_KEEP_TAIL)
        + text[-_SECRET_MASK_KEEP_TAIL:]
    )


def describe() -> Dict[str, Any]:
    """给界面用的参数结构（分组的、带来源、敏感值打码）"""
    rows = {r["key"]: r["value"] for r in db.fetchall("SELECT key, value FROM settings")}
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in SPEC:
        key = item["key"]
        value, source = effective(key)
        visible = _mask(value) if item.get("secret") else value
        groups.setdefault(item["group"], []).append(
            {
                "key": key,
                "label": item.get("label", key),
                "type": item.get("type", "str"),
                "help": item.get("help", ""),
                "secret": bool(item.get("secret")),
                "value": visible,
                "has_value": bool(str(value).strip()),
                "source": source,
                "default": "" if item.get("secret") else item.get("default", ""),
                "overridden": key in rows,
            }
        )
    return {
        "groups": [{"group": name, "items": items} for name, items in groups.items()],
        "default_retry_template": DEFAULT_RETRY_TEMPLATE,
        "wiki_url": DEFAULT_WIKI_URL,
    }


def snapshot() -> Dict[str, Any]:
    """运行期需要的全部参数快照（每次调用实时读取，改配置后下一轮生效）"""
    data: Dict[str, Any] = {}
    for item in SPEC:
        key = item["key"]
        kind = item.get("type", "str")
        raw, source = effective(key)
        if kind == "int":
            data[key] = get_int(key, int(item.get("default", 0) or 0))
        elif kind == "bool":
            data[key] = get_bool(key, bool(item.get("default", False)))
        else:
            data[key] = raw
        data[key + "__source"] = source
    return data


def feishu_ready(cfg: Dict[str, Any]) -> bool:
    has_app = bool(cfg.get("feishu_app_id")) and bool(cfg.get("feishu_app_secret"))
    has_target = bool(cfg.get("feishu_chat_id")) or bool(cfg.get("feishu_receiver_open_id"))
    return bool(cfg.get("feishu_webhook_url")) or (has_app and has_target)


def lingxing_ready(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("lingxing_auth_token"))


def poll_intervals(cfg: Dict[str, Any]) -> Dict[str, int]:
    """常规/处理中间隔（秒）"""
    normal = max(1, int(cfg.get("poll_interval_minutes", 60))) * 60
    busy = max(1, int(cfg.get("processing_poll_interval_minutes", 10))) * 60
    return {"normal": normal, "busy": busy}


ID_TYPES = {key: spec_.get("type", "str") for key, spec_ in SPEC_BY_KEY.items()}
ALL_KEYS = [item["key"] for item in SPEC]
