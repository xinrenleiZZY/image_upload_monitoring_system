# 图片上传监控系统

监控领星 ERP「亚马逊产品图片上传队列」状态，自动跟进处理中/失败的 ASIN，
失败自动在飞书群发重试消息，成功后发成功消息，超过次数提示人工检查。

参照 `E:\ZY2026\物流追踪实时库` 的结构与网页风格实现，Docker 部署，端口 **7701**（已确认 7700-7799 区间本机未占用）。

## 一、功能

| 功能 | 说明 |
| --- | --- |
| 定时取数 | 每小时调用 `POST /listing-publish-api/api/AmazonPublishQueue/list` 全量拉取（按 offset/length 翻页），**全量更新落库** |
| 状态分析 | 领星 `status`：**3=成功、4=失败、其他=处理中**（可在界面改） |
| 处理中自动跟进 | 取数发现「处理中」的 ASIN → 立刻建立跟进任务，**每 10 分钟**取一次最新状态，直到成功或失败 |
| 失败自动重试消息 | 跟进中观察到失败 → 飞书发送重试消息，重新进入跟进 |
| 单轮 30 分钟 | 每轮跟进最长 30 分钟；轮内没有出现更新的 ASIN 状态数据 → 判定重试消息失效，**重发一次**（计入 5 次） |
| 最多 5 次 | 重试消息最多 5 次，仍失败 → 发送「`XX ASIN上传失败，人工检查图片`」并结束跟进 |
| 成功通知 | 观察到成功 → 结束跟进并发送「`XX成功上传产品图片`」 |
| 开放 API | 第三方上传程序完成后推送 `ASIN + 时间戳 + 负责人（可无）`，立即进入跟进模式；**支持一次多个 ASIN**（同负责人/无负责人），按 ASIN 逐个建任务 |
| 网页 | 概览 / 上传记录 / 跟进任务 / 轮询日志 / 飞书通知 / **API 接口** / 系统设置，深浅主题，20 秒自动刷新 |
| 接口文档 | 网页「API 接口」页签内直接看可复制的调用示例；`/docs` Swagger；`/api/ui/doc/download` 下载 Markdown |

默认飞书重试消息（模板可在界面改）：

```
@二号虾掌柜 读取文档，亚马逊产品图片上传(https://qcn07e5a5sag.feishu.cn/wiki/Giwlw0pLfifm9ekHfn7c4Ulenzh)，负责人XXX，下载到本地，重试B0XXXXXXXX ASIN。执行上传产品图片自动化任务。
```

- `@二号虾掌柜`：配置「二号虾掌柜 open_id」后是真正的 @ 提醒，未配置时是纯文本；
- `负责人XXX`：第三方推送里带了负责人就显示，没带则整段省略；
- 负责人未带时，若领星记录里有操作人（`user_name`），会自动作为负责人填入。

## 二、目录结构

```
图片上传监控系统/
├── main.py                     # FastAPI 入口（含调度线程启动）
├── requirements.txt
├── Dockerfile / docker-compose.yml / .env / .env.example
├── app/
│   ├── config.py               # 环境变量 / .env 配置
│   ├── db.py                   # SQLite 建表与访问
│   ├── settings_store.py       # 界面可改参数（优先级：界面 > .env > 默认）
│   ├── http_client.py          # 标准库 HTTP 客户端（gzip / 代理 / 重试）
│   ├── lingxing.py             # 领星上传队列接口客户端 + 状态解析
│   ├── feishu.py               # 飞书通知（应用 / 自定义机器人 Webhook）+ 消息模板
│   ├── services.py             # 落库、状态分析、跟进状态机、取数主流程
│   ├── scheduler.py            # 后台调度线程
│   ├── routers/api.py          # 开放 API（/api/v1）
│   ├── routers/ui.py           # 网页接口（/api/ui）
│   └── static/index.html       # 前端页面
├── scripts/
│   ├── run_once.py             # 命令行手动取数
│   ├── send_retry_message.py   # 手动发飞书消息
│   ├── diagnose.py             # 巡检（配置自检 + 数据/会话/消息概览）
│   └── smoke_test.py           # 端到端自测（假接口，47 项检查）
├── examples/request_follow_demo.py  # 第三方「请求跟进」推送示例
├── 图片上传监控系统接口文档.md
└── data/monitor.db             # SQLite（自动生成）
```

## 三、Docker 部署

```powershell
cd "E:\ZY2026\图片上传监控系统"
docker compose up -d --build
```

- 服务地址：`http://<本机IP>:7701/`（本机 `http://localhost:7701/`）
- 在线调试：`http://localhost:7701/docs`
- 健康检查：`http://localhost:7701/healthz`
- 数据持久化：宿主机 `./data/monitor.db`（容器重建不丢数据）
- 时区：容器内 `TZ=Asia/Shanghai`

容器内访问领星需要走本机代理（Clash 7897）时，`.env` 保留：

```
LINGXING_PROXY=http://host.docker.internal:7897
```

直连可访问则留空。改完可 `docker restart image-upload-monitor`；也可直接在网页「系统设置」里改（界面值优先，保存后立即生效）。

## 四、主要参数（界面可改，也可写 .env）

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `LINGXING_AUTH_TOKEN` | 已填 | 领星登录令牌，失效后在网页「系统设置 → 领星接口」更新 |
| `QUERY_WINDOW_DAYS` | 45 | `start_time = 当前时间 - N 天`，`end_time = 当前时间` |
| `QUERY_COUNTRY_ID` | 1 | 站点过滤，留空表示不限 |
| `QUERY_PAGE_SIZE` / `QUERY_MAX_PAGES` | 100 / 50 | 翻页大小与最大页数（防死循环） |
| `POLL_INTERVAL_MINUTES` | 60 | 常规取数间隔 |
| `PROCESSING_POLL_INTERVAL_MINUTES` | 10 | 出现处理中/存在跟进任务时的取数间隔 |
| `FOLLOW_INTERVAL_MINUTES` | 10 | 跟进任务检查间隔 |
| `FOLLOW_REQUEST_DELAY_MINUTES` | 2 | **收到推送后延迟检查（分钟）**：先等这么久再取数判定，给领星留出更新状态的时间；0=收到就立刻判定 |
| `FOLLOW_ROUND_MINUTES` | 30 | 单轮跟进最长时长，超时且无新数据 → 重发 |
| `FOLLOW_MAX_ATTEMPTS` | 5 | 重试消息最大次数 |
| `FOLLOW_SESSION_MAX_HOURS` | 6 | 跟进任务最长存活，0=不限 |
| `AUTO_FOLLOW_PROCESSING` | true | 处理中自动跟进 |
| `AUTO_FOLLOW_FAILED` | false | 非本次新变化的失败是否也自动跟进 |
| `AUTO_FOLLOW_NOTIFY` | true | 自动会话是否发飞书消息（非近期记录只静默跟踪） |
| `AUTO_FOLLOW_RECENT_HOURS` | 72 | 只有最近 N 小时有动作的记录才会发消息 |
| `AUTO_FOLLOW_COOLDOWN_HOURS` | 6 | 同一 ASIN 会话结束后多久不再自动重建 |
| `FOLLOW_START_AT` | 当前设置为 `2026-09-16 17:05:13` | **失败自动跟进起始时间**：只跟进该时间之后完成/变化的失败，更早的历史失败不跟进（留空=不限制） |
| `STATUS_SUCCESS_VALUE` / `STATUS_FAILED_VALUE` | 3 / 4 | 状态判定 |
| `API_TOKEN` | 空 | 配置后 `/api/v1` 需带 `X-API-Token`；留空=完全开放 |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_CHAT_ID` / `FEISHU_BOT_OPEN_ID` | 待填 | 飞书自建应用 + 群 + 二号虾掌柜 open_id |
| `FEISHU_WEBHOOK_URL` | 空 | 也可改用群自定义机器人（与 app 二选一，优先 Webhook） |
| `NOTIFY_DRY_RUN` | false | **演练模式**：开启后所有飞书消息只写日志、不真正发到群里 |
| `AUTO_RESEND_FAILED_NOTIFY` | false | 是否自动补发发送失败的一次性通知（默认关，人工在「飞书通知」页点「重发」） |
| `FALLBACK_ENABLED` | true | **兜底数据源**：领星直连失败（如 token 过期）时自动改走自建 API 站取数 |
| `FALLBACK_BASE_URL` / `FALLBACK_PATH` | `http://192.168.40.194:7788` / `/api/proxy` | 自建领星 API 站地址与代理路径 |
| `FALLBACK_USER_ID` / `FALLBACK_USER_TOKEN` | 空 | 自建 API 站的 `X-User-Id` / `X-User-Token`（**必填，否则兜底不可用**） |
| `FALLBACK_ACCOUNT` | `xiazhanggui` | 指定用哪个领星账号的登录态取数 |
| `FALLBACK_API_NAME` / `FALLBACK_API_ID` | 刊登队列接口 | 接口库里的接口名 / api_id（优先 api_name） |
| `FALLBACK_AUTO_PAGINATION` | true | 由自建站按 total 自动翻页合并，一次拿全量 |
| `FALLBACK_ONLY_ON_AUTH` | false | 只在 token/鉴权失败时兜底；关闭则任何直连失败都兜底 |

## 五、跟进状态机（判定规则）

```
收到跟进请求 / 取数发现「处理中」
        │  （推送进来的：先等 FOLLOW_REQUEST_DELAY_MINUTES 分钟 → 再第一次取数判定）
        │
        ├─ 最新状态 = 成功 ─────────────► 结束跟进 + 发送「XX成功上传产品图片」
        │
        ├─ 最新状态 = 失败且该失败没重试过 ─► 未满 5 次：发重试消息，重开 30 分钟窗口
        │                                   已满 5 次：发「XX ASIN上传失败，人工检查图片」并结束
        │
        └─ 处理中 / 暂无记录
                 ├─ 30 分钟内出现更新的状态数据 ─► 重开 30 分钟窗口，继续跟进
                 └─ 30 分钟到且无新数据 ────────► 判定重试消息失效，重发一次（计入 5 次）
```

其它保护规则：

- **兜底数据源**：领星直连取数失败（token 过期 / 接口异常）时，系统自动改用自建 API 站（`/api/proxy` + 接口库「Amazon刊登队列列表」）取同一份数据，跟进不中断；「概览 → 数据来源」会显示本次数据来自直连还是兜底，直连失败原因也会保留，方便你更新 token；**走兜底时默认不发群消息**（需要提示就把「消息内容 → 走兜底接口时发提示」打开）；
- 同一 ASIN 同时只有一个进行中的会话；重复推送跟进请求只更新负责人，不重复建会话；
- **不会未经允许发消息**：飞书消息只在「第三方推送跟进请求 / 取数发现新失败（晚于失败跟进起始时间）/ 跟进中状态变化」这三种情况下发出；测试请用合成 ASIN（如 `B0TEST0001`）或先开启「演练模式」；
- **取数失败不重试**：auth-token 过期等取数失败时，系统只按正常节奏等下一次取数（有跟进任务 10 分钟、否则 60 分钟），不会额外重试刷接口；告警每小时最多一条，且只发「系统设置 → 消息内容 → 取数失败告警只发这个群」指定的群；**更新 auth-token 后系统会立刻重新取一次数**（不用干等到整点）；
- **会话结束后出现新结果会重新跟进**：人工结束/静默结束的会话，如果之后该 ASIN 出现了新的上传结果（新的队列记录或状态变化），会自动重新建立跟进并通知最新结果；但**已经用满 5 次重试转人工检查的 ASIN 不再自动跟进**（只在「跟进事件」里记录「人工检查阶段出现新结果」）；
- **过去失败不跟进**：系统首次上线时已有的失败当作历史数据，不会自动跟进；失败记录还要满足「失败自动跟进起始时间」（界面「系统设置 → 调度与跟进」可改），早于该时间的失败一律不建会话、不发重试消息；调整起始时间后，还没发过重试消息的历史失败会话会自动结束（已发出重试的会话继续走完 5 次流程）；
- 重试消息发送失败（飞书未配置/接口报错）**不计次数**，下一轮继续尝试，避免"没发出去也算用完 5 次"；
- 同一失败状态不会重复发送同一条消息（状态指纹 + 消息去重窗口双重保护）；
- 已提示人工检查（5 次用尽）的 ASIN，之后没有新的上传动作就不再自动跟进；有新动作会重新跟进；
- 取数失败（网络/token 失效）按 `POLL_ERROR_NOTIFY_MINUTES` 节流告警，且 `POLL_ERROR_NOTIFY_MAX_PER_DAY`（默认 3）限制每个自然日最多发送次数，并在「轮询日志」留痕。

## 六、验证与排查

```powershell
# 端到端自测（假领星接口 + 假飞书 Webhook，不碰线上数据）
docker exec image-upload-monitor python scripts/smoke_test.py

# 手动取一次数
docker exec image-upload-monitor python scripts/run_once.py

# 手动发一条重试消息
docker exec image-upload-monitor python scripts/send_retry_message.py --asin B0HCVJ6HLY --owner 张三            # 默认只预览
docker exec image-upload-monitor python scripts/send_retry_message.py --asin B0HCVJ6HLY --owner 张三 --live     # 确认发送

# 核对群里到底收到了什么（只读，需要 im:message 读取权限）
docker exec image-upload-monitor python scripts/feishu_history.py --hours 3

# 只同步数据、不发消息
docker exec image-upload-monitor python scripts/run_once.py --dry-run
```

| 现象 | 处理 |
| --- | --- |
| 页面提示领星 token 未配置或取数鉴权失败 | 重新从浏览器开发者工具复制 `auth-token`，填到「系统设置 → 领星接口」 |
| 取数报网络错误 | 确认容器到领星的网络：`LINGXING_PROXY` 代理地址，或改为直连 |
| 飞书没收到消息 | 检查 app_id/app_secret/chat_id（或 Webhook），点「发送测试」；「飞书通知」页签可看每条发送结果并重发 |
| 想暂停自动跟进 | 「系统设置 → 调度与跟进」把「发现处理中自动跟进」或「自动跟进发送通知」设为否 |
