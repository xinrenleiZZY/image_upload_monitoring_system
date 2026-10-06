"""第三方「请求跟进」推送示例（上传产品图片的程序执行完成后调用）

只要三行核心代码（标准库，无需安装依赖）：

    requests.post(URL, json={"asin": "...", "timestamp": "...", "owner": "..."})

参数说明：
    asin       必填，也可以是 parent_asin / asins=["A","B"]
    timestamp  推荐填上传完成时间（"2026-09-16 15:45:02" 或秒/毫秒时间戳），可省略（默认取服务端时间）
    owner      负责人，可省略

服务端会立刻把该 ASIN 置为跟进模式：每 10 分钟查一次领星最新状态，
失败自动发飞书重试消息，成功发成功消息，超过 5 次仍未成功提示人工检查。
"""

from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime

# 兼容 Python 2 风格的直接执行：把本文件所在目录的上一级加入 path（仅为示例方便）
sys.path.insert(0, ".")

# ── 按实际部署修改 ──
BASE_URL = "http://192.168.0.193:7701"  # 监控系统地址（Docker 部署在 7701 端口）
API_TOKEN = ""                           # 监控系统配置了 API_TOKEN 时才需要填


def request_follow(asin: str, owner: str = "", timestamp: str = "") -> dict:
    """推送一次「请求跟进」"""
    payload = {
        "asin": asin,
        "timestamp": timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "owner": owner or "",
        "source": "product-image-upload",
    }
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    if API_TOKEN:
        headers["X-API-Token"] = API_TOKEN
    req = urllib.request.Request(
        f"{BASE_URL}/api/v1/follow/request",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


if __name__ == "__main__":
    # 示例：上传完成后推送
    result = request_follow(asin="B0HCVJ6HLY", owner="何紫欣")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    # 也可以用 GET（最简调用）：
    # http://192.168.0.193:7701/api/v1/follow/request?asin=B0HCVJ6HLY&timestamp=2026-09-16%2015:45:02&owner=何紫欣
