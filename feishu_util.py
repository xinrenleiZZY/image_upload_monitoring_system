"""飞书通知工具（可选）— 独立功能模块

配置（config.json 或环境变量，见 config/settings.py）：
    feishu.app_id      飞书自建应用 App ID
    feishu.app_secret  飞书自建应用 App Secret
    feishu.chat_id     目标群聊 chat_id（receive_id_type=chat_id），多个群用英文逗号分隔

用法：
    阶段完成时统一调用一个方法即可触发发送：
        from utils.feishu_util import FeishuNotifier
        FeishuNotifier().notify_stage_complete("阶段二 手动同步", True, summary, error_file)
"""

import json
import os
from typing import Optional

import requests

from config.settings import FEISHU

# 「导入失败」卡片表格列定义：(数据键, 表头, 列宽)
IMPORT_ERROR_COLUMNS = [
    ("row", "行号", "80px"),
    ("asin", "ASIN", "130px"),
    ("msku", "MSKU", "auto"),
    ("fnsku", "FNSKU", "auto"),
    ("gtin", "GTIN", "150px"),
    ("msg", "失败原因", "auto"),
]


def _cell(value) -> str:
    """表格单元格取值：空值统一显示为 -"""
    return str(value if value is not None else "").strip() or "-"


def build_import_error_card(store_name: str, rows: list) -> dict:
    """构建「导入失败」飞书卡片（卡片 JSON 2.0：富文本 + ASIN 失败名单表格）

    Args:
        store_name: 店铺名
        rows: 领星 importTransparencyProductList 回执的 error_list，
              每项含 row / asin / msku / fnsku / gtin / msg
    """
    table_rows = [
        {key: _cell(item.get(key)) for key, _, _ in IMPORT_ERROR_COLUMNS}
        for item in rows
        if isinstance(item, dict)
    ]
    return {
        "schema": "2.0",
        "config": {"update_multi": True},
        "header": {
            "title": {"tag": "plain_text",
                      "content": f"【透明计划 · 阶段二 手动同步·{store_name}】❌ 失败"},
            "template": "red",
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": f"**导入失败 {len(table_rows)} 条 ASIN**"},
                {
                    "tag": "table",
                    "page_size": 10,
                    "row_height": "low",
                    "header_style": {
                        "text_align": "left",
                        "text_size": "normal",
                        "background_style": "grey",
                        "text_color": "grey",
                        "bold": True,
                        "lines": 1,
                    },
                    "columns": [
                        {"name": key, "display_name": title, "data_type": "text", "width": width}
                        for key, title, width in IMPORT_ERROR_COLUMNS
                    ],
                    "rows": table_rows,
                },
            ]
        },
    }


class FeishuNotifier:
    """飞书消息通知（应用机器人：tenant_access_token + im/v1/messages）"""

    def __init__(self, app_id: str = "", app_secret: str = "", chat_id: str = ""):
        self.app_id = app_id or FEISHU.get("app_id", "")
        self.app_secret = app_secret or FEISHU.get("app_secret", "")
        self.chat_id = chat_id or FEISHU.get("chat_id", "")
        self._token = ""
        self._token_expires = 0

    # ── 工具 ──
    @property
    def chat_ids(self) -> list:
        """目标群列表（chat_id 支持英文逗号分隔多个群，自动去重保序）"""
        ids = [x.strip() for x in self.chat_id.split(",") if x.strip()]
        return list(dict.fromkeys(ids))

    @property
    def enabled(self) -> bool:
        """配置是否完整（app_id/app_secret/至少一个群 齐全才算可用）"""
        return bool(self.app_id and self.app_secret and self.chat_ids)

    def _targets(self, chat_id: str = "") -> list:
        """确定发送目标：显式传入 chat_id 则单发，否则发往所有配置群"""
        return [chat_id] if chat_id else self.chat_ids

    def _post_message(self, token: str, chat_id: str, msg_type: str, content: dict) -> bool:
        """向单个群发送一条消息，返回是否成功"""
        try:
            resp = requests.post(
                "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={
                    "receive_id": chat_id,
                    "msg_type": msg_type,
                    "content": json.dumps(content, ensure_ascii=False),
                },
                timeout=15,
            )
            data = resp.json()
            if data.get("code") != 0:
                print(f"[飞书] 发送消息失败（{chat_id}）: {data.get('msg')}（code={data.get('code')}）")
                return False
            return True
        except Exception as e:
            print(f"[飞书] 发送消息失败（{chat_id}）: {e}")
            return False

    def _get_token(self) -> Optional[str]:
        """获取飞书 tenant_access_token（带缓存）"""
        if self._token:
            return self._token

        if not self.app_id or not self.app_secret:
            print("[飞书] 未配置 app_id/app_secret，跳过通知")
            return None

        try:
            resp = requests.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=10,
            )
            data = resp.json()
            if data.get("code") != 0:
                print(f"[飞书] 获取 token 失败: {data.get('msg')}")
                return None
            self._token = data.get("tenant_access_token", "")
            return self._token
        except Exception as e:
            print(f"[飞书] 获取 token 失败: {e}")
            return None

    # ── 发送 ──
    def send_text(self, text: str, chat_id: str = "") -> bool:
        """发送文本消息（chat_id 缺省发往所有配置群）"""
        targets = self._targets(chat_id)
        if not targets:
            print("[飞书] 未配置 chat_id，跳过文本消息")
            return False
        token = self._get_token()
        if not token:
            return False
        results = [self._post_message(token, cid, "text", {"text": text}) for cid in targets]
        return all(results)

    def send_file(self, file_path: str, chat_id: str = "") -> bool:
        """发送文件消息（chat_id 缺省发往所有配置群，文件只上传一次）"""
        targets = self._targets(chat_id)
        if not targets:
            print("[飞书] 未配置 chat_id，跳过文件消息")
            return False
        token = self._get_token()
        if not token:
            return False

        if not os.path.exists(file_path):
            print(f"[飞书] 文件不存在: {file_path}")
            return False

        try:
            headers = {"Authorization": f"Bearer {token}"}
            file_name = os.path.basename(file_path)
            with open(file_path, "rb") as f:
                upload_resp = requests.post(
                    "https://open.feishu.cn/open-apis/im/v1/files",
                    headers=headers,
                    files={"file": (file_name, f, "application/octet-stream")},
                    data={"file_type": "stream", "file_name": file_name},
                    timeout=60,
                )
            upload_data = upload_resp.json()
            file_key = upload_data.get("data", {}).get("file_key", "")
            if not file_key:
                print(f"[飞书] 文件上传失败: {upload_data.get('msg', upload_data)}")
                return False
            results = [
                self._post_message(token, cid, "file", {"file_key": file_key}) for cid in targets
            ]
            return all(results)
        except Exception as e:
            print(f"[飞书] 发送文件失败: {e}")
            return False

    def send_card(self, card: dict, chat_id: str = "") -> bool:
        """发送消息卡片（富文本，msg_type=interactive，chat_id 缺省发往所有配置群）"""
        targets = self._targets(chat_id)
        if not targets:
            print("[飞书] 未配置 chat_id，跳过卡片消息")
            return False
        token = self._get_token()
        if not token:
            return False
        results = [self._post_message(token, cid, "interactive", card) for cid in targets]
        return all(results)

    def notify_import_errors(self, store_name: str, rows: list, chat_id: str = "") -> bool:
        """领星导入失败通知：卡片 + ASIN 失败名单表格

        Args:
            store_name: 店铺名
            rows: 领星 error_list（row/asin/msku/fnsku/gtin/msg）
            chat_id: 目标群（缺省用配置值）

        Returns:
            True=已发送（或未配置时静默返回 True 以便流程不中断）
        """
        if not self.enabled:
            print("[飞书] 未配置 app_id/app_secret/chat_id，跳过阶段通知")
            return True
        return self.send_card(build_import_error_card(store_name, rows), chat_id)

    def notify_summary(self, summary: str, file_path: str = None, chat_id: str = "") -> bool:
        """发送汇总通知（文本 + 可选文件）"""
        ok = self.send_text(summary, chat_id)
        if file_path and os.path.exists(file_path):
            ok = self.send_file(file_path, chat_id) and ok
        return ok

    # ── 阶段完成统一入口 ──
    def notify_stage_complete(self, stage: str, success: bool, summary: str,
                              file_path: str = None, chat_id: str = "") -> bool:
        """阶段（一/二/三）完成时统一调用：发送完成状态 + 摘要 + 可选报错文件

        Args:
            stage: 阶段名（如 "阶段一 加入透明标签" / "阶段二 手动同步" / "阶段三 开启OPR"）
            success: 是否全部成功
            summary: 摘要文本（每行一条结果）
            file_path: 可选报错文件（xlsx 等），随消息推送
            chat_id: 目标群（缺省用配置值）

        Returns:
            True=已发送（或未配置时静默返回 True 以便流程不中断）
        """
        if not self.enabled:
            print("[飞书] 未配置 app_id/app_secret/chat_id，跳过阶段通知")
            return True

        icon = "✅" if success else "❌"
        text = f"【透明计划 · {stage}】{icon} {'完成' if success else '失败'}\n{summary}"
        return self.notify_summary(text, file_path, chat_id)
