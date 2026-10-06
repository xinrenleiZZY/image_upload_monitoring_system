"""开放 API 请求模型

第三方「请求跟进」脚本只需要最简结构：

    POST /api/v1/follow/request
    {"asin": "B0HCVJ6HLY", "timestamp": "2026-09-16 15:45:02", "owner": "张三"}

也支持一次提交多个 ASIN（同负责人 / 无负责人），系统会按 ASIN 逐个建立跟进任务：

    {"asins": ["B0HCVJ6HLY", "B0HDTTHHTW"], "owner": "张三"}
    {"asin": "B0HCVJ6HLY,B0HDTTHHTW"}          # 逗号/分号/空格/换行分隔
    ["B0HCVJ6HLY", "B0HDTTHHTW"]               # 直接传 JSON 数组

字段别名全部兼容（parent_asin / asins / asin_list / ASIN / time / ts / request_time /
user / operator / 负责人 …），时间戳支持 "YYYY-MM-DD HH:MM:SS"、ISO、秒级/毫秒级时间戳。
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator

MAX_ASINS = 200

# ASIN 之间可能出现的分隔符：英文/中文逗号、分号、空格、换行、竖线
_SPLIT_RE = re.compile(r"[,，;；|\s]+")


def split_asins(raw: Any) -> List[str]:
    """把各种写法拆成 ASIN 列表：字符串（含分隔符）、数组、嵌套对象都支持"""
    out: List[str] = []
    if raw is None:
        return out
    if isinstance(raw, dict):
        # 兼容 [{"asin": "A"}, ...] 这类结构
        for key in ("asin", "parent_asin", "ASIN", "asin_list", "asins", "value"):
            if key in raw:
                out.extend(split_asins(raw[key]))
        return out
    if isinstance(raw, (list, tuple, set)):
        for item in raw:
            out.extend(split_asins(item))
        return out
    text = str(raw).strip().upper()
    if not text:
        return out
    for token in _SPLIT_RE.split(text):
        token = token.strip().upper()
        if token and token not in out:
            out.append(token)
    return out


class FollowRequest(BaseModel):
    model_config = {"extra": "allow"}

    asin: Optional[Any] = None
    parent_asin: Optional[Any] = None
    asins: Optional[Any] = None
    timestamp: Optional[Any] = None
    ts: Optional[Any] = None
    time: Optional[Any] = None
    request_time: Optional[Any] = None
    owner: Optional[str] = None
    operator: Optional[str] = None
    负责人: Optional[str] = None
    source: Optional[str] = None
    remark: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _merge_asins(cls, values):
        """合并各种 ASIN 写法；兼容 asin_list / asinList / ASIN / codes / items / data"""
        if not isinstance(values, dict):
            return values
        merged: List[str] = []
        for key in ("asins", "asin_list", "asinList", "ASIN", "codes", "items", "list", "data"):
            if key in values:
                merged.extend(split_asins(values[key]))
        if merged:
            values["asins"] = merged
        return values

    def asin_list(self) -> List[str]:
        out: List[str] = []
        for raw in (self.asin, self.parent_asin, self.asins):
            for text in split_asins(raw):
                if text not in out:
                    out.append(text)
        return out

    def owner_text(self) -> str:
        for value in (self.owner, self.operator, self.负责人):
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def timestamp_value(self) -> Optional[Any]:
        for value in (self.timestamp, self.ts, self.time, self.request_time):
            if value not in (None, ""):
                return value
        return None


class AsinQuery(BaseModel):
    """批量查状态：asins 支持数组或逗号/分号/空格分隔的字符串"""

    model_config = {"extra": "allow"}

    asins: Any = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _collect(cls, values):
        if not isinstance(values, dict):
            # 直接传数组也行
            return {"asins": values}
        merged: List[str] = []
        for key in ("asins", "asin", "parent_asin", "ASIN", "codes", "list", "data"):
            if key in values:
                merged.extend(split_asins(values[key]))
        if merged:
            values["asins"] = merged
        return values

    def asin_list(self) -> List[str]:
        return split_asins(self.asins)[:500]


class CancelRequest(BaseModel):
    model_config = {"extra": "allow"}

    asin: Optional[Any] = None
    parent_asin: Optional[Any] = None
    asins: Optional[Any] = None
    reason: Optional[str] = None

    def asin_list(self) -> List[str]:
        out: List[str] = []
        for raw in (self.asin, self.parent_asin, self.asins):
            for text in split_asins(raw):
                if text not in out:
                    out.append(text)
        return out
