"""去重中心领域服务：对实时流数据执行窗口去重，按业务主键或消息ID进行去重校验。

数据链路位置：去重过滤层，在消息进入处理管道前执行去重，防止数据重复造成的统计偏差。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DedupCenterService(BaseModuleService):
    MODULE = "dedup_center"
    ENTITY = "去重键"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["去重键名", "关联数据源", "去重窗口秒数", "保留策略", "去重计数"]
    REQUIRED_FIELDS = ["去重键名", "关联数据源", "去重窗口秒数"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用去重": "已启用", "停用去重": "已停用"}
    NEGATIVE_ACTIONS = ["停用去重"]

    # ---- 业务扩展：去重校验与统计 ----

    def check_duplicate(self, entry_id: int, message_id: str) -> dict[str, Any]:
        """检查消息是否在去重窗口内重复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"duplicate": False, "message": "去重键未启用"}
        try:
            window_str = str(entry.get("去重窗口秒数", "0"))
            window = int(window_str) if window_str.isdigit() else 60
        except ValueError:
            window = 60
        payload_rows = store.rows("payload_store")
        source = entry.get("关联数据源", "")
        now = datetime.now(timezone.utc)
        seen = []
        for row in payload_rows:
            if source in str(row.get("采集批次", "")):
                time_str = str(row.get("存储时间", ""))
                try:
                    store_time = datetime.fromisoformat(time_str)
                    if (now - store_time).total_seconds() <= window:
                        seen.append(row.get("消息ID", ""))
                except (ValueError, TypeError):
                    continue
        if message_id in seen:
            count = int(entry.get("去重计数", "0"))
            entry["去重计数"] = str(count + 1)
            return {"duplicate": True, "message": "消息在去重窗口内重复", "window": window}
        return {"duplicate": False, "message": "消息通过去重", "window": window}

    def validate_dedup_window(self, entry_id: int) -> list[str]:
        """校验去重窗口参数的合法性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["去重键不存在"]
        try:
            window = int(str(entry.get("去重窗口秒数", "0")))
        except ValueError:
            errors.append("去重窗口秒数不是有效整数")
            return errors
        if window <= 0:
            errors.append("去重窗口秒数必须大于0")
        if window > 86400:
            errors.append("去重窗口秒数不能超过24小时")
        return errors

    def route_verified_to_sink(self, entry_id: int, message_id: str) -> str | None:
        """去重通过的消息路由到数据落地配置。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        sinks = store.rows("data_sink")
        source = entry.get("关联数据源", "")
        for sink in sinks:
            if sink.get("status") == "已启用" and source in str(sink.get("配置名称", "")):
                return sink.get("配置名称", "")
        return None

    def report_dedup_stat(self, entry_id: int) -> dict[str, Any]:
        """输出去重统计报表。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"total": 0, "duplicate": 0, "unique": 0}
        total = int(entry.get("去重计数", "0"))
        return {
            "key": entry.get("去重键名", ""),
            "total_checked": total,
            "window_seconds": entry.get("去重窗口秒数", "0"),
            "policy": entry.get("保留策略", ""),
        }

    def archive_dedup_state(self, entry_id: int) -> None:
        """归档去重状态到水位追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        watermark_rows.append({
            "id": wid, "数据源": entry.get("关联数据源", ""),
            "水位字段": "去重计数", "当前水位": entry.get("去重计数", "0"),
            "上次推进时间": now, "延迟分钟数": "0",
            "status": "正常水位", "pending": True, "abnormal": False,
        })