"""消息流源领域服务：管理Kafka/Pulsar/RocketMQ消息流接入源，负责消费组注册、消息拉取和路径分流。

数据链路位置：消息接入层，通过中间件消费消息并分发给下游的去重、解析、校验等处理模块。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamSourceService(BaseModuleService):
    MODULE = "stream_source"
    ENTITY = "消息流源"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源名称", "中间件类型", "Topic/Queue名", "消费组", "偏移量策略"]
    REQUIRED_FIELDS = ["源名称", "中间件类型", "Topic/Queue名"]
    STATUS_ORDER = ["待验证", "运行中", "已暂停", "连接异常"]
    ACTION_RULES = {"启动消费": "运行中", "暂停消费": "已暂停"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：消息消费与路由 ----

    def register_consumer_group(self, entry_id: int) -> dict[str, Any]:
        """注册消费组并绑定偏移量策略。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "消息流源不存在"}
        group = entry.get("消费组", "")
        topic = entry.get("Topic/Queue名", "")
        if not group or not topic:
            return {"ok": False, "message": "消费组和Topic不能为空"}
        offset_rows = store.rows("offset_manager")
        oid = max((int(r.get("id", 0)) for r in offset_rows), default=0) + 1
        offset_rows.append({
            "id": oid,
            "消费组": group,
            "Topic/Queue": topic,
            "分区/分片": "0",
            "当前Offset": "0",
            "消费延迟": "0",
            "status": "正常",
            "pending": True,
            "abnormal": False,
        })
        return {"ok": True, "message": f"消费组 {group} 已注册", "offset_id": oid}

    def consume_messages(self, entry_id: int, batch_size: int = 100) -> list[dict[str, Any]]:
        """从消息流源拉取消息并生产到载荷存储。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "运行中":
            return []
        topic = entry.get("Topic/Queue名", "unknown")
        source = entry.get("源名称", "unknown")
        messages = []
        for i in range(min(batch_size, 3)):
            msg_id = f"msg-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}-{i}"
            messages.append({"id": msg_id, "topic": topic, "source": source, "payload": f"{topic}-data-{i}"})
        return messages

    def validate_message(self, message: dict[str, Any]) -> list[str]:
        """校验消息结构和必填字段。"""
        errors = []
        if not isinstance(message.get("payload"), str):
            errors.append("缺少消息载荷")
        if not isinstance(message.get("id"), str):
            errors.append("缺少消息ID")
        return errors

    def route_to_dedup(self, entry_id: int, message_id: str) -> str | None:
        """根据消息流源配置路由到去重中心。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        dedup_keys = store.rows("dedup_center")
        for key in dedup_keys:
            if key.get("status") == "已启用" and entry.get("Topic/Queue名", "").casefold() in key.get("关联数据源", "").casefold():
                return key.get("去重键名", "")
        return None

    def track_throughput(self, entry_id: int, consumed: int) -> None:
        """记录吞吐量到告警监控。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        alarm_rows = store.rows("throughput_alarm")
        now = datetime.now(timezone.utc).isoformat()
        for alarm in alarm_rows:
            if alarm.get("status") == "已生效" and entry.get("源名称", "") == alarm.get("监控数据源", ""):
                try:
                    floor = int(alarm.get("吞吐量下限", "0"))
                except ValueError:
                    continue
                if consumed < floor:
                    alarm["abnormal"] = True