"""数据落地配置领域服务：配置处理后的数据写入目标存储的路径、分区和保留策略。

数据链路位置：数据落位层，将处理完成的结构化数据按配置写入目标存储，是数据链路最后一个写入环节。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataSinkService(BaseModuleService):
    MODULE = "data_sink"
    ENTITY = "落地配置"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["配置名称", "目标存储类型", "写入模式", "分区策略", "保留天数"]
    REQUIRED_FIELDS = ["配置名称", "目标存储类型", "写入模式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "已启用", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ---- 业务扩展：数据落位与分区管理 ----

    def sink_data(self, entry_id: int, data: dict[str, Any], partition_key: str | None = None) -> dict[str, Any]:
        """将数据按配置写入目标存储。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "落地配置不可用"}
        storage_type = entry.get("目标存储类型", "")
        write_mode = entry.get("写入模式", "")
        partition = partition_key or entry.get("分区策略", "default")
        now = datetime.now(timezone.utc).isoformat()
        payload_rows = store.rows("payload_store")
        pid = max((int(r.get("id", 0)) for r in payload_rows), default=0) + 1
        payload_rows.append({
            "id": pid, "采集批次": f"sink-{entry_id}-{partition}",
            "消息ID": f"sink-msg-{pid}", "原始载荷": str(data),
            "存储时间": now, "压缩后大小": str(len(str(data))),
            "status": "已存储", "pending": True, "abnormal": False,
        })
        return {"ok": True, "message": f"数据已落位至{storage_type}", "partition": partition,
                "write_mode": write_mode, "payload_id": pid}

    def validate_partition_strategy(self, entry_id: int) -> list[str]:
        """校验分区策略与数据源的兼容性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["落地配置不存在"]
        strategy = entry.get("分区策略", "")
        valid_strategies = ["按日期", "按小时", "按数据源", "按业务键", "无分区"]
        if strategy not in valid_strategies:
            errors.append(f"不支持的分区策略: {strategy}，可选: {', '.join(valid_strategies)}")
        try:
            retention = int(str(entry.get("保留天数", "0")))
        except ValueError:
            retention = 0
        if retention <= 0:
            errors.append("保留天数必须大于0")
        return errors

    def route_source_data(self, source_name: str) -> list[str]:
        """根据数据源名称查找匹配的落地配置。"""
        configs = store.rows(self.MODULE)
        return [c.get("配置名称", "") for c in configs if c.get("status") == "已启用" and source_name in str(c.get("配置名称", ""))]

    def cleanup_expired(self) -> dict[str, Any]:
        """清理过期的数据落地产物。"""
        rows = store.rows(self.MODULE)
        now = datetime.now(timezone.utc)
        cleaned = 0
        for row in rows:
            if row.get("status") != "已启用":
                continue
            try:
                retention = int(str(row.get("保留天数", "0")))
            except ValueError:
                continue
            payloads = store.rows("payload_store")
            config_name = row.get("配置名称", "")
            for p in payloads[:]:
                if config_name in str(p.get("采集批次", "")):
                    time_str = str(p.get("存储时间", ""))
                    try:
                        store_time = datetime.fromisoformat(time_str)
                        if (now - store_time).days > retention:
                            p["status"] = "已过期"
                            p["pending"] = False
                            cleaned += 1
                    except ValueError:
                        continue
        return {"ok": True, "expired": cleaned}

    def check_downstream_consumers(self, entry_id: int) -> list[dict[str, Any]]:
        """检查数据落位结果的下游消费者（审计、导出、回放）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        config_name = entry.get("配置名称", "")
        consumers = []
        audit = store.rows("stream_audit")
        for a in audit:
            if config_name in str(a.get("数据源", "")):
                consumers.append({"type": "审计", "id": a.get("id"), "detail": a.get("处理时间")})
                break
        export = store.rows("stream_export")
        for e in export:
            if config_name in str(e.get("数据源", "")):
                consumers.append({"type": "导出", "id": e.get("id"), "detail": e.get("任务名称")})
                break
        replay = store.rows("batch_replay")
        for r in replay:
            if config_name in str(r.get("目标Topic", "")):
                consumers.append({"type": "回放", "id": r.get("id"), "detail": r.get("回放范围")})
                break
        return consumers