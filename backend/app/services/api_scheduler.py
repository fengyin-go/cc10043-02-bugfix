"""API调用调度领域服务：按Cron表达式调度API调用、管理执行计划和调用链路编排。

数据链路位置：调度与编排层，驱动API调用计划的定时触发和调用链路的编排执行。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ApiSchedulerService(BaseModuleService):
    MODULE = "api_scheduler"
    ENTITY = "调用计划"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["计划名称", "关联API", "Cron表达式", "下次执行时间", "上次状态"]
    REQUIRED_FIELDS = ["计划名称", "关联API", "Cron表达式"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"立即执行": "执行中", "暂停计划": "待执行"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：调度编排与调用链路 ----

    def trigger_execution(self, entry_id: int) -> dict[str, Any]:
        """触发计划的立即执行，记录执行开始时间。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "调用计划不存在"}
        now = datetime.now(timezone.utc).isoformat()
        entry["上次状态"] = "执行中"
        entry["status"] = "执行中"
        entry["下次执行时间"] = now
        return {"ok": True, "message": "已触发执行", "started_at": now}

    def validate_schedule(self, entry_id: int) -> list[str]:
        """校验Cron表达式和调度参数的合法性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["计划不存在"]
        cron = entry.get("Cron表达式", "")
        if not cron:
            errors.append("Cron表达式不能为空")
        parts = cron.split()
        if len(parts) < 5:
            errors.append("Cron表达式格式无效，需5个字段")
        api_connectors = store.rows("api_connector")
        api_name = entry.get("关联API", "")
        matched = [c for c in api_connectors if api_name in str(c.get("连接器名称", ""))]
        if api_name and not matched:
            errors.append(f"关联API '{api_name}' 未找到可用连接器")
        return errors

    def route_to_rate_guard(self, entry_id: int) -> str | None:
        """调度计划匹配频控规则，防止过载。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        api_name = entry.get("关联API", "")
        rules = store.rows("rate_guard")
        for rule in rules:
            if rule.get("status") == "已生效" and api_name in rule.get("限流维度", ""):
                return rule.get("规则名称", "")
        return None

    def handle_execution_failure(self, entry_id: int, reason: str) -> dict[str, Any]:
        """执行失败时更新状态并路由到失败隔离区。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "计划不存在"}
        entry["status"] = "执行失败"
        entry["上次状态"] = f"失败: {reason}"
        now = datetime.now(timezone.utc).isoformat()
        isolation_rows = store.rows("failure_isolation")
        fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
        isolation_rows.append({
            "id": fid, "失败消息ID": f"sched-err-{entry_id}",
            "失败原因": reason, "原始Topic": entry.get("关联API", ""),
            "隔离时间": now, "重试次数": "0",
            "status": "待处理", "pending": True, "abnormal": False,
        })
        return {"ok": True, "message": "已记录失败隔离", "isolation_id": fid}

    def complete_with_audit(self, entry_id: int, record_count: int) -> None:
        """执行完成时归档审计和数据落地配置检查。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        entry["status"] = "已完成"
        entry["上次状态"] = f"完成: {record_count}条"
        entry["pending"] = False
        now = datetime.now(timezone.utc).isoformat()
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": entry.get("关联API", ""),
            "消息总数": str(record_count), "成功数": str(record_count),
            "失败原因分布": "", "status": "正常", "pending": True, "abnormal": False,
        })

    def check_data_sink(self, entry_id: int) -> list[str]:
        """检查数据落地配置是否就绪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        sinks = store.rows("data_sink")
        ready = [s.get("配置名称", "") for s in sinks if s.get("status") == "已启用"]
        return ready