"""失败隔离区领域服务：将解析和校验失败的消息放入隔离区供人工或延迟重试，管理重试和丢弃策略。

数据链路位置：异常处理层，捕获处理链中所有失败消息，提供重试入口和批量处置能力。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class FailureIsolationService(BaseModuleService):
    MODULE = "failure_isolation"
    ENTITY = "隔离记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["失败消息ID", "失败原因", "原始Topic", "隔离时间", "重试次数"]
    REQUIRED_FIELDS = ["失败消息ID", "失败原因", "原始Topic"]
    STATUS_ORDER = ["待处理", "重试中", "已丢弃", "已恢复"]
    ACTION_RULES = {"立即重试": "待处理", "批量丢弃": "已丢弃"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：失败处理与重试 ----

    def retry_message(self, entry_id: int) -> dict[str, Any]:
        """对隔离消息执行重试，更新重试次数并记录结果。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "隔离记录不存在"}
        try:
            retries = int(str(entry.get("重试次数", "0")))
        except ValueError:
            retries = 0
        if retries >= 5:
            return {"ok": False, "message": "已达最大重试次数5次，请考虑丢弃或人工处理"}
        entry["重试次数"] = str(retries + 1)
        entry["status"] = "重试中"
        reason = entry.get("失败原因", "")
        if "解析" in reason and "Schema" in reason:
            entry["status"] = "已恢复"
            entry["abnormal"] = False
            return {"ok": True, "message": "重试成功，消息已恢复", "retries": retries + 1}
        entry["status"] = "待处理"
        return {"ok": True, "message": f"重试第{retries + 1}次完成，继续位于隔离区", "retries": retries + 1}

    def batch_discard(self, reason_filter: str | None = None) -> dict[str, Any]:
        """批量丢弃匹配条件的隔离记录。"""
        rows = store.rows(self.MODULE)
        discarded = []
        for row in rows[:]:
            if row.get("status") != "待处理":
                continue
            if reason_filter and reason_filter not in str(row.get("失败原因", "")):
                continue
            row["status"] = "已丢弃"
            row["abnormal"] = False
            row["pending"] = False
            discarded.append(row.get("失败消息ID", ""))
        now = datetime.now(timezone.utc).isoformat()
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": "failure_isolation",
            "消息总数": str(len(discarded)), "成功数": str(len(discarded)),
            "失败原因分布": "", "status": "正常", "pending": True, "abnormal": False,
        })
        return {"ok": True, "message": f"已批量丢弃{len(discarded)}条", "discarded": discarded}

    def validate_isolation_policy(self, entry_id: int) -> dict[str, Any]:
        """校验隔离记录与续采策略的关联。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "记录不存在"}
        topic = entry.get("原始Topic", "")
        policies = store.rows("resume_policy")
        matching = [p.get("规则名称", "") for p in policies if p.get("status") == "已启用"]
        return {"ok": True, "topic": topic, "available_policies": matching}

    def export_failed_msgs(self, entry_id: int, export_format: str = "csv") -> dict[str, Any]:
        """将隔离失败的原始消息导出为指定格式。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "记录不存在"}
        now = datetime.now(timezone.utc).isoformat()
        export_rows = store.rows("stream_export")
        eid = max((int(r.get("id", 0)) for r in export_rows), default=0) + 1
        export_rows.append({
            "id": eid, "任务名称": f"失败导出-{entry_id}",
            "数据源": entry.get("原始Topic", ""), "导出格式": export_format,
            "时间范围": now, "文件大小": "0",
            "status": "排队中", "pending": True, "abnormal": False,
        })
        return {"ok": True, "message": "导出任务已创建", "export_id": eid}

    def calculate_recovery_rate(self) -> dict[str, Any]:
        """计算隔离区的恢复率统计。"""
        rows = store.rows(self.MODULE)
        total = len(rows)
        recovered = len([r for r in rows if r.get("status") == "已恢复"])
        discarded = len([r for r in rows if r.get("status") == "已丢弃"])
        pending = len([r for r in rows if r.get("status") == "待处理"])
        return {"total": total, "recovered": recovered, "discarded": discarded, "pending": pending,
                "recovery_rate": round(recovered / total * 100, 1) if total else 0}