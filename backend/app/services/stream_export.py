"""流式数据导出领域服务：按时间范围和条件将流式处理结果导出为离线文件。

数据链路位置：数据消费层，将处理完成的数据产物导出为文件供下游分析和离线使用。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamExportService(BaseModuleService):
    MODULE = "stream_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["任务名称", "数据源", "导出格式", "时间范围", "文件大小"]
    REQUIRED_FIELDS = ["任务名称", "数据源", "导出格式"]
    STATUS_ORDER = ["排队中", "导出中", "已完成", "导出失败"]
    ACTION_RULES = {"开始导出": "导出中", "重试导出": "导出中"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：导出执行与文件管理 ----

    def execute_export(self, entry_id: int) -> dict[str, Any]:
        """执行导出任务：读取数据源并生成指定格式文件。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "导出任务不存在"}
        source = entry.get("数据源", "")
        fmt = entry.get("导出格式", "json")
        now = datetime.now(timezone.utc).isoformat()
        entry["status"] = "导出中"
        entry["pending"] = True
        data_rows = []
        for module in ("payload_store", "api_logger", "stream_audit"):
            for row in store.rows(module):
                if source in str(row):
                    data_rows.append(row)
        content = str(data_rows)
        entry["文件大小"] = str(len(content))
        entry["status"] = "已完成"
        entry["pending"] = False
        payload_rows = store.rows("payload_store")
        pid = max((int(r.get("id", 0)) for r in payload_rows), default=0) + 1
        payload_rows.append({
            "id": pid, "采集批次": f"export-{entry_id}",
            "消息ID": f"export-file-{pid}",
            "原始载荷": content, "存储时间": now,
            "压缩后大小": str(len(content)), "status": "已存储",
            "pending": True, "abnormal": False,
        })
        return {"ok": True, "message": "导出完成", "rows": len(data_rows), "size": len(content)}

    def validate_export_params(self, entry_id: int) -> list[str]:
        """校验导出参数的有效性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["任务不存在"]
        fmt = entry.get("导出格式", "")
        if fmt not in ("csv", "json", "parquet", "avro"):
            errors.append(f"不支持的导出格式: {fmt}")
        source = entry.get("数据源", "")
        has_data = False
        for module in ("payload_store", "api_logger", "stream_audit"):
            for row in store.rows(module):
                if source in str(row):
                    has_data = True
                    break
            if has_data:
                break
        if not has_data:
            errors.append(f"数据源 '{source}' 无可用数据")
        return errors

    def link_to_audit(self, entry_id: int) -> dict[str, Any]:
        """导出结果与审计记录关联。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        entry["abnormal"] = False
        source = entry.get("数据源", "")
        now = datetime.now(timezone.utc).isoformat()
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": f"export:{source}",
            "消息总数": "1", "成功数": "1", "失败原因分布": "",
            "status": "正常", "pending": True, "abnormal": False,
        })
        return {"ok": True, "audit_id": aid}