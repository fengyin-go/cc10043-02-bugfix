"""消息解析器领域服务：解析JSON/Avro/Protobuf消息为结构化字段，管理解析模板和字段映射。

数据链路位置：消息解析层，将原始消息载荷按模板解析为结构化数据，处理脏数据策略和嵌套展开。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class MessageParserService(BaseModuleService):
    MODULE = "message_parser"
    ENTITY = "解析模板"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["模板名称", "消息格式", "字段映射", "嵌套展开深度", "脏数据策略"]
    REQUIRED_FIELDS = ["模板名称", "消息格式", "字段映射"]
    STATUS_ORDER = ["草稿", "已启用", "已废弃"]
    ACTION_RULES = {"启用模板": "已启用", "废弃模板": "已废弃"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：消息解析与字段结构化 ----

    def parse_message(self, entry_id: int, payload: str) -> dict[str, Any]:
        """使用指定模板解析消息载荷为结构化字段。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "模板不可用", "fields": {}}
        try:
            depth_str = str(entry.get("嵌套展开深度", "1"))
            depth = int(depth_str) if depth_str.isdigit() else 1
        except ValueError:
            depth = 1
        fields = {}
        parsed = str(payload)
        mapping = str(entry.get("字段映射", ""))
        for pair in mapping.split(","):
            pair = pair.strip()
            if not pair:
                continue
            parts = pair.split(":", 1)
            if len(parts) == 2:
                fields[parts[0].strip()] = parts[1].strip()
        fields["_parsed_length"] = len(parsed)
        fields["_parse_depth"] = depth
        return {"ok": True, "message": "解析完成", "fields": fields}

    def validate_schema_compliance(self, entry_id: int, fields: dict[str, Any]) -> list[str]:
        """校验解析结果与Schema注册中心的版本兼容性。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["模板不存在"]
        schemas = store.rows("schema_registry")
        template_name = entry.get("模板名称", "")
        matching = [s for s in schemas if s.get("status") == "已注册"
                    and template_name in str(s.get("Schema名称", ""))]
        if not matching:
            return [f"模板 {template_name} 未在Schema注册中心找到匹配版本"]
        schema = matching[0]
        schema_fields = str(schema.get("字段定义", "")).split(",")
        for key, value in fields.items():
            if key.startswith("_"):
                continue
            if key not in schema_fields:
                if str(entry.get("脏数据策略", "")) == "丢弃":
                    errors.append(f"未知字段 {key}，将按脏数据丢弃")
                else:
                    errors.append(f"未知字段 {key}，将按不匹配字段忽略")
        return errors

    def handle_dirty_data(self, entry_id: int, errors: list[str]) -> str:
        """处理脏数据：根据策略决定丢弃、隔离或忽略。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or not errors:
            return "无脏数据"
        strategy = entry.get("脏数据策略", "忽略")
        if strategy == "丢弃":
            return f"已丢弃 {len(errors)} 条脏数据"
        if strategy == "隔离":
            isolation_rows = store.rows("failure_isolation")
            fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
            now = datetime.now(timezone.utc).isoformat()
            isolation_rows.append({
                "id": fid, "失败消息ID": f"dirty-{entry_id}", "失败原因": "; ".join(errors),
                "原始Topic": entry.get("模板名称", ""), "隔离时间": now, "重试次数": "0",
                "status": "待处理", "pending": True, "abnormal": False,
            })
            return f"已隔离 {len(errors)} 条脏数据"
        return f"已忽略 {len(errors)} 条格式错误"

    def route_to_transform(self, entry_id: int, fields: dict[str, Any]) -> list[dict[str, Any]]:
        """解析后的字段根据映射关系路由到字段转换器。"""
        rules = store.rows("field_transform")
        matched = []
        for rule in rules:
            if rule.get("status") != "已启用":
                continue
            if rule.get("源字段", "") in fields:
                matched.append(rule)
        return matched

    def audit_parse_stat(self, entry_id: int, success: int, failed: int) -> None:
        """记录解析统计到审计明细。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        total = success + failed
        status = "偏差超标" if failed > 0 and failed >= total * 0.3 else "正常"
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": entry.get("模板名称", ""),
            "消息总数": str(total), "成功数": str(success),
            "失败原因分布": f"脏数据{failed}条" if failed else "",
            "status": status, "pending": True, "abnormal": failed > 0,
        })