"""Schema注册中心领域服务：注册和管理消息数据的Schema定义和版本兼容性。

数据链路位置：元数据管理层，为消息解析和字段转换提供Schema版本定义，确保数据格式向下兼容。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SchemaRegistryService(BaseModuleService):
    MODULE = "schema_registry"
    ENTITY = "Schema版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["Schema名称", "版本号", "字段定义", "兼容模式", "注册时间"]
    REQUIRED_FIELDS = ["Schema名称", "版本号", "字段定义"]
    STATUS_ORDER = ["草稿", "已注册", "已废弃"]
    ACTION_RULES = {"注册版本": "已注册", "废弃版本": "已废弃"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：Schema版本管理与兼容性检查 ----

    def check_compatibility(self, entry_id: int, new_fields: list[str]) -> dict[str, Any]:
        """检查新字段定义与已注册Schema版本的兼容性。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已注册":
            return {"compatible": False, "message": "Schema未注册"}
        existing = str(entry.get("字段定义", "")).split(",")
        existing = [f.strip() for f in existing if f.strip()]
        mode = entry.get("兼容模式", "向前兼容")
        added = set(new_fields) - set(existing)
        removed = set(existing) - set(new_fields)
        if mode == "向前兼容" and removed:
            return {"compatible": False, "message": "向前兼容不允许删除已有字段", "removed": list(removed)}
        if mode == "向后兼容" and added:
            return {"compatible": False, "message": "向后兼容不允许新增必填字段", "added": list(added)}
        return {"compatible": True, "message": "版本兼容", "added": list(added), "removed": list(removed)}

    def register_new_version(self, schema_name: str, version: str, fields: str, mode: str) -> dict[str, Any]:
        """注册新的Schema版本。"""
        existing = store.rows(self.MODULE)
        same_version = [e for e in existing
                        if e.get("Schema名称") == schema_name and e.get("版本号") == version]
        if same_version:
            return {"ok": False, "message": f"版本 {version} 已存在"}
        sid = max((int(r.get("id", 0)) for r in existing), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        entry = {
            "id": sid, "Schema名称": schema_name, "版本号": version,
            "字段定义": fields, "兼容模式": mode, "注册时间": now,
            "status": "已注册", "pending": True, "abnormal": False,
        }
        existing.append(entry)
        return {"ok": True, "schema_id": sid, "version": version}

    def validate_parser_binding(self, schema_name: str) -> dict[str, Any]:
        """校验Schema与消息解析器模板的绑定关系。"""
        parsers = store.rows("message_parser")
        bound = [p.get("模板名称", "") for p in parsers if schema_name in str(p.get("模板名称", ""))]
        return {"schema_name": schema_name, "bound_parsers": bound, "bound": len(bound) > 0}

    def link_to_transform_rules(self, schema_name: str) -> list[str]:
        """查找与Schema字段关联的转换规则。"""
        entry = store.find(self.MODULE, min((int(r.get("id", 0)) for r in store.rows(self.MODULE)
                                            if r.get("Schema名称") == schema_name), default=0))
        if entry is None:
            return []
        fields = str(entry.get("字段定义", "")).split(",")
        fields = [f.strip() for f in fields if f.strip()]
        transform_rules = store.rows("field_transform")
        linked = []
        for rule in transform_rules:
            if rule.get("status") == "已启用" and rule.get("源字段", "") in fields:
                linked.append(rule.get("规则名称", ""))
        return linked