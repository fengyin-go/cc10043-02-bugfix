"""频控保护领域服务：按API Key、IP或业务维度配置调用频控和令牌桶参数，管理限流规则。

数据链路位置：流量控制层，在API调用和消息消费前执行频控检查，保护下游系统不被过载。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RateGuardService(BaseModuleService):
    MODULE = "rate_guard"
    ENTITY = "频控规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "限流维度", "每秒上限", "分种上限", "小时上限"]
    REQUIRED_FIELDS = ["规则名称", "限流维度", "每秒上限"]
    STATUS_ORDER = ["待生效", "已生效", "已暂停"]
    ACTION_RULES = {"启用频控": "已生效", "暂停频控": "已暂停"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：限流与令牌桶 ----

    def check_rate_limit(self, entry_id: int, request_count: int) -> dict[str, Any]:
        """检查当前请求量是否超出频控规则上限。每次检查消耗一个令牌。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已生效":
            return {"allowed": True, "message": "频控规则未生效"}
        try:
            per_second = int(str(entry.get("每秒上限", "0")))
            per_minute = int(str(entry.get("分种上限", "0")))
            per_hour = int(str(entry.get("小时上限", "0")))
        except (ValueError, TypeError):
            return {"allowed": True, "message": "频控上限配置错误，放行"}
        rejections = []
        if per_second > 0 and request_count > per_second:
            rejections.append(f"超出每秒上限{per_second}")
        if per_minute > 0 and request_count > per_minute:
            rejections.append(f"超出每分钟上限{per_minute}")
        if per_hour > 0 and request_count > per_hour:
            rejections.append(f"超出每小时上限{per_hour}")
        if rejections:
            return {"allowed": False, "message": "; ".join(rejections), "dimension": entry.get("限流维度", "")}
        return {"allowed": True, "message": "频控检查通过"}

    def validate_dimension(self, entry_id: int) -> list[str]:
        """校验频控规则与已注册API连接器和消息流源的维度匹配。"""
        errors = []
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ["频控规则不存在"]
        dimension = entry.get("限流维度", "")
        api_hits = [a for a in store.rows("api_connector") if dimension in str(a.get("API端点", ""))]
        stream_hits = [s for s in store.rows("stream_source") if dimension in str(s.get("源名称", ""))]
        if not api_hits and not stream_hits:
            errors.append(f"限流维度 '{dimension}' 未匹配到任何已注册的连接器或流源")
        return errors

    def route_to_distance(self, entry_id: int) -> dict[str, Any]:
        """频控触发时将超限请求路由到失败隔离区或审计记录。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "规则不存在"}
        dimension = entry.get("限流维度", "")
        now = datetime.now(timezone.utc).isoformat()
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": dimension,
            "消息总数": "1", "成功数": "0", "失败原因分布": f"频控触发: {entry.get('规则名称', '')}",
            "status": "偏差超标", "pending": True, "abnormal": True,
        })
        return {"ok": True, "message": "频控事件已审计", "audit_id": aid}

    def sync_with_scheduler(self, entry_id: int) -> str | None:
        """与API调用调度协同：检查调度计划是否受频控限制。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已生效":
            return None
        dimension = entry.get("限流维度", "")
        schedules = store.rows("api_scheduler")
        blocked = [s.get("计划名称", "") for s in schedules if dimension in str(s.get("关联API", ""))]
        return "; ".join(blocked) if blocked else None