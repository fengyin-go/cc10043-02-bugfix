"""消息载荷存储领域服务：存储消息的原始载荷副本，支持事后审计和回放，管理压缩和过期策略。

数据链路位置：数据归档层，保存原始消息载荷作为全链数据审计和回放的事实来源。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class PayloadStoreService(BaseModuleService):
    MODULE = "payload_store"
    ENTITY = "原始载荷"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["采集批次", "消息ID", "原始载荷", "存储时间", "压缩后大小"]
    REQUIRED_FIELDS = ["采集批次", "消息ID", "原始载荷"]
    STATUS_ORDER = ["已存储", "已压缩", "已过期"]
    ACTION_RULES = {"批量压缩": "已存储", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：载荷管理与归档 ----

    def batch_compress_payloads(self, batch_name: str | None = None) -> dict[str, Any]:
        """批量压缩指定批次的载荷。"""
        rows = store.rows(self.MODULE)
        if batch_name:
            rows = [r for r in rows if batch_name in str(r.get("采集批次", ""))]
        compressed = 0
        for row in rows:
            if row.get("status") != "已存储":
                continue
            payload = str(row.get("原始载荷", ""))
            if len(payload) > 100:
                row["原始载荷"] = payload[:100] + "...[compressed]"
                row["压缩后大小"] = str(len(row["原始载荷"]))
            row["status"] = "已压缩"
            compressed += 1
        return {"ok": True, "compressed": compressed}

    def get_replay_source(self, topic: str, limit: int = 100) -> list[dict[str, Any]]:
        """从载荷存储提取回放数据源。"""
        rows = store.rows(self.MODULE)
        matched = [r for r in rows if topic in str(r.get("采集批次", "")) and r.get("status") in ("已存储", "已压缩")]
        return matched[:limit]

    def validate_batch_integrity(self, batch_name: str) -> dict[str, Any]:
        """校验采集批次的存储完整性。"""
        rows = store.rows(self.MODULE)
        batch_rows = [r for r in rows if batch_name in str(r.get("采集批次", ""))]
        total = len(batch_rows)
        stored = len([r for r in batch_rows if r.get("status") == "已存储"])
        compressed = len([r for r in batch_rows if r.get("status") == "已压缩"])
        expired = len([r for r in batch_rows if r.get("status") == "已过期"])
        return {"batch": batch_name, "total": total, "stored": stored,
                "compressed": compressed, "expired": expired, "intact": total > 0 and expired == 0}

    def route_to_watermark(self, batch_name: str) -> str | None:
        """载荷批次路由到水位追踪。"""
        batch_rows = [r for r in store.rows(self.MODULE) if batch_name in str(r.get("采集批次", ""))]
        if not batch_rows:
            return None
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        watermark_rows.append({
            "id": wid, "数据源": batch_name,
            "水位字段": "载荷计数", "当前水位": str(len(batch_rows)),
            "上次推进时间": now, "延迟分钟数": "0",
            "status": "正常水位", "pending": True, "abnormal": False,
        })
        return now

    def export_snapshot(self, batch_name: str) -> dict[str, Any]:
        """导出载荷快照用于离线分析。"""
        payloads = self.get_replay_source(batch_name, limit=500)
        export_rows = store.rows("stream_export")
        eid = max((int(r.get("id", 0)) for r in export_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        export_rows.append({
            "id": eid, "任务名称": f"载荷快照-{batch_name}",
            "数据源": batch_name, "导出格式": "json",
            "时间范围": now, "文件大小": str(len(str(payloads))),
            "status": "排队中", "pending": True, "abnormal": False,
        })
        return {"ok": True, "export_id": eid, "payload_count": len(payloads)}