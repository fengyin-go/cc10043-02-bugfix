"""水位追踪领域服务：记录每个数据源的批次时间戳水位和消费进度，管理水位推进和停滞告警。

数据链路位置：进度监控层，跟踪全部数据源的水位推进状态，是审计偏差判断的核心依据。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WatermarkTrackerService(BaseModuleService):
    MODULE = "watermark_tracker"
    ENTITY = "水位记录"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["数据源", "水位字段", "当前水位", "上次推进时间", "延迟分钟数"]
    REQUIRED_FIELDS = ["数据源", "水位字段", "当前水位"]
    STATUS_ORDER = ["正常水位", "水位停滞", "水位倒退"]
    ACTION_RULES = {"手动推进水位": "水位停滞", "重置水位": "水位倒退"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：水位推进与延迟审计 ----

    def advance_watermark(self, entry_id: int, new_value: str) -> dict[str, Any]:
        """推进当前水位并更新延迟分钟数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "水位记录不存在"}
        now = datetime.now(timezone.utc)
        entry["上次推进时间"] = now.isoformat()
        old_value = entry.get("当前水位", "")
        if old_value > new_value:
            entry["status"] = "水位倒退"
            entry["abnormal"] = True
            return {"ok": False, "message": f"水位倒退拒绝: {old_value} → {new_value}"}
        entry["当前水位"] = new_value
        try:
            last_time = datetime.fromisoformat(str(entry.get("上次推进时间", "")))
        except ValueError:
            last_time = now
        delay_minutes = int((now - last_time).total_seconds() / 60)
        entry["延迟分钟数"] = str(delay_minutes)
        if delay_minutes > 30:
            entry["status"] = "水位停滞"
            entry["abnormal"] = True
        else:
            entry["status"] = "正常水位"
            entry["abnormal"] = False
        return {"ok": True, "message": f"水位推进至 {new_value}", "delay": delay_minutes}

    def validate_watermark_chain(self, source: str) -> dict[str, Any]:
        """校验同一数据源的水位链是否连续推进。"""
        rows = store.rows(self.MODULE)
        chain = sorted(
            [r for r in rows if source in str(r.get("数据源", ""))],
            key=lambda r: str(r.get("上次推进时间", "")))
        regressions = []
        for i in range(1, len(chain)):
            if chain[i].get("当前水位", "0") < chain[i - 1].get("当前水位", "0"):
                regressions.append(i)
        return {
            "source": source,
            "records": len(chain),
            "regressions": regressions,
            "latest": chain[-1].get("当前水位") if chain else None,
        }

    def detect_stagnant_sources(self, threshold_minutes: int = 60) -> list[dict[str, Any]]:
        """检测停滞数据源并生成告警列表。"""
        rows = store.rows(self.MODULE)
        now = datetime.now(timezone.utc)
        stagnant = []
        for row in rows:
            time_str = str(row.get("上次推进时间", ""))
            try:
                last_time = datetime.fromisoformat(time_str)
                delay = int((now - last_time).total_seconds() / 60)
            except (ValueError, TypeError):
                delay = 9999
            if delay > threshold_minutes:
                row["status"] = "水位停滞"
                row["延迟分钟数"] = str(delay)
                row["abnormal"] = True
                stagnant.append({"source": row.get("数据源", ""), "delay": delay})
        return stagnant

    def route_to_alarm(self, entry_id: int) -> str | None:
        """水位停滞时路由到吞吐量告警。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "水位停滞":
            return None
        alarms = store.rows("throughput_alarm")
        source = entry.get("数据源", "")
        for alarm in alarms:
            if alarm.get("status") == "已生效" and source in alarm.get("监控数据源", ""):
                alarm["abnormal"] = True
                return alarm.get("通知方式", "")
        return None

    def compare_with_audit(self, entry_id: int) -> dict[str, Any]:
        """将水位数据与审计明细对比，计算接入量与落地量偏差。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"deviation": 0}
        source = entry.get("数据源", "")
        audit_rows = store.rows("stream_audit")
        matched = [a for a in audit_rows if source in str(a.get("数据源", ""))]
        total_success = sum(int(a.get("成功数", "0")) if str(a.get("成功数", "")).isdigit() else 0 for a in matched)
        try:
            watermark = int(entry.get("当前水位", "0"))
        except ValueError:
            watermark = 0
        return {"source": source, "total_success_in_audit": total_success, "current_watermark": watermark,
                "deviation": abs(watermark - total_success)}

    def archive_snapshot(self, entry_id: int) -> str:
        """归档水位快照到载荷存储。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ""
        now = datetime.now(timezone.utc).isoformat()
        payload_rows = store.rows("payload_store")
        pid = max((int(r.get("id", 0)) for r in payload_rows), default=0) + 1
        payload_rows.append({
            "id": pid, "采集批次": f"watermark-snapshot-{entry_id}",
            "消息ID": f"snap-{entry_id}", "原始载荷": str(entry),
            "存储时间": now, "压缩后大小": str(len(str(entry))),
            "status": "已存储", "pending": True, "abnormal": False,
        })
        return now