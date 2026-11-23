"""批量消息回放领域服务：将历史消息按时间范围回放到指定Topic，用于数据修复和断点续传。

数据链路位置：数据修复层，在审计发现偏差时通过批量回放机制修复遗漏或失败的消息。

补录口径（预检与实际交付共用同一套规则）：
- 只读取原始采集批次：采集批次与目标Topic精确相等，存储时间落在所填时间段内；
- 同名前缀Topic（如 orders 与 orders-eu）互不匹配，窗外消息不参与补录；
- 回放中间产生的副本（replay- 批次 / replayed- 消息）永远不再作为输入；
- 审核员确认后补录名单冻结，同源迟到的消息不插队，只能另开一轮补录；
- 逐条交付回执与目的端副本对账，恢复时先确认已交付数据，保证每条恰好交付一次；
- 审计明细按同一批回执重算，缺口始终等于目的端实际未补录数。
"""
from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

REPLAY_BATCH_PREFIX = "replay-"
REPLAY_MSG_PREFIX = "replayed-"


def _parse_moment(raw: Any) -> datetime | time | None:
    """把填写的时间解析成可比较的时刻，支持日期时间、日期和当天时刻（HH:MM）。"""
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
    if parsed is not None:
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _moment_le(left: Any, right: Any) -> bool:
    """比较两个时间值：同类时刻按时刻比，否则退化为字符串比较，保证结果确定。"""
    a, b = _parse_moment(left), _parse_moment(right)
    if a is not None and b is not None and type(a) is type(b):
        return a <= b
    return str(left) <= str(right)


def in_time_window(moment: Any, start: Any, end: Any) -> bool:
    """存储时间是否落在所填时间段内（含边界），未填的边界视为开口。"""
    if str(start or "").strip() and not _moment_le(start, moment):
        return False
    if str(end or "").strip() and not _moment_le(moment, end):
        return False
    return True


def is_replay_copy(row: dict[str, Any]) -> bool:
    """识别回放中间产生的副本，副本不能再作为回放输入。"""
    return (
        str(row.get("采集批次", "")).startswith(REPLAY_BATCH_PREFIX)
        or str(row.get("消息ID", "")).startswith(REPLAY_MSG_PREFIX)
    )


def original_members(topic: str, start: Any, end: Any) -> list[dict[str, Any]]:
    """按目标Topic和所填时间段读取原始采集批次（精确匹配，排除回放副本）。"""
    members = []
    for row in store.rows("payload_store"):
        if str(row.get("采集批次", "")) != topic:
            continue
        if is_replay_copy(row):
            continue
        if not in_time_window(row.get("存储时间", ""), start, end):
            continue
        members.append(row)
    return members


def destination_copies(topic: str) -> dict[str, dict[str, Any]]:
    """目的端已写入的回放副本：{原始消息ID: 副本行}。"""
    copies: dict[str, dict[str, Any]] = {}
    batch = f"{REPLAY_BATCH_PREFIX}{topic}"
    for row in store.rows("payload_store"):
        if str(row.get("采集批次", "")) != batch:
            continue
        msg_id = str(row.get("消息ID", ""))
        if msg_id.startswith(REPLAY_MSG_PREFIX):
            copies.setdefault(msg_id[len(REPLAY_MSG_PREFIX):], row)
    return copies


def receipted_ids(topic: str) -> set[str]:
    """逐条交付回执的统一视角：目的端副本 ∪ 同Topic全部回放任务的回执。"""
    delivered = set(destination_copies(topic))
    for task in store.rows("batch_replay"):
        if str(task.get("目标Topic", "")) != topic:
            continue
        delivered.update(str(mid) for mid in (task.get("delivery_receipts") or {}))
    return delivered


class BatchReplayService(BaseModuleService):
    MODULE = "batch_replay"
    ENTITY = "回放任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["回放范围", "开始时间", "结束时间", "目标Topic", "回放速率"]
    REQUIRED_FIELDS = ["回放范围", "开始时间", "结束时间"]
    STATUS_ORDER = ["待回放", "回放中", "已完成", "回放中断"]
    ACTION_RULES = {"启动回放": "回放中", "中断回放": "回放中断"}
    NEGATIVE_ACTIONS = []

    # ---- 动作入口：走真实补录流程，而不是只改状态 ----

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启动回放":
            result = self.start_replay(entry_id)
            entry = store.find(self.MODULE, entry_id) if result.get("ok") else None
            return entry, result["message"]
        if action == "中断回放":
            result = self.handle_interrupt(entry_id, "人工中断")
            entry = store.find(self.MODULE, entry_id) if result.get("ok") else None
            return entry, result["message"]
        return super().run_action(entry_id, action)

    # ---- 预检与名单确认 ----

    def validate_replay_range(self, entry_id: int) -> dict[str, Any]:
        """预检：按目标Topic和所填时间段核对原始采集批次，并按交付回执重算未补录明细。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        start_str = str(entry.get("开始时间", "") or "").strip()
        end_str = str(entry.get("结束时间", "") or "").strip()
        if not start_str or not end_str:
            return {"ok": False, "message": "缺少开始或结束时间"}
        if not _moment_le(start_str, end_str):
            return {"ok": False, "message": "开始时间不能晚于结束时间"}
        target = str(entry.get("目标Topic", "") or "").strip()
        if not target:
            return {"ok": False, "message": "缺少目标Topic"}
        members = [str(p.get("消息ID", "")) for p in original_members(target, start_str, end_str)]
        delivered = receipted_ids(target)
        confirmed = [str(m) for m in (entry.get("confirmed_members") or [])]
        scope = confirmed or members
        result: dict[str, Any] = {
            "ok": True,
            "range": f"{start_str}~{end_str}",
            "in_range_count": len(members),
            "members": members,
            "already_replayed": [m for m in scope if m in delivered],
            "unreplayed": [m for m in scope if m not in delivered],
        }
        if confirmed:
            # 名单已确认：同源迟到的消息只提示、不插队，可另开一轮补录。
            result["confirmed_members"] = confirmed
            result["late_arrivals"] = [
                m for m in members if m not in confirmed and m not in delivered
            ]
        return result

    def confirm_members(self, entry_id: int) -> dict[str, Any]:
        """审核员确认补录名单：冻结当前未补录明细，之后同源迟到的消息不再插队。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回放任务不存在"}
        existing = [str(m) for m in (entry.get("confirmed_members") or [])]
        if existing:
            return {"ok": True, "confirmed": existing, "message": "名单已确认，保持不变"}
        check = self.validate_replay_range(entry_id)
        if not check.get("ok"):
            return check
        members = list(check["unreplayed"])
        entry["confirmed_members"] = members
        entry["确认时间"] = datetime.now(timezone.utc).isoformat()
        entry.setdefault("delivery_receipts", {})
        return {"ok": True, "confirmed": members, "message": f"已确认补录名单{len(members)}条"}

    # ---- 交付与恢复 ----

    def start_replay(self, entry_id: int) -> dict[str, Any]:
        """启动/恢复回放：确认名单→对账回执→逐条交付未补录成员→同步审计。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回放任务不存在"}
        confirmed = self.confirm_members(entry_id)
        if not confirmed.get("ok"):
            return confirmed
        members = confirmed["confirmed"]
        if not members:
            entry["status"] = "已完成"
            entry["pending"] = False
            self._sync_audit(entry)
            return {"ok": True, "message": "窗口内消息均已有交付回执，无需补录", "replayed": 0}
        entry["status"] = "回放中"
        entry["pending"] = True
        recovered = self._reconcile_receipts(entry)
        replayed = 0
        while True:
            step = self.deliver_next(entry_id)
            if not step.get("ok") or not step.get("delivered"):
                break
            replayed += 1
        unreplayed = self.unreplayed_detail(entry_id)["unreplayed"]
        message = f"回放已启动，本次交付{replayed}条"
        if recovered:
            message += f"，对账确认{len(recovered)}条"
        if unreplayed:
            message += f"，未补录{len(unreplayed)}条：{'、'.join(unreplayed)}"
            self._sync_audit(entry)
            return {"ok": True, "message": message, "replayed": replayed,
                    "recovered": recovered, "unreplayed": unreplayed}
        final = self.complete_with_audit(entry_id)
        return {"ok": True, "message": message, "replayed": replayed,
                "recovered": recovered, "unreplayed": [],
                "delivered_total": final.get("delivered", 0)}

    def deliver_next(self, entry_id: int, *, lose_receipt: bool = False,
                     fail: bool = False) -> dict[str, Any]:
        """逐条交付下一条未补录成员：先写目的端副本，再登记交付回执。

        lose_receipt / fail 仅用于故障演练：模拟副本已写但回执丢失、或写入失败。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回放任务不存在"}
        confirmed = [str(m) for m in (entry.get("confirmed_members") or [])]
        if not confirmed:
            return {"ok": False, "message": "请先确认补录名单"}
        self._reconcile_receipts(entry)
        receipts = self._receipts(entry)
        pending = [m for m in confirmed if m not in receipts]
        if not pending:
            return {"ok": True, "message": "无待补录消息", "delivered": None}
        mid = pending[0]
        if fail:
            self._sync_audit(entry)
            return {"ok": False, "message": f"消息{mid}写入目的端失败",
                    "delivered": None, "failed": mid}
        source = self._source_row(entry, mid)
        if source is None:
            self._sync_audit(entry)
            return {"ok": False, "message": f"原始消息{mid}已不存在",
                    "delivered": None, "failed": mid}
        copy_row = self._write_copy(entry, source)
        if lose_receipt:
            self._sync_audit(entry)
            return {"ok": True, "message": f"消息{mid}已写目的端但回执丢失",
                    "delivered": mid, "receipt": False}
        receipts[mid] = {
            "交付时间": datetime.now(timezone.utc).isoformat(),
            "目的端副本": str(copy_row.get("消息ID", "")),
            "来源": "逐条交付回执",
        }
        self._sync_audit(entry)
        return {"ok": True, "message": f"消息{mid}已交付", "delivered": mid, "receipt": True}

    def handle_interrupt(self, entry_id: int, reason: str) -> dict[str, Any]:
        """处理回放中断：记录中断原因和当时的未补录明细，等待恢复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        entry["status"] = "回放中断"
        entry["abnormal"] = True
        entry["pending"] = True
        unreplayed = self.unreplayed_detail(entry_id)["unreplayed"]
        now = datetime.now(timezone.utc).isoformat()
        isolation_rows = store.rows("failure_isolation")
        fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
        detail = f"{reason}；未补录{len(unreplayed)}条：{'、'.join(unreplayed)}" if unreplayed else reason
        isolation_rows.append({
            "id": fid, "失败消息ID": f"replay-err-{entry_id}",
            "失败原因": detail, "原始Topic": entry.get("目标Topic", ""),
            "隔离时间": now, "重试次数": "0",
            "status": "待处理", "pending": True, "abnormal": False,
        })
        self._sync_audit(entry)
        return {"ok": True, "message": "回放中断已记录", "isolation_id": fid,
                "unreplayed": unreplayed}

    # ---- 对账与审计 ----

    def unreplayed_detail(self, entry_id: int) -> dict[str, Any]:
        """未补录明细：补录名单 − 逐条交付回执（与审计列表共用同一口径）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回放任务不存在"}
        target = str(entry.get("目标Topic", ""))
        confirmed = [str(m) for m in (entry.get("confirmed_members") or [])]
        if not confirmed:
            check = self.validate_replay_range(entry_id)
            confirmed = list(check.get("unreplayed", [])) if check.get("ok") else []
        delivered = receipted_ids(target)
        unreplayed = [m for m in confirmed if m not in delivered]
        return {"ok": True, "任务": entry_id, "目标Topic": target,
                "confirmed": confirmed, "unreplayed": unreplayed,
                "delivered": [m for m in confirmed if m in delivered]}

    def complete_with_audit(self, entry_id: int) -> dict[str, Any]:
        """回放完成：按交付回执联动审计和偏移量管理，缺口与目的端实际补录数一致。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        already_done = entry.get("status") == "已完成"
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        audit_row = self._sync_audit(entry)
        delivered_count = int(audit_row["成功数"])
        if not already_done:
            target = str(entry.get("目标Topic", ""))
            for row in store.rows("offset_manager"):
                if str(row.get("Topic/Queue", "")) != target:
                    continue
                try:
                    current = int(row.get("当前Offset", "0"))
                except ValueError:
                    continue
                row["当前Offset"] = str(current + delivered_count)
                row["消费延迟"] = "0"
        return {"ok": True, "message": "回放完成已审计", "delivered": delivered_count}

    # ---- 内部工具 ----

    @staticmethod
    def _receipts(entry: dict[str, Any]) -> dict[str, Any]:
        return entry.setdefault("delivery_receipts", {})

    def _reconcile_receipts(self, entry: dict[str, Any]) -> list[str]:
        """恢复前先对账：目的端已有副本但回执缺失的，补确认回执且不再重发。"""
        target = str(entry.get("目标Topic", ""))
        copies = destination_copies(target)
        receipts = self._receipts(entry)
        recovered = []
        for mid in [str(m) for m in (entry.get("confirmed_members") or [])]:
            if mid in receipts or mid not in copies:
                continue
            receipts[mid] = {
                "交付时间": datetime.now(timezone.utc).isoformat(),
                "目的端副本": str(copies[mid].get("消息ID", "")),
                "来源": "目的端对账确认",
            }
            recovered.append(mid)
        return recovered

    def _source_row(self, entry: dict[str, Any], mid: str) -> dict[str, Any] | None:
        """按消息ID取回原始采集行（精确Topic、排除回放副本）。"""
        target = str(entry.get("目标Topic", ""))
        for row in store.rows("payload_store"):
            if str(row.get("采集批次", "")) != target or is_replay_copy(row):
                continue
            if str(row.get("消息ID", "")) == mid:
                return row
        return None

    def _write_copy(self, entry: dict[str, Any], source_row: dict[str, Any]) -> dict[str, Any]:
        """把一条原始消息写入目的端（回放副本），返回副本行。"""
        target = str(entry.get("目标Topic", ""))
        rows = store.rows("payload_store")
        now = datetime.now(timezone.utc).isoformat()
        payload = str(source_row.get("原始载荷", ""))
        copy_row = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "采集批次": f"{REPLAY_BATCH_PREFIX}{target}",
            "消息ID": f"{REPLAY_MSG_PREFIX}{source_row.get('消息ID', '')}",
            "原始载荷": payload,
            "存储时间": now,
            "压缩后大小": str(len(payload)),
            "来源任务": str(entry.get("id", "")),
            "status": "已存储", "pending": True, "abnormal": False,
        }
        rows.append(copy_row)
        return copy_row

    def _sync_audit(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把逐条交付回执汇总成审计明细（按关联任务upsert），缺口=已确认−已交付。"""
        detail = self.unreplayed_detail(int(entry.get("id", 0)))
        confirmed = detail["confirmed"]
        delivered = detail["delivered"]
        unreplayed = detail["unreplayed"]
        now = datetime.now(timezone.utc).isoformat()
        audit_rows = store.rows("stream_audit")
        row = None
        for candidate in audit_rows:
            if str(candidate.get("关联回放任务", "")) == str(entry.get("id", "")):
                row = candidate
                break
        if row is None:
            row = {
                "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
                "关联回放任务": str(entry.get("id", "")),
            }
            audit_rows.append(row)
        row.update({
            "处理时间": now,
            "数据源": str(entry.get("目标Topic", "")),
            "消息总数": str(len(confirmed)),
            "成功数": str(len(delivered)),
            "失败原因分布": (
                f"未补录{len(unreplayed)}条：{'、'.join(unreplayed)}" if unreplayed else ""
            ),
            "status": "偏差超标" if unreplayed else "正常",
            "pending": bool(unreplayed),
            "abnormal": bool(unreplayed),
        })
        return row
