"""回放预检与交付的端到端核对脚本（直接驱动领域服务，内存仓库逐场景快照还原）。

覆盖任务书要求的四类场景：
1. 预检计数：窗外消息、同名前缀Topic、上次补录副本都不得选中；
2. 实际交付：目的端恰好收到确认名单的副本，审计缺口与目的端一致，副本不再作为输入；
3. 丢回执中断恢复：A 写目的端但回执丢失，恢复时对账确认不重发，A/B 各交付一次，
   同源迟到的 C 不插队、另开一轮补录；
4. 失败重提：审计列表与补录入口共用回执重算未补录明细，重新提交只派发失败成员。

运行：cd backend && .venv/bin/python tests/test_batch_replay_flow.py
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.batch_replay import BatchReplayService
from app.services.stream_audit import StreamAuditService
from app.store import store

svc = BatchReplayService()
audit_svc = StreamAuditService()

TOPIC = "orders"
START = "2026-11-23 12:00"
END = "2026-11-23 13:00"


def snapshot() -> dict:
    return copy.deepcopy(store._tables)


def restore(snap: dict) -> None:
    store._tables.clear()
    store._tables.update(copy.deepcopy(snap))


def put_payload(mid: str, batch: str, stored_at: str) -> None:
    rows = store.rows("payload_store")
    payload = f"payload-of-{mid}"
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "采集批次": batch, "消息ID": mid, "原始载荷": payload,
        "存储时间": stored_at, "压缩后大小": str(len(payload)),
        "status": "已存储", "pending": True, "abnormal": False,
    })


def make_task(topic: str = TOPIC, start: str = START, end: str = END) -> int:
    entry, missing = svc.create_entry({
        "回放范围": f"{topic} {start}~{end} 补录",
        "开始时间": start, "结束时间": end,
        "目标Topic": topic, "回放速率": "100",
    })
    assert not missing and entry, missing
    return int(entry["id"])


def audit_row_for(task_id: int) -> dict:
    rows = [
        r for r in store.rows("stream_audit")
        if str(r.get("关联回放任务", "")) == str(task_id)
    ]
    assert rows, f"任务{task_id}缺少审计明细"
    return rows[-1]


def copies_of(mid: str) -> list[dict]:
    return [
        r for r in store.rows("payload_store")
        if r.get("采集批次") == f"replay-{TOPIC}" and r.get("消息ID") == f"replayed-{mid}"
    ]


def scenario_precheck() -> None:
    """场景1：预检只数窗口内的原始消息，窗外/同名前缀/补录副本都不得选中。"""
    snap = snapshot()
    try:
        put_payload("A", "orders", "2026-11-23 12:00")
        put_payload("B", "orders", "2026-11-23 12:30")
        put_payload("X", "orders", "2026-11-23 11:30")        # 窗外消息
        put_payload("Y", "orders-eu", "2026-11-23 12:15")     # 同名前缀Topic
        put_payload("replayed-A", "replay-orders", "2026-11-23 12:20")  # 上次补录副本
        tid = make_task()
        pre = svc.validate_replay_range(tid)
        assert pre["ok"], pre
        # 副本/窗外/前缀都不得作为输入：候选成员只有窗口内的 A、B
        assert pre["in_range_count"] == 2, pre
        assert pre["members"] == ["A", "B"], pre
        # 上次补录副本留在目的端 → A 按回执视角视为已交付，未补录只剩 B
        assert pre["already_replayed"] == ["A"], pre
        assert pre["unreplayed"] == ["B"], pre
        # 预检是只读的，不得冻结名单
        assert store.find("batch_replay", tid).get("confirmed_members") is None
        print("PASS 场景1 预检计数：窗外消息/同名前缀Topic/上次补录副本均未选中，in_range=2 (A,B)")
    finally:
        restore(snap)


def scenario_delivery() -> None:
    """场景2：实际交付只派发确认名单，审计与目的端一致，副本不再作为输入。"""
    snap = snapshot()
    try:
        put_payload("A", "orders", "2026-11-23 12:00")
        put_payload("B", "orders", "2026-11-23 12:30")
        put_payload("X", "orders", "2026-11-23 11:30")
        put_payload("Y", "orders-eu", "2026-11-23 12:15")
        tid = make_task()
        res = svc.start_replay(tid)
        assert res["ok"] and res["replayed"] == 2, res
        entry = store.find("batch_replay", tid)
        assert entry["status"] == "已完成", entry["status"]
        assert entry["confirmed_members"] == ["A", "B"]
        assert set(entry["delivery_receipts"]) == {"A", "B"}
        # 目的端结果：恰好 replayed-A、replayed-B 各一份，窗外/前缀消息未交付
        assert len(copies_of("A")) == 1 and len(copies_of("B")) == 1
        assert not copies_of("X") and not copies_of("Y")
        # 审计缺口与目的端实际补录数一致
        audit = audit_row_for(tid)
        assert audit["消息总数"] == "2" and audit["成功数"] == "2", audit
        assert audit["status"] == "正常" and audit["失败原因分布"] == "", audit
        # 中间产生的回放副本不再作为输入：同窗口新任务预检仍只数到 A、B
        tid2 = make_task()
        pre2 = svc.validate_replay_range(tid2)
        assert pre2["in_range_count"] == 2 and pre2["members"] == ["A", "B"], pre2
        assert pre2["unreplayed"] == [], pre2  # A、B 已有回执
        print("PASS 场景2 实际交付：目的端恰好 A、B 各一份，审计 2/2 与回执一致，副本不再入选")
    finally:
        restore(snap)


def scenario_resume_after_lost_receipt() -> None:
    """场景3：A 写目的端但丢回执后中断，恢复时对账确认，A/B 各交付一次，C 另开一轮。"""
    snap = snapshot()
    try:
        put_payload("A", "orders", "2026-11-23 12:00")
        put_payload("B", "orders", "2026-11-23 12:30")
        tid = make_task()
        confirmed = svc.confirm_members(tid)
        assert confirmed["confirmed"] == ["A", "B"], confirmed
        step = svc.deliver_next(tid, lose_receipt=True)  # A 已写目的端但没收到回执
        assert step["delivered"] == "A" and not step["receipt"], step
        svc.handle_interrupt(tid, "链路中断")            # 随即中断
        assert store.find("batch_replay", tid)["status"] == "回放中断"
        put_payload("C", "orders", "2026-11-23 12:45")   # 同源迟到，落在同一时间段
        res = svc.start_replay(tid)                      # 恢复：先确认已交付数据和回执
        assert res["ok"], res
        assert res["recovered"] == ["A"], res            # A 的副本对账确认，回执补齐
        entry = store.find("batch_replay", tid)
        assert entry["confirmed_members"] == ["A", "B"]  # 已确认名单保持 A、B
        assert set(entry["delivery_receipts"]) == {"A", "B"}
        assert len(copies_of("A")) == 1, "A 不得重发"     # A 最终只交付一次
        assert len(copies_of("B")) == 1                   # B 最终只交付一次一次
        assert not copies_of("C")                         # 迟到的 C 不插队
        audit = audit_row_for(tid)
        assert audit["消息总数"] == "2" and audit["成功数"] == "2", audit
        # C 能另开一轮补录：新任务只派发 C
        tid2 = make_task()
        pre2 = svc.validate_replay_range(tid2)
        assert pre2["in_range_count"] == 3 and pre2["unreplayed"] == ["C"], pre2
        res2 = svc.start_replay(tid2)
        assert res2["replayed"] == 1, res2
        assert len(copies_of("C")) == 1
        assert store.find("batch_replay", tid)["confirmed_members"] == ["A", "B"]
        print("PASS 场景3 中断恢复：A 对账确认未重发，A/B 各交付一次，名单保持 A、B，C 另开一轮")
    finally:
        restore(snap)


def scenario_failed_member_resubmit() -> None:
    """场景4：B 交付失败，审计列表与补录入口的未补录明细一致，重新提交只派发 B。"""
    snap = snapshot()
    try:
        put_payload("A", "orders", "2026-11-23 12:00")
        put_payload("B", "orders", "2026-11-23 12:30")
        tid = make_task()
        svc.confirm_members(tid)
        assert svc.deliver_next(tid)["delivered"] == "A"
        failed = svc.deliver_next(tid, fail=True)        # B 写入目的端失败
        assert not failed["ok"] and failed["failed"] == "B", failed
        # 审计列表和补录入口共用逐条交付回执重算未补录明细
        entry_view = svc.unreplayed_detail(tid)["unreplayed"]
        audit_view = audit_svc.unreplayed_detail("orders")["unreplayed"]
        assert entry_view == ["B"], entry_view
        assert audit_view == ["B"], audit_view
        audit = audit_row_for(tid)
        assert audit["消息总数"] == "2" and audit["成功数"] == "1", audit
        assert audit["status"] == "偏差超标" and "B" in audit["失败原因分布"], audit
        # 审核员在列表核对记录后重新提交：只会派发 B
        res = svc.start_replay(tid)
        assert res["ok"] and res["replayed"] == 1, res
        assert len(copies_of("A")) == 1, "A 不得重发"
        assert len(copies_of("B")) == 1
        audit = audit_row_for(tid)
        assert audit["消息总数"] == "2" and audit["成功数"] == "2", audit
        assert audit["status"] == "正常" and audit["失败原因分布"] == "", audit
        assert store.find("batch_replay", tid)["status"] == "已完成"
        print("PASS 场景4 失败重提：审计缺口=1 与目的端一致，重新提交只派发 B，最终 2/2")
    finally:
        restore(snap)


def main() -> int:
    scenario_precheck()
    scenario_delivery()
    scenario_resume_after_lost_receipt()
    scenario_failed_member_resubmit()
    print("全部回放场景核对通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
