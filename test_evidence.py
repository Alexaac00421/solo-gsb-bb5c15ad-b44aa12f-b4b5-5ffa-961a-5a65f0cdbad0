"""端到端验证：历史温控证据报告的全部时间语义与页面/CSV 行为（SQLite 快照）。"""
import csv
import io
import os

os.environ["DATABASE_URL"] = "sqlite:////tmp/evidence_test.db"
os.environ.pop("SEED_DEMO", None)

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.database import Base, SessionLocal, engine
from app.main import app
from app import services
from app.models import (AuditLog, EquipmentFailure, FailureImpact, Handover,
                        QuarantineRecord, ReadingCorrection, Sample,
                        TempRangeAdjustment, TemperatureReading)

# 干净库
Base.metadata.drop_all(bind=engine)

dt = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)

with TestClient(app) as client:  # 触发 lifespan: create_all
    pass

db = SessionLocal()

# ---- 构造数据 ----
s = Sample(code="T-001", temp_min=2.0, temp_max=8.0,
           current_location="冷库A", initial_location="冷库A",
           status="ACTIVE", version=1, created_at=dt("2026-01-01T00:00"))
db.add(s); db.flush()
# h1：Jan3 00:00 确认到 B
h1 = Handover(sample_id=s.id, from_person="x", to_person="y", destination="冷库B",
              status="CONFIRMED", created_at=dt("2026-01-02T00:00"),
              decided_at=dt("2026-01-03T00:00"))
# h2/h3：同一时刻 Jan5 00:00 确认，ID 大者（h3→冷库C）优先
h2 = Handover(sample_id=s.id, from_person="y", to_person="z", destination="冷库D",
              status="CONFIRMED", created_at=dt("2026-01-04T00:00"),
              decided_at=dt("2026-01-05T00:00"))
h3 = Handover(sample_id=s.id, from_person="y", to_person="w", destination="冷库C",
              status="CONFIRMED", created_at=dt("2026-01-04T00:01"),
              decided_at=dt("2026-01-05T00:00"))
db.add_all([h1, h2, h3]); db.flush()

def add_reading(rid_time, value, oor, rid=None):
    r = TemperatureReading(sample_id=s.id, value=value, recorded_by="tester",
                           is_out_of_range=oor, recorded_at=dt(rid_time))
    db.add(r); db.flush(); return r

r1 = add_reading("2026-01-02T10:00", 5.0, False)   # A，[2,8]，在范围，== 窗口起点用例
r3 = add_reading("2026-01-04T00:00", 10.0, True)   # B，原始越界；后更正为 5 → 最终在范围
r4 = add_reading("2026-01-04T18:00", -15.0, False)  # B，已调整为 [-20,-10]，在范围
r2 = add_reading("2026-01-05T00:00", -15.0, False)  # 恰在交接同时刻：库位应为 C；温区仍为 [-20,-10]
r5 = add_reading("2026-01-06T00:00", 9.5, False)   # C，两调整同时刻取 ID 大者 [1,9] → 越界

# r3 更正链：10 → 7 → 5（最终值 5）
db.add(ReadingCorrection(reading_id=r3.id, old_value=10.0, new_value=7.0,
                         reason="c1", operator="op", created_at=dt("2026-01-04T01:00")))
db.add(ReadingCorrection(reading_id=r3.id, old_value=7.0, new_value=5.0,
                         reason="c2", operator="op", created_at=dt("2026-01-04T02:00")))

# 温区调整：Jan4 12:00 [2,8]→[-20,-10]；Jan6 00:00 两条同时刻，ID 大者 [1,9] 生效
a0 = TempRangeAdjustment(sample_id=s.id, old_min=2.0, old_max=8.0,
                         new_min=-20.0, new_max=-10.0, reason="plan",
                         operator="op", effective_at=dt("2026-01-04T12:00"),
                         review_action="NO_READING")
a1 = TempRangeAdjustment(sample_id=s.id, old_min=-20.0, old_max=-10.0,
                         new_min=0.0, new_max=10.0, reason="same-time-1",
                         operator="op", effective_at=dt("2026-01-06T00:00"),
                         review_action="IN_RANGE")
a2 = TempRangeAdjustment(sample_id=s.id, old_min=0.0, old_max=10.0,
                         new_min=1.0, new_max=9.0, reason="same-time-2",
                         operator="op", effective_at=dt("2026-01-06T00:00"),
                         review_action="IN_RANGE")
db.add_all([a0, a1, a2])
db.commit()

# 故障登记（走真实服务，固定影响清单）：
# F1 在 B [Jan3,Jan4) 与位置段 B[Jan3,Jan5) 相交；登记时未隔离 → 隔离样品
f1, created, imp1 = services.register_failure(
    db, "F-001", "冷库B", "2026-01-03T00:00", "2026-01-04T00:00",
    "制冷异常", "op1")
assert created and len(imp1) == 1, f"F1 impacts: {len(imp1)}"
assert imp1[0].overlap_start == dt("2026-01-03T00:00")
assert imp1[0].overlap_end == dt("2026-01-04T00:00")
# F2 在 C [Jan10,Jan11)：样品已隔离 → 仅关联，仍固定影响清单
f2, created, imp2 = services.register_failure(
    db, "F-002", "冷库C", "2026-01-10T00:00", "2026-01-11T00:00",
    "化霜", "op2")
assert created and len(imp2) == 1 and imp2[0].was_quarantined

# 记录证据报告前后的状态，证明报告只读
def counts():
    return {
        "audit": db.query(AuditLog).count(),
        "quar": db.query(QuarantineRecord).count(),
        "hand": db.query(Handover).count(),
        "sver": db.get(Sample, s.id).version,
    }
before = counts()

# ===================================================================
print("== 1. 主窗口 [Jan2, Jan7) ==")
rep = services.build_evidence_report(db, s.id, dt("2026-01-02T00:00"), dt("2026-01-07T00:00"))
by_id = {r.id: r for r in rep.readings}
assert [r.id for r in rep.readings] == [r1.id, r3.id, r4.id, r2.id, r5.id], \
    [r.id for r in rep.readings]

rr = by_id[r1.id]
assert (rr.original_value, rr.final_value) == (5.0, 5.0)
assert (rr.range_min, rr.range_max) == (2.0, 8.0) and rr.location == "冷库A"
assert not rr.original_out_of_range and not rr.final_out_of_range
assert rr.recorded_at == dt("2026-01-02T10:00")

rr = by_id[r3.id]
assert (rr.original_value, rr.final_value) == (10.0, 5.0) and rr.correction_count == 2
assert (rr.range_min, rr.range_max) == (2.0, 8.0) and rr.location == "冷库B"
assert rr.original_out_of_range is True and rr.final_out_of_range is False  # 保留原始判定，最终标示在范围

rr = by_id[r4.id]
assert rr.final_value == -15.0 and (rr.range_min, rr.range_max) == (-20.0, -10.0)
assert rr.location == "冷库B" and not rr.final_out_of_range

rr = by_id[r2.id]
assert rr.location == "冷库C", f"同一时刻交接 ID 较大者优先失败: {rr.location}"
assert (rr.range_min, rr.range_max) == (-20.0, -10.0)
assert rr.final_value == -15.0 and not rr.final_out_of_range

rr = by_id[r5.id]
assert rr.location == "冷库C"
assert (rr.range_min, rr.range_max) == (1.0, 9.0), (rr.range_min, rr.range_max)
assert rr.final_out_of_range is True  # 9.5 超出同时刻 ID 较大调整 [1,9]

assert len(rep.failures) == 1
f = rep.failures[0]
assert f.failure_code == "F-001"
assert (f.overlap_start, f.overlap_end) == (dt("2026-01-03T00:00"), dt("2026-01-04T00:00"))
assert (f.impact_overlap_start, f.impact_overlap_end) == (
    dt("2026-01-03T00:00"), dt("2026-01-04T00:00"))
assert rep.final_oor_count == 1 and rep.original_oor_count == 1
assert rep.verdict == "NON_COMPLIANT", rep.verdict
print("   读数重放、同时刻 tie-break、最终/原始越界、故障交集、判定 NON_COMPLIANT  OK")

print("== 2. 左闭右开边界：[Jan2 10:00, Jan4 00:00) 含 r1 不含 r3 ==")
rep2 = services.build_evidence_report(db, s.id, dt("2026-01-02T10:00"), dt("2026-01-04T00:00"))
assert [r.id for r in rep2.readings] == [r1.id]
# F1 固定重叠 [Jan3,Jan4) 与查询窗有交集 → 即便窗内读数合规也判 NON_COMPLIANT
assert len(rep2.failures) == 1 and rep2.failures[0].failure_code == "F-001"
assert rep2.verdict == "NON_COMPLIANT"
print("   边界包含/排除、故障交集优先于合规判定  OK")

print("== 3. 窗内无读数但有相交故障 → NON_COMPLIANT（不得判合规）==")
rep3 = services.build_evidence_report(db, s.id, dt("2026-01-10T00:00"), dt("2026-01-11T00:00"))
assert rep3.readings == [] and len(rep3.failures) == 1
assert rep3.failures[0].failure_code == "F-002"
assert (rep3.failures[0].overlap_start, rep3.failures[0].overlap_end) == (
    dt("2026-01-10T00:00"), dt("2026-01-11T00:00"))
assert rep3.verdict == "NON_COMPLIANT"
print("   无读数 + 故障 = 不合规  OK")

print("== 4. 窗内无读数且无故障 → INCONCLUSIVE（不得判合规）==")
rep4 = services.build_evidence_report(db, s.id, dt("2025-12-01T00:00"), dt("2025-12-31T00:00"))
assert rep4.readings == [] and rep4.failures == [] and rep4.verdict == "INCONCLUSIVE"
# 紧邻故障但不相交：[Jan4 00:00, Jan5 00:00) 与 F1 [Jan3,Jan4) 在端点相接、无交集
rep4b = services.build_evidence_report(db, s.id, dt("2026-01-04T00:00"), dt("2026-01-05T00:00"))
assert [r.id for r in rep4b.readings] == [r3.id, r4.id]
assert rep4b.failures == []  # r3 已更正至 5 在范围
assert rep4b.verdict == "COMPLIANT"
print("   空窗 INCONCLUSIVE、端点相接不算相交、全在范围 COMPLIANT  OK")

print("== 5. 参数校验 ==")
for bad in [(dt("2026-01-05T00:00"), dt("2026-01-05T00:00")),  # start==end
            (dt("2026-01-06T00:00"), dt("2026-01-05T00:00"))]:  # start>end
    try:
        services.build_evidence_report(db, s.id, *bad); raise AssertionError("应报错: %r" % (bad,))
    except services.DomainError as e:
        assert e.status_code == 400
# 结束晚于当前时间
try:
    services.build_evidence_report(db, s.id, dt("2026-01-01T00:00"),
                                   datetime(2099, 1, 1, tzinfo=timezone.utc))
    raise AssertionError("未来结束时间应报错")
except services.DomainError as e:
    assert e.status_code == 400 and "当前时间" in e.message
# 样品不存在
try:
    services.build_evidence_report(db, 99999, dt("2026-01-01T00:00"), dt("2026-01-02T00:00"))
    raise AssertionError("不存在样品应报 404")
except services.DomainError as e:
    assert e.status_code == 404
print("   起止相等/倒序/未来结束/样品不存在 全部明确报错  OK")

print("== 6. 报告只读：隔离、交接、审计、版本均不变 ==")
after = counts()
assert before == after, (before, after)
db.rollback()  # 关闭快照事务
print("   ", before, "未变化  OK")

# ===================================================================
print("== 7. HTTP：HTML 页面 ==")
with TestClient(app) as client:
    r = client.get("/samples/%d/evidence" % s.id, params={
        "start": "2026-01-02T00:00", "end": "2026-01-07T00:00"})
    assert r.status_code == 200
    body = r.text
    assert "历史温控证据报告" in body and "T-001" in body
    assert "不合规" in body
    assert "2026-01-03 00:00:00Z ~ 2026-01-04 00:00:00Z" in body  # 故障交集 UTC
    assert "原始越界" in body            # r3 保留原始判定
    assert "/samples/%d/evidence.csv" % s.id in body
    # r3 最终在范围（5.0）不应有 row-bad；r5 最终越界应有
    assert "9.5" in body

    # 无效时间窗 → 303 回详情页带错误
    r = client.get("/samples/%d/evidence" % s.id, params={
        "start": "2026-01-07T00:00", "end": "2026-01-06T00:00"}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/samples/%d" % s.id)
    # 缺参（无法解析）同样 303
    r = client.get("/samples/%d/evidence" % s.id, params={"start": "", "end": ""},
                   follow_redirects=False)
    assert r.status_code == 303
    # 样品不存在 → 回首页
    r = client.get("/samples/99999/evidence", params={
        "start": "2026-01-01T00:00", "end": "2026-01-02T00:00"}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/?msg=")

    print("   HTML 200 / 无效时间 303 / 样品不存在 303  OK")

    print("== 8. HTTP：CSV 下载（与页面同内容）==")
    r = client.get("/samples/%d/evidence.csv" % s.id, params={
        "start": "2026-01-02T00:00", "end": "2026-01-07T00:00"})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv")
    cd = r.headers["content-disposition"]
    assert cd.startswith("attachment;") and "evidence_T-001_" in cd
    text_csv = r.text
    assert text_csv.startswith("﻿")
    rows = list(csv.reader(io.StringIO(text_csv.lstrip("﻿"))))
    flat = ["|".join(row) for row in rows]
    assert any("历史温控证据报告" in c for c in flat)
    assert any(c.startswith("总体判定|") and "不合规" in c for c in flat)
    assert any("查询起始（UTC，左闭）|2026-01-02 00:00:00Z" == c for c in flat)
    # r3 一行：原值 10、最终 5、更正 2 次、温区 2~8、库位 B、原始越界=是、最终=否
    assert any("10.0|5.0|2|2.0|8.0|冷库B|是|否" in c for c in flat), [c for c in flat if "冷库B" in c]
    # r5：9.5 无更正、最终越界=是，温区 1~9
    assert any("9.5|9.5|0|1.0|9.0|冷库C|否|是" in c for c in flat)
    # 故障段交集
    assert any("F-001" in c and "冷库B" in c and "2026-01-03 00:00:00Z" in c for c in flat)

    # 无读数窗：CSV 仍列故障，且判定不合规
    r = client.get("/samples/%d/evidence.csv" % s.id, params={
        "start": "2026-01-10T00:00", "end": "2026-01-11T00:00"})
    assert r.status_code == 200
    rows = list(csv.reader(io.StringIO(r.text.lstrip("﻿"))))
    flat = ["|".join(row) for row in rows]
    assert any(c.startswith("总体判定|") and "不合规" in c for c in flat)
    assert any("查询窗内无读数" in c for c in flat)
    assert any("F-002" in c for c in flat)

    # CSV 错误：无效时间 400 纯文本；样品不存在 404
    r = client.get("/samples/%d/evidence.csv" % s.id, params={
        "start": "2026-01-07T00:00", "end": "2026-01-06T00:00"})
    assert r.status_code == 400 and "时间窗" in r.text
    r = client.get("/samples/99999/evidence.csv", params={
        "start": "2026-01-01T00:00", "end": "2026-01-02T00:00"})
    assert r.status_code == 404 and "样品不存在" in r.text
    print("   CSV 内容/文件名/BOM/窗内无读数/400/404  OK")

    print("== 9. 详情页入口卡片存在 ==")
    r = client.get("/samples/%d" % s.id)
    assert r.status_code == 200
    assert "/samples/%d/evidence" % s.id in r.text
    assert "/samples/%d/evidence.csv" % s.id in r.text
    print("   入口表单（查看 / 下载 CSV） OK")

# 启动方式不变：健康检查
with TestClient(app) as client:
    assert client.get("/health").json() == {"status": "ok"}
print("== 10. 原有启动与健康检查保持可用  OK")

print("\n全部断言通过 ✅")
