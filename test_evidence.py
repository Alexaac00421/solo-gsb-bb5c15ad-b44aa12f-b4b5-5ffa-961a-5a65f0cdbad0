# -*- coding: utf-8 -*-
"""端到端验证历史温控证据报告（SQLite，FastAPI TestClient）。"""
import csv as csvmod
import io
import os
os.environ["DATABASE_URL"] = "sqlite:///./test_evidence.db"
os.environ["SEED_DEMO"] = "false"

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.main import app
from app.database import Base, engine, SessionLocal
from app import services
from app.models import (Handover, ReadingCorrection, Sample,
                        TempRangeAdjustment, TemperatureReading)

if os.path.exists("./test_evidence.db"):
    os.remove("./test_evidence.db")
Base.metadata.create_all(bind=engine)

t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _is_num(v):
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


db = SessionLocal()

# 样品初始温区 [2, 8]，登记于 冷库A-01
s = services.register_sample(db, "T-001", 2.0, 8.0, "冷库A-01", "tester")
sid = s.id
# 回填登记时刻，使读数/交接/故障时间线自洽（历史数据场景）
s.created_at = t0
db.flush()

# 读数 1 @ 02:00 值 5.0 在界（初始温区、登记位置）
r1 = TemperatureReading(sample_id=sid, value=5.0, recorded_by="a",
                        is_out_of_range=False, recorded_at=t0 + timedelta(hours=2))
# 读数 2 @ 03:00 值 9.0 原始越界；后更正为 4.0（最终在初始温区内）
r2 = TemperatureReading(sample_id=sid, value=9.0, recorded_by="a",
                        is_out_of_range=True, recorded_at=t0 + timedelta(hours=3))
# 读数 3 @ 04:00 值 5.5（旧温区 [2,8] 下在界）
r3 = TemperatureReading(sample_id=sid, value=5.5, recorded_by="a",
                        is_out_of_range=False, recorded_at=t0 + timedelta(hours=4))
db.add_all([r1, r2, r3])
db.flush()
db.add(ReadingCorrection(reading_id=r2.id, old_value=9.0, new_value=4.0,
                         reason="探头偏移", operator="c"))

# 温区调整：05:00 先收紧为 [0,4]，同刻再改为 [0,5]（ID 较大，同刻优先）
adj1 = TempRangeAdjustment(sample_id=sid, old_min=2.0, old_max=8.0,
                           new_min=0.0, new_max=4.0, reason="方案变更",
                           operator="c", effective_at=t0 + timedelta(hours=5),
                           review_reading_id=r3.id, review_value=5.5,
                           review_out_of_range=False, review_action="IN_RANGE")
adj2 = TempRangeAdjustment(sample_id=sid, old_min=0.0, old_max=4.0,
                           new_min=0.0, new_max=5.0, reason="同刻修正",
                           operator="c", effective_at=t0 + timedelta(hours=5),
                           review_reading_id=r3.id, review_value=5.5,
                           review_out_of_range=False, review_action="IN_RANGE")
db.add_all([adj1, adj2])

# 读数 4 @ 06:00 值 4.5：温区 [0,5]（取同刻较大 ID），最终在界；
# 故意保留原始越界标记以验证「原始判定保留不改写」。
r4 = TemperatureReading(sample_id=sid, value=4.5, recorded_by="a",
                        is_out_of_range=True, recorded_at=t0 + timedelta(hours=6))
db.add(r4)

# 07:00 两条同刻确认交接：ID 较大的目的地 冷库C-03 生效
h1 = Handover(sample_id=sid, from_person="a", to_person="b", destination="冷库B-02",
              status="CONFIRMED", created_at=t0 + timedelta(hours=6, minutes=50),
              decided_at=t0 + timedelta(hours=7))
h2 = Handover(sample_id=sid, from_person="a", to_person="b", destination="冷库C-03",
              status="CONFIRMED", created_at=t0 + timedelta(hours=6, minutes=51),
              decided_at=t0 + timedelta(hours=7))
db.add_all([h1, h2])
db.flush()

# 读数 5 @ 08:00 值 6.0：库位 冷库C-03、温区 [0,5] -> 最终越界；原始未标记
r5 = TemperatureReading(sample_id=sid, value=6.0, recorded_by="a",
                        is_out_of_range=False, recorded_at=t0 + timedelta(hours=8))
# 读数 6 在查询窗外（12:00），不应出现
r6 = TemperatureReading(sample_id=sid, value=3.0, recorded_by="a",
                        is_out_of_range=False, recorded_at=t0 + timedelta(hours=12))
db.add_all([r5, r6])
db.commit()

# 故障 F-COLD-A：冷库A-01 02:30~03:30，与登记位置时段相交 -> 固定清单并隔离
f1, created1, imp1 = services.register_failure(
    db, "F-COLD-A", "冷库A-01", iso(t0 + timedelta(hours=2, minutes=30)),
    iso(t0 + timedelta(hours=3, minutes=30)), "压缩机故障", "op")
assert created1 and len(imp1) == 1, (created1, len(imp1))
# 故障 F-NONE：样品从未到过的库位 -> 空清单
f2, created2, imp2 = services.register_failure(
    db, "F-NONE", "冷库X-99", iso(t0 + timedelta(hours=20)),
    iso(t0 + timedelta(hours=21)), "无样品库位故障", "op")
assert created2 and imp2 == [], imp2

# 故障登记后直接插一条待确认交接（样品被故障隔离，仅为验证待确认交接不决定库位）
hp = Handover(sample_id=sid, from_person="a", to_person="x", destination="冷库Z-99",
              status="PENDING", created_at=t0 + timedelta(hours=7, minutes=30))
db.add(hp)
db.commit()
db.close()

with TestClient(app) as client:
    win_start = iso(t0)
    win_end = iso(t0 + timedelta(hours=10))

    # ---------- 1. HTML 报告 ----------
    resp = client.get(f"/samples/{sid}/evidence",
                      params={"start": win_start, "end": win_end})
    assert resp.status_code == 200, resp.status_code
    html = resp.text
    for text, expect in [
        ("历史温控证据报告", True),
        ("冷库C-03", True),
        ("冷库Z-99", False),                    # 待确认交接不参与库位
        ("冷库B-02", False),                    # 同刻确认取 ID 较大者
        ("F-COLD-A", True),
        ("F-NONE", False),                      # 空清单故障不出现
        (f"温区调整 #{adj2.id}", True),         # 同刻取 ID 较大
        (f"温区调整 #{adj1.id}", False),
        (f"已确认交接 #{h2.id}", True),         # 同刻确认取 ID 较大
        ("2026-01-01T02:00:00Z", True),
        ("2026-01-01T12:00:00Z", False),        # 窗外读数
        ("不构成合规证明", True),
        ("不符合", True),
        ("最终值判定越界", True),               # 读数5
        ("原始判定保留", True),                 # 读数2/4
        ("evidence.csv?start=", True),
    ]:
        assert (text in html) == expect, f"HTML check {text!r} expected {expect}"

    # ---------- 2. CSV 同内容 ----------
    resp_csv = client.get(f"/samples/{sid}/evidence.csv",
                          params={"start": win_start, "end": win_end})
    assert resp_csv.status_code == 200
    assert "text/csv" in resp_csv.headers["content-type"]
    cd = resp_csv.headers["content-disposition"]
    assert cd.startswith("attachment; filename=\"evidence_T-001_"), cd
    csv_text = resp_csv.content.decode("utf-8-sig")
    assert "F-COLD-A" in csv_text and "F-NONE" not in csv_text
    assert "冷库Z-99" not in csv_text and "冷库B-02" not in csv_text
    assert f"温区调整#{adj2.id}" in csv_text
    assert f"已确认交接#{h2.id}" in csv_text
    assert "不符合" in csv_text
    rows = list(csvmod.reader(io.StringIO(csv_text)))
    reading_rows = [r for r in rows if len(r) >= 4 and r[0].isdigit()
                    and r[1].startswith("2026-") and _is_num(r[2])
                    and _is_num(r[3])]
    assert len(reading_rows) == 5, reading_rows
    assert all("2026-01-01T12:00:00Z" not in r[1] for r in reading_rows)

    # ---------- 3. 窗内无读数但有相交故障 ----------
    resp2 = client.get(f"/samples/{sid}/evidence", params={
        "start": iso(t0 + timedelta(hours=2, minutes=45)),
        "end": iso(t0 + timedelta(hours=2, minutes=50))})
    assert resp2.status_code == 200
    assert "查询窗内无温度读数" in resp2.text
    assert "F-COLD-A" in resp2.text
    assert "不符合" in resp2.text

    # 窗内无读数无故障 -> INCONCLUSIVE
    resp3 = client.get(f"/samples/{sid}/evidence", params={
        "start": iso(t0 + timedelta(hours=10)),
        "end": iso(t0 + timedelta(hours=11))})
    assert resp3.status_code == 200
    assert "无法判定合规" in resp3.text
    assert "不能证明全程受控" in resp3.text
    assert "F-COLD-A" not in resp3.text

    # 窗内读数全在界、无故障 -> 仍不作合规判定
    resp4 = client.get(f"/samples/{sid}/evidence", params={
        "start": iso(t0 + timedelta(hours=2)),
        "end": iso(t0 + timedelta(hours=2, minutes=30))})
    assert resp4.status_code == 200
    assert "无法判定合规" in resp4.text
    assert "均未越界且无相交故障" in resp4.text

    # 左闭右开边界：[03:00, 12:00) 含 03:00、不含 02:00 与 12:00
    resp5 = client.get(f"/samples/{sid}/evidence", params={
        "start": iso(t0 + timedelta(hours=3)),
        "end": iso(t0 + timedelta(hours=12))})
    assert resp5.status_code == 200
    assert "2026-01-01T03:00:00Z" in resp5.text
    assert "2026-01-01T02:00:00Z" not in resp5.text
    # 12:00 仅作为右开窗口端点出现，不能作为读数记录时刻出现
    body = resp5.text.split("一、窗内温度读数", 1)[1].split("二、", 1)[0]
    assert "2026-01-01T12:00:00Z" not in body

    # ---------- 4. 错误 ----------
    r404 = client.get("/samples/99999/evidence",
                      params={"start": win_start, "end": win_end})
    assert r404.status_code == 404 and "样品不存在" in r404.text
    r404_csv = client.get("/samples/99999/evidence.csv",
                         params={"start": win_start, "end": win_end})
    assert r404_csv.status_code == 404 and "样品不存在" in r404_csv.text

    r_inv = client.get(f"/samples/{sid}/evidence",
                       params={"start": win_end, "end": win_start})
    assert r_inv.status_code == 400 and "起始时间必须早于结束时间" in r_inv.text

    r_future = client.get(f"/samples/{sid}/evidence", params={
        "start": win_start,
        "end": iso(datetime.now(timezone.utc) + timedelta(days=1))})
    assert r_future.status_code == 400 and "不得晚于当前时间" in r_future.text

    r_empty = client.get(f"/samples/{sid}/evidence",
                         params={"start": "", "end": win_end})
    assert r_empty.status_code == 400 and "不能为空" in r_empty.text

    r_badfmt = client.get(f"/samples/{sid}/evidence",
                          params={"start": "not-a-date", "end": win_end})
    assert r_badfmt.status_code == 400 and "时间格式无效" in r_badfmt.text

    r_bad_csv = client.get(f"/samples/{sid}/evidence.csv",
                           params={"start": "not-a-date", "end": win_end})
    assert r_bad_csv.status_code == 400 and "时间格式无效" in r_bad_csv.text

# ---------- 5. 服务层逐条断言 ----------
db2 = SessionLocal()
rep = services.build_evidence_report(db2, sid, win_start, win_end)
db2.close()
by_id = {row["reading_id"]: row for row in rep["rows"]}

assert by_id[1]["range_adjustment_id"] is None
assert (by_id[1]["range_min"], by_id[1]["range_max"]) == (2.0, 8.0)
assert by_id[1]["location"] == "冷库A-01"
assert by_id[1]["location_handover_id"] is None
assert by_id[1]["final_out_of_range"] is False

assert by_id[2]["original_value"] == 9.0 and by_id[2]["final_value"] == 4.0
assert (by_id[2]["range_min"], by_id[2]["range_max"]) == (2.0, 8.0)
assert by_id[2]["original_out_of_range"] is True
assert by_id[2]["final_out_of_range"] is False

assert (by_id[3]["range_min"], by_id[3]["range_max"]) == (2.0, 8.0)
assert by_id[3]["final_out_of_range"] is False

assert (by_id[4]["range_min"], by_id[4]["range_max"]) == (0.0, 5.0)
assert by_id[4]["range_adjustment_id"] == adj2.id
assert by_id[4]["original_out_of_range"] is True
assert by_id[4]["final_out_of_range"] is False

assert by_id[5]["location"] == "冷库C-03"
assert by_id[5]["location_handover_id"] == h2.id
assert (by_id[5]["range_min"], by_id[5]["range_max"]) == (0.0, 5.0)
assert by_id[5]["original_out_of_range"] is False
assert by_id[5]["final_out_of_range"] is True

assert len(rep["failure_rows"]) == 1
fr = rep["failure_rows"][0]
assert fr["failure"].code == "F-COLD-A"
assert fr["intersect_start"] == t0 + timedelta(hours=2, minutes=30)
assert fr["intersect_end"] == t0 + timedelta(hours=3, minutes=30)
assert rep["verdict"] == "NON_COMPLIANT"

# ---------- 6. 纯只读：查询不改隔离/交接 ----------
db3 = SessionLocal()
sample_after = db3.get(Sample, sid)
assert sample_after.status == "QUARANTINED"   # 由故障登记导致，非报告导致
assert sample_after.version == 2              # 故障登记 +1，之后无变化
pending_after = db3.scalar(select(Handover).where(Handover.id == hp.id))
assert pending_after.status == "PENDING"
db3.close()

print("ALL EVIDENCE REPORT TESTS PASSED")
