import csv
import io
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

from datetime import timezone

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from . import services
from .database import Base, SessionLocal, engine, get_db, get_report_db
from .models import (AuditLog, EquipmentFailure, FailureImpact, Handover,
                     QuarantineRecord, ReadingCorrection, Sample,
                     TempRangeAdjustment, TemperatureReading)
from .services import DomainError

BASE_DIR = Path(__file__).resolve().parent

EVENT_NAMES = {
    "SAMPLE_REGISTERED": "样品录入",
    "HANDOVER_CREATED": "交接发起",
    "HANDOVER_CONFIRMED": "交接确认",
    "HANDOVER_INVALIDATED": "交接失效",
    "READING_RECORDED": "温度记录",
    "READING_CORRECTED": "读数更正",
    "QUARANTINED": "样品隔离",
    "QUARANTINE_RELEASED": "隔离解除",
    "FAULT_AFFECTED": "故障影响",
    "FAULT_LINKED": "故障关联（已隔离）",
    "TEMP_RANGE_ADJUSTED": "温区调整",
}


def init_db() -> None:
    """初始化表结构，等待数据库就绪（Compose 一键启动时数据库可能仍在启动）。

    create_all 只建新表，不会给既有表加列；因此对旧版本库做一次轻量迁移：
    补齐 samples.initial_location、handovers.failure_id、
    quarantine_records.failure_id，并按登记审计回填初始位置。
    """
    for _ in range(30):
        try:
            Base.metadata.create_all(bind=engine)
            _migrate_schema()
            return
        except OperationalError:
            time.sleep(1)
    raise RuntimeError("无法连接数据库，初始化失败")


def _migrate_schema() -> None:
    """给既有库补齐本轮新增的可空列（幂等），并回填 samples.initial_location。"""
    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    statements: list[str] = []
    if "samples" in existing_tables:
        cols = {c["name"] for c in inspector.get_columns("samples")}
        if "initial_location" not in cols:
            statements.append("ALTER TABLE samples ADD COLUMN initial_location VARCHAR(128)")
    if "handovers" in existing_tables:
        cols = {c["name"] for c in inspector.get_columns("handovers")}
        if "failure_id" not in cols:
            statements.append("ALTER TABLE handovers ADD COLUMN failure_id INTEGER")
    if "quarantine_records" in existing_tables:
        cols = {c["name"] for c in inspector.get_columns("quarantine_records")}
        if "failure_id" not in cols:
            statements.append("ALTER TABLE quarantine_records ADD COLUMN failure_id INTEGER")
    if not statements:
        return
    db = SessionLocal()
    try:
        for stmt in statements:
            db.execute(text(stmt))
        # 回填初始位置：从 SAMPLE_REGISTERED 审计明细取登记位置，兜底用当前位置。
        for sid, current_location in db.execute(
                text("SELECT id, current_location FROM samples "
                     "WHERE initial_location IS NULL")).all():
            reg = db.execute(
                text("SELECT detail FROM audit_logs WHERE sample_id = :sid "
                     "AND event_type = 'SAMPLE_REGISTERED' ORDER BY id LIMIT 1"),
                {"sid": sid}).first()
            loc = None
            if reg is not None:
                detail = reg[0]
                if isinstance(detail, str):  # 裸 SQL 读取时 JSON 列可能以文本返回
                    try:
                        detail = json.loads(detail)
                    except (ValueError, TypeError):
                        detail = None
                if isinstance(detail, dict):
                    loc = detail.get("location")
            db.execute(text("UPDATE samples SET initial_location = :loc WHERE id = :sid"),
                       {"loc": loc or current_location, "sid": sid})
        db.commit()
    finally:
        db.close()


def seed_demo() -> None:
    """可选的演示数据（SEED_DEMO=true 时启用）。"""
    db = SessionLocal()
    try:
        if db.scalar(select(Sample).limit(1)):
            return
        s1 = services.register_sample(db, "DEMO-001", 2.0, 8.0, "冷库A-01", "system")
        s2 = services.register_sample(db, "DEMO-002", -20.0, -10.0, "冷库B-02", "system")
        services.record_temperature(db, s1.id, 5.2, "张三")
        services.create_handover(db, s2.id, "张三", "李四", "实验室C-101", "例行转移")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if os.getenv("SEED_DEMO", "").lower() in ("1", "true", "yes"):
        seed_demo()
    yield


app = FastAPI(title="实验室样品流转系统", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def fmt_dt(value) -> str:
    if not value:
        return "-"
    if isinstance(value, str):
        return value
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def fmt_utc(value) -> str:
    """证据报告：统一按 UTC 展示（ISO，秒级），与页面输入的 UTC 时间一致。"""
    if not value:
        return ""
    if isinstance(value, str):
        return value
    if value.tzinfo is None:  # SQLite 取回的无时区值按 UTC 解释
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


templates.env.filters["dt"] = fmt_dt
templates.env.filters["utc"] = fmt_utc
templates.env.filters["localdt"] = lambda value: (
    value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M") if value else "")
templates.env.filters["urlencode"] = lambda value: quote(str(value), safe="")
templates.env.filters["json_detail"] = lambda d: json.dumps(d or {}, ensure_ascii=False)


def _minutes_between(pair) -> int:
    """两个 datetime 之间的整分钟数（左闭右开区间时长）。"""
    start, end = pair
    if not start or not end:
        return 0
    return int((end - start).total_seconds() // 60)


templates.env.filters["minutes_between"] = _minutes_between


def render(request: Request, name: str, status_code: int = 200, **ctx):
    ctx.setdefault("msg", request.query_params.get("msg"))
    ctx.setdefault("level", request.query_params.get("level", "success"))
    return templates.TemplateResponse(request, name, ctx, status_code=status_code)


def _is_lock_conflict(exc: OperationalError) -> bool:
    """识别数据库死锁/锁等待失败（事务已被库回滚，提示用户重试即可）。"""
    text_exc = str(exc.orig).lower()
    markers = ("deadlock detected", "lock not available", "could not obtain lock",
               "database is locked", "database table is locked")
    return any(m in text_exc for m in markers)


def lock_conflict_redirect(db: Session, url: str, where: str):
    db.rollback()
    return redirect(url, f"{where}与其他操作并发冲突，本次未生效（无部分变更），请刷新后重试", "error")


def redirect(url: str, msg: str | None = None, level: str = "success") -> RedirectResponse:
    if msg:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}msg={quote(msg)}&level={level}"
    return RedirectResponse(url, status_code=303)


# ---------------------------------------------------------------- 页面

@app.get("/", response_class=HTMLResponse)
def index(request: Request, db: Session = Depends(get_db)):
    samples = db.scalars(select(Sample).order_by(Sample.id.desc())).all()
    pending_counts = dict(db.execute(
        select(Handover.sample_id, func.count())
        .where(Handover.status == "PENDING")
        .group_by(Handover.sample_id)
    ).all())
    return render(request, "index.html", samples=samples, pending_counts=pending_counts)


@app.get("/samples/new", response_class=HTMLResponse)
def new_sample(request: Request):
    return render(request, "new_sample.html")


# ---------------------------------------------------------------- 设备故障页面

@app.get("/failures", response_class=HTMLResponse)
def failures(request: Request, db: Session = Depends(get_db)):
    items = db.scalars(select(EquipmentFailure).order_by(EquipmentFailure.id.desc())).all()
    counts = dict(db.execute(
        select(FailureImpact.failure_id, func.count())
        .group_by(FailureImpact.failure_id)).all())
    return render(request, "failures.html", failures=items, counts=counts)


@app.get("/failures/new", response_class=HTMLResponse)
def new_failure(request: Request):
    return render(request, "new_failure.html")


@app.get("/failures/{failure_id}", response_class=HTMLResponse)
def failure_detail(failure_id: int, request: Request, db: Session = Depends(get_db)):
    failure = db.get(EquipmentFailure, failure_id)
    if failure is None:
        return redirect("/failures", "故障登记不存在", "error")
    impacts = db.scalars(
        select(FailureImpact).where(FailureImpact.failure_id == failure_id)
        .order_by(FailureImpact.id)).all()
    sample_map = {s.id: s for s in db.scalars(select(Sample)).all()}
    # 隔离依据：本次故障直接产生的隔离记录与失效交接
    quarantines = db.scalars(
        select(QuarantineRecord).where(QuarantineRecord.failure_id == failure_id)
        .order_by(QuarantineRecord.id)).all()
    invalidated = db.scalars(
        select(Handover).where(Handover.failure_id == failure_id)
        .order_by(Handover.id)).all()
    return render(request, "failure_detail.html", failure=failure, impacts=impacts,
                  sample_map=sample_map, quarantines=quarantines,
                  invalidated=invalidated)


@app.get("/samples/{sample_id}", response_class=HTMLResponse)
def sample_detail(sample_id: int, request: Request, db: Session = Depends(get_db)):
    sample = db.get(Sample, sample_id)
    if sample is None:
        return redirect("/", "样品不存在", "error")
    handovers = db.scalars(
        select(Handover).where(Handover.sample_id == sample_id)
        .order_by(Handover.id.desc())).all()
    readings = db.scalars(
        select(TemperatureReading).where(TemperatureReading.sample_id == sample_id)
        .order_by(TemperatureReading.id.desc())).all()
    corrections = db.scalars(
        select(ReadingCorrection).join(TemperatureReading)
        .where(TemperatureReading.sample_id == sample_id)
        .order_by(ReadingCorrection.id)).all()
    corrections_by_reading: dict[int, list] = {}
    effective: dict[int, float] = {}
    for r in readings:
        effective[r.id] = r.value
    for c in corrections:
        corrections_by_reading.setdefault(c.reading_id, []).append(c)
        effective[c.reading_id] = c.new_value
    quarantines = db.scalars(
        select(QuarantineRecord).where(QuarantineRecord.sample_id == sample_id)
        .order_by(QuarantineRecord.id.desc())).all()
    audits = db.scalars(
        select(AuditLog).where(AuditLog.sample_id == sample_id)
        .order_by(AuditLog.id.desc())).all()
    # 该样品涉及的设备故障（按故障影响清单），用于展示隔离/关联依据
    impact_rows = db.scalars(
        select(FailureImpact).where(FailureImpact.sample_id == sample_id)
        .order_by(FailureImpact.id)).all()
    failure_ids = sorted({imp.failure_id for imp in impact_rows})
    failure_map = {f.id: f for f in db.scalars(
        select(EquipmentFailure).where(EquipmentFailure.id.in_(failure_ids))).all()} if failure_ids else {}
    impacts_by_failure: dict[int, list] = {}
    for imp in impact_rows:
        impacts_by_failure.setdefault(imp.failure_id, []).append(imp)
    pending = next((h for h in handovers if h.status == "PENDING"), None)
    adjustments = db.scalars(
        select(TempRangeAdjustment).where(TempRangeAdjustment.sample_id == sample_id)
        .order_by(TempRangeAdjustment.id.desc())).all()
    return render(request, "sample_detail.html",
                  sample=sample, handovers=handovers, readings=readings,
                  corrections_by_reading=corrections_by_reading, effective=effective,
                  quarantines=quarantines, audits=audits, pending=pending,
                  failure_map=failure_map, impacts_by_failure=impacts_by_failure,
                  adjustments=adjustments, event_names=EVENT_NAMES)


# ---------------------------------------------------------------- 历史温控证据报告

VERDICT_LABELS = {
    "NON_COMPLIANT": "不符合（按最终值存在越界和/或与已固定故障时段相交）",
    "INCONCLUSIVE": "无法判定合规（证据不足，本报告任何情况下均不作合规结论）",
}


def _evidence_context(sample_id: int, start_raw: str, end_raw: str,
                      db: Session) -> dict:
    report = services.build_evidence_report(db, sample_id, start_raw, end_raw)
    report["verdict_label"] = VERDICT_LABELS[report["verdict"]]
    return report


@app.get("/samples/{sample_id}/evidence", response_class=HTMLResponse)
def evidence_report(sample_id: int, request: Request,
                    start: str = "", end: str = "",
                    db: Session = Depends(get_report_db)):
    try:
        report = _evidence_context(sample_id, start, end, db)
    except DomainError as e:
        return render(request, "evidence_error.html", status_code=e.status_code,
                      sample_id=sample_id, start=start, end=end, message=e.message)
    return render(request, "evidence_report.html", report=report)


def _evidence_csv(report: dict) -> str:
    """与证据报告页面同内容的 CSV（UTF-8 BOM，Excel 可直接打开）。"""
    buf = io.StringIO()
    buf.write("﻿")
    writer = csv.writer(buf)
    sample = report["sample"]
    writer.writerow(["历史温控证据报告（左闭右开，UTC，只读一致快照，非合规证明）"])
    writer.writerow(["样品ID", sample.id, "样品编号", sample.code])
    writer.writerow(["查询起始[", fmt_utc(report["start"])])
    writer.writerow(["查询结束)", fmt_utc(report["end"])])
    writer.writerow(["报告生成时刻(UTC)", fmt_utc(report["generated_at"])])
    writer.writerow(["结论", report["verdict_label"]])
    writer.writerow([])
    writer.writerow(["一、窗内读数（按记录时刻升序；越界以最终更正值对照记录时生效温区判定，"
                     "原始越界判定保留）"])
    writer.writerow(["读数ID", "记录时刻(UTC)", "原始值(°C)", "最终更正值(°C)",
                     "记录时允许下限(°C)", "记录时允许上限(°C)", "生效温区依据",
                     "所在库位", "库位依据", "最终值越界", "原始越界判定", "记录人"])
    if report["rows"]:
        for r in report["rows"]:
            writer.writerow([
                r["reading_id"], fmt_utc(r["recorded_at"]),
                r["original_value"], r["final_value"],
                r["range_min"], r["range_max"],
                ("初始温区" if r["range_adjustment_id"] is None
                 else f"温区调整#{r['range_adjustment_id']}"),
                r["location"],
                ("登记位置" if r["location_handover_id"] is None
                 else f"已确认交接#{r['location_handover_id']}"),
                "是" if r["final_out_of_range"] else "否",
                "是" if r["original_out_of_range"] else "否",
                r["recorded_by"],
            ])
    else:
        writer.writerow(["窗内无温度读数（不判合规）"])
    writer.writerow([])
    writer.writerow(["二、已固定故障影响清单中与查询窗相交的故障及交集时段"])
    writer.writerow(["故障ID", "故障编号", "库位", "故障开始[ (UTC)", "故障结束) (UTC)",
                     "交集起[ (UTC)", "交集止) (UTC)", "登记时样品状态", "故障原因", "操作者"])
    if report["failure_rows"]:
        for fr in report["failure_rows"]:
            f, imp = fr["failure"], fr["impact"]
            writer.writerow([
                f.id, f.code, f.location,
                fmt_utc(f.start_time), fmt_utc(f.end_time),
                fmt_utc(fr["intersect_start"]), fmt_utc(fr["intersect_end"]),
                ("已隔离·仅关联" if imp.was_quarantined else "未隔离·本次隔离"),
                f.reason, f.operator,
            ])
    else:
        writer.writerow(["无相交故障"])
    writer.writerow([])
    writer.writerow(["三、结论说明（仅列事实，任何情况下均不构成合规证明；"
                     "报告为生成时刻的一致性快照，不改变隔离、交接与历史审计）"])
    for i, reason in enumerate(report["reasons"], 1):
        writer.writerow([f"{i}.", reason])
    return buf.getvalue()


@app.get("/samples/{sample_id}/evidence.csv")
def evidence_report_csv(sample_id: int, start: str = "", end: str = "",
                        db: Session = Depends(get_report_db)):
    try:
        report = _evidence_context(sample_id, start, end, db)
    except DomainError as e:
        return PlainTextResponse(f"证据报告无法生成：{e.message}\n",
                                 status_code=e.status_code,
                                 media_type="text/plain; charset=utf-8")
    stamp = report["generated_at"].astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"evidence_{report['sample'].code}_{stamp}.csv"
    return Response(
        content=_evidence_csv(report), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


# ---------------------------------------------------------------- 操作

@app.post("/samples")
def create_sample(code: str = Form(...), temp_min: float = Form(...),
                  temp_max: float = Form(...), location: str = Form(...),
                  operator: str = Form(""), db: Session = Depends(get_db)):
    try:
        sample = services.register_sample(db, code, temp_min, temp_max,
                                          location, operator or None)
    except DomainError as e:
        return redirect("/samples/new", e.message, "error")
    except IntegrityError:
        db.rollback()
        return redirect("/samples/new", f"样品编号 {code} 已存在", "error")
    return redirect(f"/samples/{sample.id}", "样品录入成功")


@app.post("/samples/{sample_id}/handovers")
def create_handover(sample_id: int, from_person: str = Form(...),
                    to_person: str = Form(...), destination: str = Form(...),
                    note: str = Form(""), db: Session = Depends(get_db)):
    try:
        services.create_handover(db, sample_id, from_person, to_person, destination, note)
    except DomainError as e:
        return redirect(f"/samples/{sample_id}", e.message, "error")
    except OperationalError as e:
        if _is_lock_conflict(e):
            return lock_conflict_redirect(db, f"/samples/{sample_id}", "交接发起")
        raise
    return redirect(f"/samples/{sample_id}", "交接已创建，等待接收人确认（确认前位置不变）")


@app.post("/handovers/{handover_id}/confirm")
def confirm_handover(handover_id: int, db: Session = Depends(get_db)):
    handover = db.get(Handover, handover_id)
    sample_id = handover.sample_id if handover else None
    try:
        services.confirm_handover(db, handover_id)
    except DomainError as e:
        return redirect(f"/samples/{sample_id}" if sample_id else "/", e.message, "error")
    except OperationalError as e:
        if _is_lock_conflict(e):
            return lock_conflict_redirect(db, f"/samples/{sample_id}" if sample_id else "/",
                                          "交接确认")
        raise
    return redirect(f"/samples/{sample_id}", "交接已确认，样品位置已更新")


@app.post("/samples/{sample_id}/readings")
def add_reading(sample_id: int, value: float = Form(...),
                recorded_by: str = Form(...), db: Session = Depends(get_db)):
    try:
        _, out_of_range = services.record_temperature(db, sample_id, value, recorded_by)
    except DomainError as e:
        return redirect(f"/samples/{sample_id}", e.message, "error")
    except OperationalError as e:
        if _is_lock_conflict(e):
            return lock_conflict_redirect(db, f"/samples/{sample_id}", "温度记录")
        raise
    if out_of_range:
        return redirect(f"/samples/{sample_id}",
                        f"读数 {value}°C 越界：样品已立即隔离，待确认交接已失效", "error")
    return redirect(f"/samples/{sample_id}", "温度读数已记录")


@app.post("/readings/{reading_id}/corrections")
def correct_reading(reading_id: int, new_value: float = Form(...),
                    reason: str = Form(...), operator: str = Form(...),
                    db: Session = Depends(get_db)):
    try:
        _, sample_id = services.correct_reading(db, reading_id, new_value, reason, operator)
    except DomainError as e:
        reading = db.get(TemperatureReading, reading_id)
        url = f"/samples/{reading.sample_id}" if reading else "/"
        return redirect(url, e.message, "error")
    return redirect(f"/samples/{sample_id}",
                    "读数更正已保存（原值保留；更正不会自动解除隔离）")


@app.post("/samples/{sample_id}/release")
def release_quarantine(sample_id: int, reason: str = Form(...),
                       released_by: str = Form(...), expected_version: int = Form(...),
                       db: Session = Depends(get_db)):
    try:
        services.release_quarantine(db, sample_id, reason, released_by, expected_version)
    except DomainError as e:
        return redirect(f"/samples/{sample_id}", e.message, "error")
    except OperationalError as e:
        if _is_lock_conflict(e):
            return lock_conflict_redirect(db, f"/samples/{sample_id}", "解除隔离")
        raise
    return redirect(f"/samples/{sample_id}", "隔离已解除，样品恢复在库状态")


@app.post("/samples/{sample_id}/temp-range")
def adjust_temp_range(sample_id: int, new_min: float = Form(...),
                      new_max: float = Form(...), reason: str = Form(...),
                      operator: str = Form(...), expected_version: int = Form(...),
                      db: Session = Depends(get_db)):
    try:
        adj = services.adjust_temp_range(db, sample_id, new_min, new_max,
                                         reason, operator, expected_version)
    except DomainError as e:
        return redirect(f"/samples/{sample_id}", e.message, "error")
    except OperationalError as e:
        if _is_lock_conflict(e):
            return lock_conflict_redirect(db, f"/samples/{sample_id}", "温区调整")
        raise
    if adj.review_action == "QUARANTINED":
        return redirect(f"/samples/{sample_id}",
                        f"温区已调整为 [{adj.new_min}, {adj.new_max}]°C；复核越界：最新读数 "
                        f"#{adj.review_reading_id} 最终更正值 {adj.review_value}°C 超出新温区，"
                        "样品已立即隔离，待确认交接已失效", "error")
    if adj.review_action == "ALREADY_QUARANTINED":
        return redirect(f"/samples/{sample_id}",
                        f"温区已调整为 [{adj.new_min}, {adj.new_max}]°C；复核越界：最新读数 "
                        f"#{adj.review_reading_id} 最终更正值 {adj.review_value}°C 超出新温区，"
                        "样品本已隔离，仅记录复核结果，未重复隔离", "error")
    if adj.review_action == "IN_RANGE":
        return redirect(f"/samples/{sample_id}",
                        f"温区已调整为 [{adj.new_min}, {adj.new_max}]°C；复核：最新读数 "
                        f"#{adj.review_reading_id} 最终更正值 {adj.review_value}°C 在新温区内")
    return redirect(f"/samples/{sample_id}",
                    f"温区已调整为 [{adj.new_min}, {adj.new_max}]°C（暂无温度读数，仅更新温区）")


@app.post("/failures")
def create_failure(code: str = Form(...), location: str = Form(...),
                   start_time: str = Form(...), end_time: str = Form(...),
                   reason: str = Form(...), operator: str = Form(...),
                   db: Session = Depends(get_db)):
    try:
        failure, created, impacts = services.register_failure(
            db, code, location, start_time, end_time, reason, operator)
    except DomainError as e:
        return redirect("/failures/new", e.message, "error")
    except OperationalError as e:
        if _is_lock_conflict(e):
            return lock_conflict_redirect(db, "/failures/new", "故障登记")
        raise
    if not created:
        return redirect(f"/failures/{failure.id}",
                        f"故障编号 {failure.code} 已登记过，已返回原结果（受影响样品 {len(impacts)} 个，清单未变更）")
    if impacts:
        return redirect(f"/failures/{failure.id}",
                        f"故障 {failure.code} 登记完成：固定受影响样品 {len(impacts)} 个，"
                        "登记时未隔离的样品已隔离、其待确认交接已失效")
    return redirect(f"/failures/{failure.id}",
                    f"故障 {failure.code} 登记完成：无样品在该库位与故障时段重叠，已保存空影响清单",
                    "success")


# ---------------------------------------------------------------- 导出

def _audit_csv(audits) -> str:
    buf = io.StringIO()
    buf.write("﻿")  # BOM，便于 Excel 打开中文
    writer = csv.writer(buf)
    writer.writerow(["id", "时间", "事件类型", "操作人", "详情"])
    for a in audits:
        writer.writerow([
            a.id,
            a.created_at.isoformat() if a.created_at else "",
            EVENT_NAMES.get(a.event_type, a.event_type),
            a.actor or "",
            json.dumps(a.detail or {}, ensure_ascii=False),
        ])
    return buf.getvalue()


@app.get("/samples/{sample_id}/export.csv")
def export_sample(sample_id: int, db: Session = Depends(get_db)):
    sample = db.get(Sample, sample_id)
    if sample is None:
        return redirect("/", "样品不存在", "error")
    audits = db.scalars(
        select(AuditLog).where(AuditLog.sample_id == sample_id)
        .order_by(AuditLog.id)).all()
    return Response(
        content=_audit_csv(audits), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="audit_{sample.code}.csv"'})


@app.get("/export.csv")
def export_all(db: Session = Depends(get_db)):
    audits = db.scalars(select(AuditLog).order_by(AuditLog.id)).all()
    return Response(
        content=_audit_csv(audits), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="audit_all.csv"'})


@app.get("/health")
def health():
    return {"status": "ok"}
