"""业务逻辑：所有状态变更均在事务内完成。

并发控制策略：
- 样品状态变更前先 SELECT ... FOR UPDATE 锁定样品行，串行化互斥操作；
- 交接确认、隔离解除使用条件 UPDATE（WHERE status=... / version=...），
  只有基于最新状态的操作才能生效，rowcount=0 即冲突，返回明确提示；
  温区调整把带版本号的条件 UPDATE 作为事务第一条语句（PG 行锁 /
  SQLite 写库锁一次到位），版本过期即 409 冲突，不留下部分变更。
- 设备故障登记会影响一批样品：在事务的第一步（取任何行锁之前）对相关表执行
  LOCK TABLE ... IN EXCLUSIVE MODE（PostgreSQL）。EXCLUSIVE 与 ROW SHARE
  （SELECT FOR UPDATE）和 ROW EXCLUSIVE（INSERT/UPDATE/DELETE）都互斥，
  因此登记期间交接确认/录温/样品登记一律排队等其提交，且由于表锁先于行锁获取，
  不会形成「对方持行锁等表锁、我方持表锁等行锁」的死锁；普通只读页面访问
  （ACCESS SHARE）不受阻塞。SQLite 无表锁语法，依赖单写库锁（首次 INSERT 后
  持有写锁）+ busy_timeout 等待。整段登记在单事务内原子提交，任何失败整体
  回滚，不会留下部分清单或部分状态变更。

历史温控证据报告为只读功能：PostgreSQL 下在事务第一条语句设置
「ISOLATION LEVEL REPEATABLE READ, READ ONLY」，整份报告（读数、更正链、
温区调整、位置交接、故障影响清单）取自同一个一致性快照，报告期间并发的读数
更正或故障登记不会混入；READ ONLY 从数据库层保证报告不会改动隔离、交接与
审计；普通快照读取与写事务互不阻塞（ACCESS SHARE 不与 EXCLUSIVE 表锁冲突）。
SQLite 本地调试无对应语法，依赖单写库锁与只读事务快照，同事务内读取同样一致。
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import func, select, text, update
from sqlalchemy.orm import Session

from .models import (AuditLog, EquipmentFailure, FailureImpact, Handover,
                     QuarantineRecord, ReadingCorrection, Sample,
                     TempRangeAdjustment, TemperatureReading)


class DomainError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class ConflictError(DomainError):
    """状态冲突（他人已先行变更），HTTP 409。"""

    def __init__(self, message: str):
        super().__init__(message, 409)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _audit(db: Session, sample_id: int, event_type: str, actor: str | None, detail: dict):
    db.add(AuditLog(sample_id=sample_id, event_type=event_type,
                    actor=actor, detail=detail or {}))


def _lock_sample(db: Session, sample_id: int) -> Sample:
    sample = db.execute(
        select(Sample).where(Sample.id == sample_id).with_for_update()
    ).scalar_one_or_none()
    if sample is None:
        raise DomainError("样品不存在", 404)
    return sample


# ---------------------------------------------------------------- 样品

def register_sample(db: Session, code: str, temp_min: float, temp_max: float,
                    location: str, operator: str | None = None) -> Sample:
    code = (code or "").strip()
    location = (location or "").strip()
    if not code:
        raise DomainError("样品编号不能为空")
    if temp_min >= temp_max:
        raise DomainError("允许温区下限必须小于上限")
    if not location:
        raise DomainError("当前位置不能为空")
    if db.scalar(select(Sample).where(Sample.code == code)):
        raise ConflictError(f"样品编号 {code} 已存在")
    sample = Sample(code=code, temp_min=temp_min, temp_max=temp_max,
                    current_location=location, initial_location=location,
                    status="ACTIVE", version=1)
    db.add(sample)
    db.flush()
    _audit(db, sample.id, "SAMPLE_REGISTERED", operator or "system", {
        "code": code, "temp_min": temp_min, "temp_max": temp_max, "location": location})
    db.commit()
    return sample


# ---------------------------------------------------------------- 交接

def create_handover(db: Session, sample_id: int, from_person: str, to_person: str,
                    destination: str, note: str = "") -> Handover:
    sample = _lock_sample(db, sample_id)
    from_person = (from_person or "").strip()
    to_person = (to_person or "").strip()
    destination = (destination or "").strip()
    if not from_person or not to_person or not destination:
        raise DomainError("交出人、接收人、目的地均为必填")
    if sample.status == "QUARANTINED":
        raise ConflictError("样品处于隔离状态，禁止发起转移")
    pending = db.scalar(
        select(func.count()).select_from(Handover).where(
            Handover.sample_id == sample_id, Handover.status == "PENDING")
    )
    if pending:
        raise ConflictError("已存在待确认的交接，确认或失效前不能重复发起")
    # 条件写入：仅当样品仍为 ACTIVE 时才落交接，防止与并发隔离
    # （越界录温 / 设备故障登记）交错产生「隔离中仍挂着待确认交接」。
    # PostgreSQL 下该路径已被故障登记的 EXCLUSIVE 表锁串行化，此条件为双保险。
    res = db.execute(
        update(Sample)
        .where(Sample.id == sample_id, Sample.status == "ACTIVE")
        .values(version=Sample.version + 1)
    )
    if res.rowcount == 0:
        db.rollback()
        raise ConflictError("样品刚被隔离，交接发起未生效，请刷新查看最新状态")
    handover = Handover(sample_id=sample_id, from_person=from_person,
                        to_person=to_person, destination=destination,
                        note=(note or "").strip(), status="PENDING")
    db.add(handover)
    db.flush()
    _audit(db, sample_id, "HANDOVER_CREATED", from_person, {
        "handover_id": handover.id, "from": from_person, "to": to_person,
        "destination": destination, "note": note})
    db.commit()
    return handover


def confirm_handover(db: Session, handover_id: int, actor: str | None = None) -> Handover:
    handover = db.get(Handover, handover_id)
    if handover is None:
        raise DomainError("交接记录不存在", 404)
    # 先锁样品行，与温度记录/隔离串行化
    sample = _lock_sample(db, handover.sample_id)
    if sample.status == "QUARANTINED":
        raise ConflictError("样品已隔离，待确认交接已失效，无法确认")
    # 条件更新：只有仍处于 PENDING 的交接才能被确认，并发下仅一个请求生效
    res = db.execute(
        update(Handover)
        .where(Handover.id == handover_id, Handover.status == "PENDING")
        .values(status="CONFIRMED", decided_at=_now())
    )
    if res.rowcount == 0:
        db.rollback()
        raise ConflictError("该交接已被确认或已失效，本次操作未生效，请刷新查看最新状态")
    sample.current_location = handover.destination
    sample.version += 1
    _audit(db, sample.id, "HANDOVER_CONFIRMED", actor or handover.to_person, {
        "handover_id": handover.id, "from": handover.from_person,
        "to": handover.to_person, "destination": handover.destination})
    db.commit()
    return handover


# ---------------------------------------------------------------- 温度

def record_temperature(db: Session, sample_id: int, value: float,
                       recorded_by: str) -> tuple[TemperatureReading, bool]:
    sample = _lock_sample(db, sample_id)
    recorded_by = (recorded_by or "").strip() or "unknown"
    out_of_range = value < sample.temp_min or value > sample.temp_max
    reading = TemperatureReading(sample_id=sample_id, value=value,
                                 recorded_by=recorded_by, is_out_of_range=out_of_range)
    db.add(reading)
    db.flush()
    _audit(db, sample_id, "READING_RECORDED", recorded_by, {
        "reading_id": reading.id, "value": value,
        "allowed": [sample.temp_min, sample.temp_max], "out_of_range": out_of_range})
    if out_of_range:
        # 越界：待确认交接立即失效
        pending = db.scalars(
            select(Handover).where(Handover.sample_id == sample_id,
                                   Handover.status == "PENDING")
        ).all()
        for h in pending:
            h.status = "INVALIDATED"
            h.decided_at = _now()
            _audit(db, sample_id, "HANDOVER_INVALIDATED", "system", {
                "handover_id": h.id, "reason": f"温度越界读数 #{reading.id}"})
        # 越界：样品立即隔离
        if sample.status != "QUARANTINED":
            sample.status = "QUARANTINED"
            sample.version += 1
            db.add(QuarantineRecord(
                sample_id=sample_id,
                reason=f"温度越界：{value}°C 超出允许温区 "
                       f"[{sample.temp_min}, {sample.temp_max}]°C（读数 #{reading.id}）"))
            _audit(db, sample_id, "QUARANTINED", "system", {
                "reading_id": reading.id, "value": value,
                "invalidated_handovers": [h.id for h in pending]})
    db.commit()
    return reading, out_of_range


def correct_reading(db: Session, reading_id: int, new_value: float,
                    reason: str, operator: str) -> tuple[ReadingCorrection, int]:
    reading = db.get(TemperatureReading, reading_id)
    if reading is None:
        raise DomainError("温度读数不存在", 404)
    reason = (reason or "").strip()
    operator = (operator or "").strip()
    if not reason:
        raise DomainError("更正读数必须填写理由")
    if not operator:
        raise DomainError("更正读数必须填写操作者")
    # 锁样品行：与录温、温区调整等基于最新读数/更正状态的操作串行化
    _lock_sample(db, reading.sample_id)
    # 原值 = 上一次更正后的值（或原始值），形成完整更正链
    last = db.scalars(
        select(ReadingCorrection).where(ReadingCorrection.reading_id == reading_id)
        .order_by(ReadingCorrection.id.desc())
    ).first()
    old_value = last.new_value if last else reading.value
    correction = ReadingCorrection(reading_id=reading_id, old_value=old_value,
                                   new_value=new_value, reason=reason, operator=operator)
    db.add(correction)
    db.flush()
    # 注意：更正不会自动解除隔离，也不改变样品状态
    _audit(db, reading.sample_id, "READING_CORRECTED", operator, {
        "reading_id": reading_id, "old_value": old_value, "new_value": new_value,
        "reason": reason, "note": "更正不自动解除隔离"})
    db.commit()
    return correction, reading.sample_id


# ---------------------------------------------------------------- 隔离

def release_quarantine(db: Session, sample_id: int, reason: str,
                       released_by: str, expected_version: int) -> None:
    reason = (reason or "").strip()
    released_by = (released_by or "").strip()
    if not reason:
        raise DomainError("解除隔离必须填写注明理由的复核结论")
    if not released_by:
        raise DomainError("请填写复核人")
    # 条件更新：仅当样品仍处于隔离状态且版本号与页面读取时一致才生效
    res = db.execute(
        update(Sample)
        .where(Sample.id == sample_id,
               Sample.status == "QUARANTINED",
               Sample.version == expected_version)
        .values(status="ACTIVE", version=Sample.version + 1)
    )
    if res.rowcount == 0:
        db.rollback()
        raise ConflictError("样品状态已变更（可能已被他人处理），本次解除未生效，请刷新后重试")
    now = _now()
    active = db.scalars(
        select(QuarantineRecord).where(QuarantineRecord.sample_id == sample_id,
                                       QuarantineRecord.status == "ACTIVE")
    ).all()
    for q in active:
        q.status = "RELEASED"
        q.released_at = now
        q.release_reason = reason
        q.released_by = released_by
    _audit(db, sample_id, "QUARANTINE_RELEASED", released_by, {
        "reason": reason, "quarantine_ids": [q.id for q in active]})
    db.commit()


# ---------------------------------------------------------------- 温区调整

def adjust_temp_range(db: Session, sample_id: int, new_min: float, new_max: float,
                      reason: str, operator: str, expected_version: int
                      ) -> TempRangeAdjustment:
    """调整样品允许温区，提交时刻生效。

    - 保存旧/新温区、理由、操作者与生效时间（TempRangeAdjustment + 审计）；
    - 历史读数原值、更正链、当时越界判定与已形成的隔离记录一律不回写；
    - 提交时按最新一条读数的最终更正值复核新温区：
      无读数只更新温区；越界且未隔离则立即隔离并使待确认交接失效；
      已隔离只记录复核结果，不重复隔离；放宽温区不自动解除隔离；
    - 与录温/更正/交接确认/解除隔离并发时基于最新状态生效：事务第一条语句即
      执行带版本号的条件 UPDATE（PostgreSQL 下取得行锁，SQLite 下立即取得
      写库锁），版本过期 rowcount=0 即 409 冲突，不留下部分变更。
    """
    reason = (reason or "").strip()
    operator = (operator or "").strip()
    if not reason:
        raise DomainError("调整温区必须填写理由")
    if not operator:
        raise DomainError("调整温区必须填写操作者姓名")
    if not new_min < new_max:
        raise DomainError("允许温区下限必须小于上限")
    # 条件更新占位（本事务第一条 SQL）：仅当页面版本与最新状态一致才生效。
    # 该 UPDATE 在 PostgreSQL 下锁定样品行、在 SQLite 下取得写库锁，此后本事务
    # 的复核读取与全部写入都与录温/更正/交接确认/解除隔离串行化；
    # 版本号 SQL 侧自增，避免并发下丢失更新。
    res = db.execute(
        update(Sample)
        .where(Sample.id == sample_id, Sample.version == expected_version)
        .values(version=Sample.version + 1)
    )
    if res.rowcount == 0:
        db.rollback()
        if not db.scalar(select(func.count()).select_from(Sample)
                         .where(Sample.id == sample_id)):
            raise DomainError("样品不存在", 404)
        raise ConflictError("页面版本已过期（样品状态已被其他操作变更），本次调整未生效，请刷新后重试")
    # 已持有样品行锁（PG）/写库锁（SQLite）：读到的是最新已提交状态
    sample = db.get(Sample, sample_id)
    now = _now()
    old_min, old_max = sample.temp_min, sample.temp_max

    # ---- 复核：最新一条读数的最终更正值对照新温区 ----
    reading = db.scalars(
        select(TemperatureReading).where(TemperatureReading.sample_id == sample_id)
        .order_by(TemperatureReading.id.desc())
    ).first()
    review_value: float | None = None
    review_out_of_range = False
    review_action = "NO_READING"
    if reading is not None:
        last = db.scalars(
            select(ReadingCorrection).where(ReadingCorrection.reading_id == reading.id)
            .order_by(ReadingCorrection.id.desc())
        ).first()
        review_value = last.new_value if last else reading.value
        review_out_of_range = review_value < new_min or review_value > new_max
        if review_out_of_range:
            review_action = ("ALREADY_QUARANTINED" if sample.status == "QUARANTINED"
                             else "QUARANTINED")
        else:
            review_action = "IN_RANGE"

    adjustment = TempRangeAdjustment(
        sample_id=sample_id, old_min=old_min, old_max=old_max,
        new_min=new_min, new_max=new_max, reason=reason, operator=operator,
        effective_at=now, review_reading_id=reading.id if reading else None,
        review_value=review_value, review_out_of_range=review_out_of_range,
        review_action=review_action)
    db.add(adjustment)
    db.flush()
    # 版本号已在守卫 UPDATE 中自增，这里只落新温区
    sample.temp_min = new_min
    sample.temp_max = new_max
    _audit(db, sample_id, "TEMP_RANGE_ADJUSTED", operator, {
        "adjustment_id": adjustment.id,
        "old_range": [old_min, old_max], "new_range": [new_min, new_max],
        "reason": reason, "effective_at": now.isoformat(),
        "review": {"reading_id": reading.id if reading else None,
                   "value": review_value, "out_of_range": review_out_of_range,
                   "action": review_action},
        "note": "历史读数与隔离记录不回写；放宽温区不自动解除隔离"})

    if review_action == "QUARANTINED":
        # 复核越界且未隔离：立即隔离并使待确认交接失效（与越界录温同规则）
        pending = db.scalars(
            select(Handover).where(Handover.sample_id == sample_id,
                                   Handover.status == "PENDING")
        ).all()
        for h in pending:
            h.status = "INVALIDATED"
            h.decided_at = now
            _audit(db, sample_id, "HANDOVER_INVALIDATED", operator, {
                "handover_id": h.id,
                "reason": f"温区调整 #{adjustment.id} 复核越界"})
        sample.status = "QUARANTINED"
        db.add(QuarantineRecord(
            sample_id=sample_id,
            reason=f"温区调整后复核越界：最新读数 #{reading.id} 最终更正值 "
                   f"{review_value}°C 超出新温区 [{new_min}, {new_max}]°C"
                   f"（温区调整 #{adjustment.id}）"))
        _audit(db, sample_id, "QUARANTINED", operator, {
            "adjustment_id": adjustment.id, "reading_id": reading.id,
            "value": review_value,
            "invalidated_handovers": [h.id for h in pending]})
    # ALREADY_QUARANTINED：复核结果已随调整记录与审计保存，不重复隔离、不动交接
    db.commit()
    return adjustment


# ---------------------------------------------------------------- 设备故障

def parse_dt(value: str, field: str) -> datetime:
    """解析表单时间为带 UTC 时区的 datetime；支持 'Z' 后缀与不带时区的本地输入（按 UTC 处理）。"""
    value = (value or "").strip()
    if not value:
        raise DomainError(f"{field}不能为空")
    text_value = value.replace("Z", "+00:00") if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(text_value)
    except ValueError:
        raise DomainError(f"{field}时间格式无效，正确格式如 2026-10-06T14:30")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dt_key(dt: datetime | None) -> str:
    """时间字段的比较指纹：统一到 UTC 微秒精度字符串。"""
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).isoformat()


def _same_fields(existing: EquipmentFailure, location: str,
                 start_time: datetime, end_time: datetime,
                 reason: str, operator: str) -> bool:
    """判断重复提交的登记字段是否与既有故障完全一致。"""
    return (existing.location == location
            and _dt_key(existing.start_time) == _dt_key(start_time)
            and _dt_key(existing.end_time) == _dt_key(end_time)
            and existing.reason == reason
            and existing.operator == operator)


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:  # SQLite 取回的时间为无时区值，按 UTC 解释
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _position_segments(db: Session, sample: Sample) -> list[tuple[str, datetime, datetime | None]]:
    """样品由「登记位置 + 已确认交接」形成的位置时间段（左闭右开）。

    第 i 段：[第 i-1 个已确认交接的确认时刻, 第 i 个已确认交接的确认时刻)，
    位置为第 i-1 个交接的目的地；首段从样品登记时刻开始、位于登记位置；
    末段终点为 None，表示在库至今（右端正无穷）。
    待确认、已失效交接均不改变位置，不参与时间线。
    """
    initial = sample.initial_location or sample.current_location
    confirmed = db.scalars(
        select(Handover)
        .where(Handover.sample_id == sample.id, Handover.status == "CONFIRMED")
        .order_by(Handover.decided_at.asc(), Handover.id.asc())
    ).all()
    segments: list[tuple[str, datetime, datetime | None]] = []
    location = initial
    seg_start = _as_utc(sample.created_at)
    for h in confirmed:
        decided = _as_utc(h.decided_at or h.created_at)
        segments.append((location, seg_start, decided))
        location = h.destination
        seg_start = decided
    segments.append((location, seg_start, None))
    return segments


def _lock_failure_tables(db: Session) -> None:
    """PostgreSQL：登记开始时锁定涉及的表，与样品登记/交接确认/录温完全串行化。

    EXCLUSIVE 与 ROW SHARE（SELECT ... FOR UPDATE）及 ROW EXCLUSIVE
    （写操作）互斥，但与 ACCESS SHARE（普通 SELECT）兼容。必须在本事务获取
    任何行锁之前调用，以避免与并发事务已持有的行锁形成死锁。
    SQLite 无对应语法，靠单写库锁（首次 INSERT 后持有）+ busy_timeout，此处不处理。
    """
    if db.get_bind().dialect.name != "postgresql":
        return
    db.execute(text(
        "LOCK TABLE samples, handovers, temperature_readings, quarantine_records, "
        "equipment_failures, failure_impacts, audit_logs IN EXCLUSIVE MODE"))


def register_failure(db: Session, code: str, location: str,
                     start_raw: str, end_raw: str, reason: str, operator: str
                     ) -> tuple[EquipmentFailure, bool, list[FailureImpact]]:
    """登记设备故障并一次性固定受影响样品清单。

    返回 (故障, 是否新建, 影响清单)。相同编号且字段完全一致的重复提交返回原结果；
    编号相同而字段不同抛 ConflictError（409）。任何失败整体回滚。
    """
    # ---- 1. 参数校验（失败不写任何数据）----
    code = (code or "").strip()
    location = (location or "").strip()
    reason = (reason or "").strip()
    operator = (operator or "").strip()
    missing = [name for name, val in (("故障编号", code), ("库位", location),
                                      ("故障原因", reason), ("操作者", operator)) if not val]
    if missing:
        raise DomainError("必填字段缺失：" + "、".join(missing))
    start_time = parse_dt(start_raw, "故障开始时间")
    end_time = parse_dt(end_raw, "故障结束时间")
    if not start_time < end_time:
        raise DomainError("起止时间无效：开始时间必须早于结束时间（时间段左闭右开）")
    if end_time > _now():
        raise DomainError("故障结束时间不得晚于提交时刻")

    # ---- 2. 串行化：先锁表（PG），再查重；SQLite 靠单写库锁（首次写入后持有）----
    _lock_failure_tables(db)
    existing = db.scalar(select(EquipmentFailure).where(EquipmentFailure.code == code))
    if existing is not None:
        if not _same_fields(existing, location, start_time, end_time, reason, operator):
            raise ConflictError(f"故障编号 {code} 已存在且登记字段不同，存在冲突；请核对后使用新编号")
        impacts = db.scalars(
            select(FailureImpact).where(FailureImpact.failure_id == existing.id)
            .order_by(FailureImpact.id)).all()
        return existing, False, impacts

    failure = EquipmentFailure(code=code, location=location, start_time=start_time,
                               end_time=end_time, reason=reason, operator=operator)
    db.add(failure)
    try:
        db.flush()  # 触发唯一约束；SQLite 下同时取得写库锁
    except Exception:
        # 并发下编号可能已被其他事务占用：回滚后按幂等/冲突规则处理
        db.rollback()
        existing = db.scalar(select(EquipmentFailure).where(EquipmentFailure.code == code))
        if existing is None:
            raise DomainError("故障登记与其他提交并发冲突，请稍后重试")
        if not _same_fields(existing, location, start_time, end_time, reason, operator):
            raise ConflictError(f"故障编号 {code} 已存在且登记字段不同，存在冲突；请核对后使用新编号")
        impacts = db.scalars(
            select(FailureImpact).where(FailureImpact.failure_id == existing.id)
            .order_by(FailureImpact.id)).all()
        return existing, False, impacts

    # ---- 3. 逐样品加行锁、基于最新状态计算与故障 [start,end) × 库位 的重叠 ----
    samples = db.scalars(select(Sample).order_by(Sample.id)).all()
    impacts: list[FailureImpact] = []
    for s in samples:
        sample = _lock_sample(db, s.id)  # FOR UPDATE：与交接确认/录温互斥
        for seg_loc, seg_start, seg_end in _position_segments(db, sample):
            if seg_loc != location:
                continue
            overlap_start = max(seg_start, start_time)
            overlap_end = min(seg_end or end_time, end_time)
            # 左闭右开：overlap_start < overlap_end 才算有交集
            if overlap_start >= overlap_end:
                continue
            was_quarantined = sample.status == "QUARANTINED"
            impact = FailureImpact(
                failure_id=failure.id, sample_id=sample.id,
                overlap_start=overlap_start, overlap_end=overlap_end,
                segment_start=seg_start, segment_end=seg_end,
                was_quarantined=was_quarantined)
            db.add(impact)
            impacts.append(impact)
            db.flush()
            if was_quarantined:
                # 已隔离样品只关联本故障：不重复生成有效隔离、不动待确认交接
                _audit(db, sample.id, "FAULT_LINKED", operator, {
                    "failure_id": failure.id, "failure_code": code,
                    "overlap": [overlap_start.isoformat(), overlap_end.isoformat()],
                    "note": "登记时样品已隔离，仅关联故障，不重复隔离"})
                continue

    # ---- 4. 对本次清单中登记时未隔离的样品：隔离并使其待确认交接失效 ----
    # 行锁在第 3 步已按 id 顺序获取并在本事务内保持，这里直接基于最新状态处理。
    affected_sample_ids = sorted({imp.sample_id for imp in impacts})
    for sid in affected_sample_ids:
        sample = db.get(Sample, sid)
        if sample.status == "QUARANTINED":
            continue  # 登记时已隔离（was_quarantined=True 的段），只关联
        sample.status = "QUARANTINED"
        sample.version += 1
        pending = db.scalars(
            select(Handover).where(Handover.sample_id == sid, Handover.status == "PENDING")
        ).all()
        invalidated_ids: list[int] = []
        for h in pending:
            h.status = "INVALIDATED"
            h.decided_at = _now()
            h.failure_id = failure.id
            invalidated_ids.append(h.id)
        quarantine = QuarantineRecord(
            sample_id=sid, failure_id=failure.id,
            reason=f"设备故障 {code}：{location} 于 "
                   f"{start_time.strftime('%Y-%m-%d %H:%M')}~"
                   f"{end_time.strftime('%Y-%m-%d %H:%M')} 发生故障，"
                   f"样品位置时段与故障时段重叠")
        db.add(quarantine)
        db.flush()
        overlap = [(imp.overlap_start.isoformat(), imp.overlap_end.isoformat())
                   for imp in impacts if imp.sample_id == sid]
        _audit(db, sid, "QUARANTINED", operator, {
            "failure_id": failure.id, "failure_code": code,
            "quarantine_id": quarantine.id,
            "invalidated_handovers": invalidated_ids,
            "overlaps": overlap,
            "reason": "设备故障登记：受影响样品隔离"})
        for hid in invalidated_ids:
            _audit(db, sid, "HANDOVER_INVALIDATED", operator, {
                "handover_id": hid, "failure_id": failure.id,
                "reason": f"设备故障 {code} 影响，样品隔离"})
        _audit(db, sid, "FAULT_AFFECTED", operator, {
            "failure_id": failure.id, "failure_code": code,
            "overlaps": overlap, "quarantine_id": quarantine.id,
            "invalidated_handovers": invalidated_ids})

    # ---- 5. 原子提交；空清单同样保存（仅故障行，无影响行）----
    db.commit()
    return failure, True, impacts


# ---------------------------------------------------------------- 历史温控证据报告

@dataclass
class ReportReading:
    """证据报告中的一条窗内读数（值与判定均按报告生成时的快照重放）。"""

    id: int
    recorded_at: datetime
    original_value: float                 # 读数原值（永不修改）
    final_value: float                    # 最终更正值（无更正即原值）
    correction_count: int                 # 更正链长度
    range_min: float                      # 记录时生效的允许温区下限
    range_max: float                      # 记录时生效的允许温区上限
    location: str                         # 记录时所在库位
    original_out_of_range: bool           # 原始越界判定（历史留痕，保留不回写）
    final_out_of_range: bool              # 按最终更正值 + 记录时温区的越界标示


@dataclass
class ReportFailure:
    """证据报告中与查询窗相交的已固定故障影响（逐段一行）及交集时段。"""

    failure_id: int
    failure_code: str
    failure_location: str
    failure_start: datetime
    failure_end: datetime
    reason: str
    operator: str
    overlap_start: datetime               # 与查询窗的交集起（左闭）
    overlap_end: datetime                 # 与查询窗的交集止（右开）
    impact_overlap_start: datetime        # 故障登记时固定的位置重叠时段起
    impact_overlap_end: datetime          # 故障登记时固定的位置重叠时段止
    was_quarantined: bool                 # 故障登记时样品是否已隔离


@dataclass
class EvidenceReport:
    """单个样品、单个 [start, end) 查询窗的历史温控证据报告（一致性快照）。

    verdict 取值：
    - COMPLIANT：窗内有读数，全部按最终值在记录时温区内，且无相交故障；
    - NON_COMPLIANT：存在按最终值越界的读数，或存在与查询窗相交的已固定故障；
    - INCONCLUSIVE：窗内无读数——即便没有故障，也不得据此判为合规。
    """

    sample_id: int
    sample_code: str
    start: datetime
    end: datetime
    generated_at: datetime
    readings: list[ReportReading] = field(default_factory=list)
    failures: list[ReportFailure] = field(default_factory=list)
    final_oor_count: int = 0
    original_oor_count: int = 0
    verdict: str = "INCONCLUSIVE"


def _effective_range_at(adjustments: list[TempRangeAdjustment], sample: Sample,
                        at: datetime) -> tuple[float, float]:
    """读数时刻 at 生效的允许温区。

    取生效时间不晚于 at 的最后一次温区调整；同一生效时刻按调整记录 ID
    较大者优先（与「同一时刻按记录 ID 较大者优先」的全局约定一致）。
    早于全部调整的读数，其生效温区为最早一次调整保存的旧温区（即登记温区）；
    从未调整过时才取样品当前温区（此时它就等于登记温区）。
    """
    chosen: TempRangeAdjustment | None = None
    for adj in adjustments:  # 已按 (effective_at, id) 升序
        if _as_utc(adj.effective_at) <= at:
            chosen = adj  # 升序遍历：最后满足条件者即 (effective_at, id) 最大者
    if chosen is not None:
        return chosen.new_min, chosen.new_max
    if adjustments:
        return adjustments[0].old_min, adjustments[0].old_max
    return sample.temp_min, sample.temp_max


def _begin_snapshot_read(db: Session) -> None:
    """开启只读一致性快照事务（BEGIN 必须是事务第一条语句）。

    随后本事务所有 SELECT 共享同一快照，报告期间并发的读数更正或故障登记
    （已提交）不会混入，HTML 与 CSV 之间也不会出现内容矛盾：
    - PostgreSQL：通过执行选项开启 REPEATABLE READ + READ ONLY 事务（由方言
      在事务开始时下发），READ ONLY 从数据库层禁止任何写操作；快照读与写事务
      互不阻塞（ACCESS SHARE 不与登记使用的 EXCLUSIVE 表锁冲突）。
    - SQLite：显式 BEGIN（deferred）以共享锁开启读事务，单连接内后续读全部
      基于同一快照；若写者已持锁则立即得到 SQLITE_BUSY（OperationalError），
      由路由按既有锁冲突处理提示重试，绝不退回「逐条读、中间被改写」的视图。

    先 rollback() 结束 Session 上可能残留的（只读）事务，保证隔离级别在
    事务开始前设置；报告自身从不写入，回滚不会丢弃任何更改。
    """
    db.rollback()
    if db.get_bind().dialect.name == "postgresql":
        db.connection(execution_options={
            "isolation_level": "REPEATABLE READ",
            "postgresql_readonly": True})
    else:
        db.execute(text("BEGIN"))


def build_evidence_report(db: Session, sample_id: int,
                          start: datetime, end: datetime) -> EvidenceReport:
    """基于单一只读快照构建 [start, end) 内的历史温控证据报告。

    start/end 统一为带 UTC 时区的 datetime；起始须早于结束、结束不得晚于
    当前时刻，否则 DomainError(400)；样品不存在 DomainError(404)。
    调用方通常在只读端点中使用：请求结束时依赖 Session 关闭释放事务。
    同一 Session 再次调用前需先 rollback() 结束上一个快照事务。
    """
    # ---- 0. 时间窗校验（左闭右开；结束不晚于当前时间）----
    start = _as_utc(start)
    end = _as_utc(end)
    if not start < end:
        raise DomainError("时间窗无效：起始时间必须早于结束时间（时间窗左闭右开）")
    if end > _now():
        raise DomainError("时间窗无效：结束时间不得晚于当前时间")
    # ---- 1. 快照事务第一条语句（PG）：之后所有读取同一视图 ----
    _begin_snapshot_read(db)
    sample = db.get(Sample, sample_id)
    if sample is None:
        raise DomainError("样品不存在", 404)

    # ---- 2. 快照内取齐重放所需的全部原始事实 ----
    all_readings = db.scalars(
        select(TemperatureReading).where(TemperatureReading.sample_id == sample_id)
        .order_by(TemperatureReading.id.asc())).all()
    corrections = db.scalars(
        select(ReadingCorrection).join(TemperatureReading)
        .where(TemperatureReading.sample_id == sample_id)
        .order_by(ReadingCorrection.id.asc())).all()
    adjustments = db.scalars(
        select(TempRangeAdjustment).where(TempRangeAdjustment.sample_id == sample_id)
        .order_by(TempRangeAdjustment.effective_at.asc(),
                  TempRangeAdjustment.id.asc())).all()
    # 位置时间段：登记位置 + 已确认交接，确认时刻同时刻按交接 ID 较大者优先
    segments = _position_segments(db, sample)
    # 已固定的故障影响清单：登记后不随后续位置/状态变化，只读引用即可
    impacts = db.scalars(
        select(FailureImpact).where(FailureImpact.sample_id == sample_id)
        .order_by(FailureImpact.id.asc())).all()
    failure_rows = db.scalars(
        select(EquipmentFailure)
        .where(EquipmentFailure.id.in_(sorted({i.failure_id for i in impacts})))
        .order_by(EquipmentFailure.id.asc())).all() if impacts else []
    failure_map = {f.id: f for f in failure_rows}

    # ---- 3. 逐条重放窗内读数 ----
    corrections_by_reading: dict[int, list[ReadingCorrection]] = {}
    for c in corrections:
        corrections_by_reading.setdefault(c.reading_id, []).append(c)

    report_readings: list[ReportReading] = []
    for r in all_readings:
        recorded_at = _as_utc(r.recorded_at)
        if not (start <= recorded_at < end):  # 左闭右开
            continue
        chain = corrections_by_reading.get(r.id, [])
        final_value = chain[-1].new_value if chain else r.value
        range_min, range_max = _effective_range_at(adjustments, sample, recorded_at)
        location = sample.initial_location or sample.current_location
        for seg_loc, seg_start, seg_end in segments:
            # 段为左闭右开；末段 seg_end 为 None 表示在库至今
            if seg_start <= recorded_at and (seg_end is None or recorded_at < seg_end):
                location = seg_loc
                break
        final_oor = final_value < range_min or final_value > range_max
        report_readings.append(ReportReading(
            id=r.id, recorded_at=recorded_at, original_value=r.value,
            final_value=final_value, correction_count=len(chain),
            range_min=range_min, range_max=range_max, location=location,
            original_out_of_range=bool(r.is_out_of_range),
            final_out_of_range=final_oor))

    # ---- 4. 已固定故障影响中与查询窗相交者：交集时段 = 窗 ∩ 固定重叠时段 ----
    report_failures: list[ReportFailure] = []
    for imp in impacts:
        f = failure_map.get(imp.failure_id)
        if f is None:  # 理论上不会发生（外键），防御性跳过
            continue
        imp_start = _as_utc(imp.overlap_start)
        imp_end = _as_utc(imp.overlap_end)
        inter_start = max(start, imp_start)
        inter_end = min(end, imp_end)
        if inter_start >= inter_end:  # 左闭右开：无交集
            continue
        report_failures.append(ReportFailure(
            failure_id=f.id, failure_code=f.code, failure_location=f.location,
            failure_start=_as_utc(f.start_time), failure_end=_as_utc(f.end_time),
            reason=f.reason, operator=f.operator,
            overlap_start=inter_start, overlap_end=inter_end,
            impact_overlap_start=imp_start, impact_overlap_end=imp_end,
            was_quarantined=bool(imp.was_quarantined)))
    # 按故障编号、交集起、影响记录 ID 稳定排序
    report_failures.sort(key=lambda x: (x.failure_id, x.overlap_start))

    final_oor_count = sum(1 for r in report_readings if r.final_out_of_range)
    original_oor_count = sum(1 for r in report_readings if r.original_out_of_range)
    if final_oor_count > 0 or report_failures:
        verdict = "NON_COMPLIANT"
    elif report_readings:
        verdict = "COMPLIANT"
    else:
        verdict = "INCONCLUSIVE"  # 窗内无读数：即便无故障也不得判为合规

    return EvidenceReport(
        sample_id=sample_id, sample_code=sample.code, start=start, end=end,
        generated_at=_now(), readings=report_readings, failures=report_failures,
        final_oor_count=final_oor_count, original_oor_count=original_oor_count,
        verdict=verdict)
