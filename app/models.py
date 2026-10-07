from sqlalchemy import (JSON, Boolean, Column, DateTime, Float, ForeignKey,
                        Integer, String, func)

from .database import Base


class Sample(Base):
    __tablename__ = "samples"

    id = Column(Integer, primary_key=True)
    code = Column(String(64), unique=True, nullable=False, index=True)
    temp_min = Column(Float, nullable=False)
    temp_max = Column(Float, nullable=False)
    current_location = Column(String(128), nullable=False)
    # 登记位置：与已确认交接一起构成样品的位置时间线（旧库通过轻量迁移回填）
    initial_location = Column(String(128), nullable=True)
    status = Column(String(16), nullable=False, default="ACTIVE")  # ACTIVE / QUARANTINED
    version = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Handover(Base):
    """交接单：接收人确认前样品位置不变；越界隔离/设备故障会使 PENDING 交接失效。"""

    __tablename__ = "handovers"

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False, index=True)
    from_person = Column(String(64), nullable=False)
    to_person = Column(String(64), nullable=False)
    destination = Column(String(128), nullable=False)
    note = Column(String(256), default="")
    status = Column(String(16), nullable=False, default="PENDING")  # PENDING / CONFIRMED / INVALIDATED
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    decided_at = Column(DateTime(timezone=True))
    # 因设备故障失效时记录来源故障，便于在故障页展示隔离依据
    failure_id = Column(Integer, ForeignKey("equipment_failures.id"), nullable=True, index=True)


class TemperatureReading(Base):
    """温度读数：原始值永不修改，更正通过 ReadingCorrection 追加。"""

    __tablename__ = "temperature_readings"

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False, index=True)
    value = Column(Float, nullable=False)
    recorded_by = Column(String(64), nullable=False)
    is_out_of_range = Column(Boolean, nullable=False, default=False)
    recorded_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ReadingCorrection(Base):
    """读数更正：保留原值、新值、理由与操作者；不会自动解除隔离。"""

    __tablename__ = "reading_corrections"

    id = Column(Integer, primary_key=True)
    reading_id = Column(Integer, ForeignKey("temperature_readings.id"), nullable=False, index=True)
    old_value = Column(Float, nullable=False)
    new_value = Column(Float, nullable=False)
    reason = Column(String(256), nullable=False)
    operator = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class QuarantineRecord(Base):
    """隔离记录：越界或设备故障时创建；仅能通过注明理由的复核结论解除。"""

    __tablename__ = "quarantine_records"

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False, index=True)
    reason = Column(String(256), nullable=False)
    status = Column(String(16), nullable=False, default="ACTIVE")  # ACTIVE / RELEASED
    quarantined_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    released_at = Column(DateTime(timezone=True))
    release_reason = Column(String(256))
    released_by = Column(String(64))
    # 因设备故障隔离时关联来源故障（隔离依据）；越界隔离为空
    failure_id = Column(Integer, ForeignKey("equipment_failures.id"), nullable=True, index=True)


class EquipmentFailure(Base):
    """冷库设备故障登记：编号唯一；受影响样品清单一次性固定，不随后续状态变化。"""

    __tablename__ = "equipment_failures"

    id = Column(Integer, primary_key=True)
    code = Column(String(64), unique=True, nullable=False, index=True)  # 故障编号
    location = Column(String(128), nullable=False)                     # 库位
    start_time = Column(DateTime(timezone=True), nullable=False)       # 故障开始（左闭）
    end_time = Column(DateTime(timezone=True), nullable=False)         # 故障结束（右开，<= 提交时刻）
    reason = Column(String(256), nullable=False)                       # 故障原因
    operator = Column(String(64), nullable=False)                      # 操作者
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class FailureImpact(Base):
    """故障影响清单：登记时固定。同一样品与故障时段有多段位置重叠时逐段各存一行。"""

    __tablename__ = "failure_impacts"

    id = Column(Integer, primary_key=True)
    failure_id = Column(Integer, ForeignKey("equipment_failures.id"),
                        nullable=False, index=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False, index=True)
    overlap_start = Column(DateTime(timezone=True), nullable=False)  # 重叠起 [
    overlap_end = Column(DateTime(timezone=True), nullable=False)    # 重叠止 )
    segment_start = Column(DateTime(timezone=True), nullable=False)  # 该位置时段起
    segment_end = Column(DateTime(timezone=True), nullable=True)     # 该位置时段止（在库至今为空）
    was_quarantined = Column(Boolean, nullable=False, default=False)  # 登记时是否已隔离
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class TempRangeAdjustment(Base):
    """温区调整：提交时刻生效；保存旧/新温区、理由、操作者、生效时间与复核结果。

    历史读数原值、更正链、当时越界判定与已形成的隔离记录均不回写；
    放宽温区也不自动解除隔离。
    """

    __tablename__ = "temp_range_adjustments"

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False, index=True)
    old_min = Column(Float, nullable=False)
    old_max = Column(Float, nullable=False)
    new_min = Column(Float, nullable=False)
    new_max = Column(Float, nullable=False)
    reason = Column(String(256), nullable=False)
    operator = Column(String(64), nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)  # 生效时间 = 提交时刻
    # 提交时复核：按最新一条读数的最终更正值对照新温区（无读数则三项为空/默认值）
    review_reading_id = Column(Integer, ForeignKey("temperature_readings.id"), nullable=True)
    review_value = Column(Float, nullable=True)        # 最新读数的最终更正值
    review_out_of_range = Column(Boolean, nullable=False, default=False)
    # NO_READING 无读数仅更新 / IN_RANGE 复核未越界 /
    # QUARANTINED 复核越界本次隔离 / ALREADY_QUARANTINED 已隔离仅记录复核结果
    review_action = Column(String(24), nullable=False, default="NO_READING")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AuditLog(Base):
    """审计日志：所有状态变更的完整流水，可导出。"""

    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False, index=True)
    event_type = Column(String(32), nullable=False)
    actor = Column(String(64))
    detail = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
