import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://lab:labpass@localhost:5432/labdb",
)

_connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    # 本地 SQLite 调试：写锁冲突时等待而非立即报 database is locked
    _connect_args = {"timeout": 30}

engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args=_connect_args)

if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA busy_timeout=30000")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base = declarative_base()

# 历史温控证据报告使用的一致性快照会话：
# PostgreSQL 下以 REPEATABLE READ 只读事务打开，报告全部读取共享同一数据视图，
# 期间他人并发提交的读数更正、温区调整、故障登记不会造成报告内部自相矛盾；
# 只读事务不持表写锁，不阻塞任何业务写入。
# SQLite（本地调试）复用默认引擎，在会话打开后显式开启一个延迟读事务
# （pysqlite 默认会在每条 SELECT 后结束事务，无法跨语句持锁）：回滚日志模式下
# SHARED 锁持续到会话关闭，期间的写入提交排队等待（busy_timeout 超时才失败），
# 从而报告的多条读取也来自同一视图。纯只读，绝不影响隔离、交接与历史审计。
if DATABASE_URL.startswith("postgresql"):
    report_engine = create_engine(
        DATABASE_URL, pool_pre_ping=True,
        execution_options={"postgresql_readonly": True,
                           "isolation_level": "REPEATABLE READ"})
else:
    report_engine = engine

ReportSessionLocal = sessionmaker(bind=report_engine, autoflush=False,
                                  expire_on_commit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_report_db():
    """证据报告专用：单一只读事务内完成全部读取，保证一致数据视图。

    PostgreSQL 首次查询即开启 REPEATABLE READ 只读事务并固定快照；
    SQLite 显式 BEGIN 持有 SHARED 锁到会话结束。会话关闭时释放事务，
    不写入任何数据，不改变隔离、交接与历史审计。
    """
    db = ReportSessionLocal()
    dbapi = None
    if not DATABASE_URL.startswith("postgresql"):
        # 退出 pysqlite 的自动事务层并显式开启延迟事务，使跨 SELECT 持续持锁；
        # 结束时显式 ROLLBACK 释放 SHARED 锁并归还连接。
        dbapi = db.connection().connection
        dbapi.isolation_level = None
        dbapi.cursor().execute("BEGIN")
    try:
        yield db
    finally:
        if dbapi is not None:
            try:
                dbapi.cursor().execute("ROLLBACK")
            except Exception:
                pass
        db.close()
