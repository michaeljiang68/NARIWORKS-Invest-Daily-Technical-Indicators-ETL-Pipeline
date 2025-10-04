
# postgres.py
# Database connection, session, and table creation utilities for Postgres.
#
# Features:
# - Creates SQLAlchemy engine and session for Postgres
# - Defines declarative base for ORM models
# - Provides ensure_table_exists() to auto-create feature_daily table if missing
# - Uses environment settings for connection parameters
#
# Usage:
#   Import engine, SessionLocal, Base for ORM and DB operations
#   Call ensure_table_exists() at startup to guarantee table exists
#
# Functions:
#   ensure_table_exists(): Checks and creates feature_daily table if not present
#
# Returns:
#   engine: SQLAlchemy engine instance
#   SessionLocal: sessionmaker for DB sessions
#   Base: declarative base for ORM
#   ensure_table_exists: function to check/create table

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.setups import settings
from datetime import date

Base = declarative_base()

__all__ = ["engine", "SessionLocal", "ensure_table_exists", "Base"]

POSTGRES_URL = (
    f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
    f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
)

engine = create_engine(POSTGRES_URL, echo=False, future=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def ensure_table_exists():
    """
    Ensure the feature_daily table exists in the Postgres database.
    If the table does not exist, it will be created with the required schema.

    Table schema:
        - stock_id: TEXT, primary key
        - trade_date: DATE, primary key
        - ma5, ema12, ema26, macd_dif, macd_dea, macd_osc, bb_ma20, bb_std20, bb_upper20, bb_lower20, bb_width20, bb_pos20: DOUBLE PRECISION
        - bb_break_up, bb_break_dn: BOOLEAN
        - etl_run_id: UUID, not null
        - source_version, commit_sha: TEXT
        - created_at, updated_at: TIMESTAMP
        - PRIMARY KEY (stock_id, trade_date)

    Returns:
        bool: True if table exists or was created successfully, False otherwise
    """
    table_name = getattr(settings, "POSTGRES_TABLE", "daily_features_etl")
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        return True
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        stock_id         TEXT        NOT NULL,
        trade_date       DATE        NOT NULL,
        ma5              DOUBLE PRECISION,
        ema12            DOUBLE PRECISION,
        ema26            DOUBLE PRECISION,
        macd_dif         DOUBLE PRECISION,
        macd_dea         DOUBLE PRECISION,
        macd_osc         DOUBLE PRECISION,
        bb_ma20          DOUBLE PRECISION,
        bb_std20         DOUBLE PRECISION,
        bb_upper20       DOUBLE PRECISION,
        bb_lower20       DOUBLE PRECISION,
        bb_width20       DOUBLE PRECISION,
        bb_pos20         DOUBLE PRECISION,
        bb_break_up      BOOLEAN,
        bb_break_dn      BOOLEAN,
        etl_run_id       UUID        NOT NULL,
        source_version   TEXT,
        commit_sha       TEXT,
        created_at       TIMESTAMP DEFAULT NOW(),
        updated_at       TIMESTAMP DEFAULT NOW(),
        CONSTRAINT feature_daily_pkey PRIMARY KEY (stock_id, trade_date)
    );
    '''
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()
