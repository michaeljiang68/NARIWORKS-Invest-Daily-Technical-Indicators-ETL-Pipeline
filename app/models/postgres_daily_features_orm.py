from sqlalchemy import Column, String, Date, Double, Boolean, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, insert
from app.db.postgres import SessionLocal
from app.core.logger import logger
from sqlalchemy.ext.declarative import declarative_base
from app.core.setups import settings

Base = declarative_base()

class FeatureDaily(Base):
    table_name = getattr(settings, "POSTGRES_TABLE", "daily_features_etl")
    __tablename__ = table_name
    """
    SQLAlchemy ORM class for the feature_daily table, which stores daily calculated technical indicators for each stock.

    Table columns:
        - stock_id (String): Stock symbol, part of primary key
        - trade_date (Date): Trade date, part of primary key
        - ma5 (Double): 5-day moving average of close price
        - ema12, ema26 (Double): 12/26-day exponential moving averages
        - macd_dif, macd_dea, macd_osc (Double): MACD indicators
        - bb_ma20, bb_std20 (Double): Bollinger Band mean and std
        - bb_upper20, bb_lower20 (Double): Bollinger Band upper/lower
        - bb_width20 (Double): Band width
        - bb_pos20 (Double): Position of close within band
        - bb_break_up (Boolean): True if close > upper band (breakout)
        - bb_break_dn (Boolean): True if close < lower band (breakdown)
        - etl_run_id (UUID): ETL run identifier
        - source_version (String): Source data version
        - commit_sha (String): Git commit SHA for traceability
        - created_at, updated_at (TIMESTAMP): Record timestamps

    Usage:
        Used for bulk upsert of calculated features from ETL pipeline.
        Enables efficient query and update of daily technical indicators for stocks.
    """
    stock_id = Column(String, primary_key=True)
    trade_date = Column(Date, primary_key=True)
    ma5 = Column(Double)
    ema12 = Column(Double)
    ema26 = Column(Double)
    macd_dif = Column(Double)
    macd_dea = Column(Double)
    macd_osc = Column(Double)
    bb_ma20 = Column(Double)
    bb_std20 = Column(Double)
    bb_upper20 = Column(Double)
    bb_lower20 = Column(Double)
    bb_width20 = Column(Double)
    bb_pos20 = Column(Double)
    bb_break_up = Column(Boolean)
    bb_break_dn = Column(Boolean)
    etl_run_id = Column(PG_UUID, nullable=False)
    source_version = Column(String)
    commit_sha = Column(String)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

    @classmethod
    def bulk_upsert_to_db(cls, dict_list):
        """
        Bulk upsert a list of feature dicts into the feature_daily table.
        Uses PostgreSQL ON CONFLICT DO UPDATE for efficient batch insert/update.

        Args:
            dict_list (list): List of dicts, each dict contains feature values for one day/stock

        Returns:
            int: Number of rows upserted

        Raises:
            Exception: Any database error during upsert

        Logging:
            Logs info for empty input, success, and error cases
        """
        if not dict_list:
            logger.info("bulk_upsert_to_db: input dict_list is empty, nothing to upsert.")
            return 0
        session = SessionLocal()
        try:
            stmt = insert(cls).values(dict_list)
            update_dict = {
                col: getattr(stmt.excluded, col)
                for col in dict_list[0].keys()
                if col not in ["stock_id", "trade_date"]
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["stock_id", "trade_date"],
                set_=update_dict
            )
            result = session.execute(stmt)
            session.commit()
            logger.info(f"bulk_upsert_to_db: upserted {result.rowcount} rows into feature_daily table.")
            return result.rowcount
        except Exception as e:
            session.rollback()
            logger.error(f"bulk_upsert_to_db: upsert failed: {e}")
            raise e
        finally:
            session.close()
