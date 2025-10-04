from sqlalchemy import Column, String, Date, Float, Integer, BigInteger
from sqlalchemy.orm import declarative_base
from app.core.setups import settings
from app.db.postgres import engine, SessionLocal

Base = declarative_base()

class PostgresKline(Base):
    """
    SQLAlchemy ORM class for the kline table, which stores daily OHLCV and related data for each stock.

    Table columns:
        - id (Integer): Auto-increment primary key
        - stock_id (String): Stock symbol
        - date (Date): Trade date
        - trading_volume (BigInteger): Trading volume
        - trading_money (BigInteger): Trading money
        - open (Float): Opening price
        - max (Float): Highest price
        - min (Float): Lowest price
        - close (Float): Closing price
        - spread (Float): Price spread
        - trading_turnover (BigInteger): Trading turnover

    Usage:
        Used for querying daily kline (OHLCV) data for feature calculation and ETL.
        Table name is configurable via settings.POSTGRES_KLINE.
    """
    __tablename__ = getattr(settings, "POSTGRES_KLINE", "kline")
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(String(255), nullable=False)
    date = Column(Date, nullable=False)
    trading_volume = Column(BigInteger)
    trading_money = Column(BigInteger)
    open = Column(Float)
    max = Column(Float)
    min = Column(Float)
    close = Column(Float)
    spread = Column(Float)
    trading_turnover = Column(BigInteger)
    __table_args__ = (
        # UNIQUE constraint for (date, stock_id)
        {'sqlite_autoincrement': True},
    )

def get_kline(stock_id, start_date=None, end_date=None):
    """
    Query kline (OHLCV) data for a given stock_id and date range from the database.

    Args:
        stock_id (str): Stock symbol to query
        start_date (str or date, optional): Start date (inclusive)
        end_date (str or date, optional): End date (inclusive)

    Returns:
        list: List of PostgresKline ORM objects, ordered by date ascending

    Usage:
        Used in ETL pipeline to fetch raw kline data for feature calculation.
    """
    session = SessionLocal()
    try:
        query = session.query(PostgresKline).filter(PostgresKline.stock_id == stock_id)
        if start_date:
            query = query.filter(PostgresKline.date >= start_date)
        if end_date:
            query = query.filter(PostgresKline.date <= end_date)
        query = query.order_by(PostgresKline.date.asc())
        return query.all()
    finally:
        session.close()
