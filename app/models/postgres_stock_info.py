
# postgres_stock_info.py
# SQLAlchemy ORM model and utility function for Taiwan stock info table.
# This module defines the table schema and provides a function to fetch all stock IDs.

from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base
from app.core.setups import settings
from app.db.postgres import engine, SessionLocal

# Base class for SQLAlchemy ORM models
Base = declarative_base()

class PostgresStockInfo(Base):
    """
    SQLAlchemy ORM model for the stock info table.
    Table name is dynamically set from settings.POSTGRES_STOCK_INFO.
    This table stores basic stock information, such as stock_id.
    """
    __tablename__ = settings.POSTGRES_STOCK_INFO
    stock_id = Column(String, primary_key=True)  # Stock symbol (e.g., '2330')
    # Extend with additional columns as needed (e.g., stock_name, industry_category)

def get_all_stock_ids():
    """
    Fetch all stock_id values from the stock info table.
    This function queries the table and returns a list of all stock symbols.
    Returns:
        list: List of stock_id strings present in the table.
    """
    session = SessionLocal()
    try:
        results = session.query(PostgresStockInfo.stock_id).all()
        # Extract stock_id from each row and return as a list
        return [row.stock_id for row in results]
    finally:
        session.close()
