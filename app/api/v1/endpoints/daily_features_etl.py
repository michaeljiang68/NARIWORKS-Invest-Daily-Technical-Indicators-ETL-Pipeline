
# daily_features_etl.py
# FastAPI endpoints for batch and single stock technical indicator ETL and upsert.
#
# Endpoints:
#   /etl_all_stocks: Calculate technical indicators for all stocks in a date range and batch upsert results.
#   /etl_by_stock: Calculate technical indicators for a single stock in a date range and upsert results.
#
# Features:
# - Uses DailyFeatureETL for feature calculation (moving average, MACD, Bollinger Bands, etc.)
# - Uses FeatureDaily.bulk_upsert_to_db for efficient batch upsert to Postgres
# - Supports batch size control, logging, and error handling
# - Returns upserted row count and calculated feature rows
#
# Parameters:
#   start_date, end_date: Date range for calculation (YYYY-MM-DD)
#   stock_id: Stock symbol for single stock endpoint
#
# Error Handling:
#   All exceptions are logged and returned as HTTP 500 errors with details

from fastapi import APIRouter, HTTPException, Query
from datetime import date
from app.core.logger import logger
from app.models.postgres_daily_features_etl import DailyFeatureETL
from app.models.postgres_daily_features_orm import FeatureDaily

router = APIRouter()


@router.get("/etl_all_stocks")
def etl_all_stocks(
    start_date: date = Query(..., description="Start date, format YYYY-MM-DD"),
    end_date: date = Query(..., description="End date, format YYYY-MM-DD")
):
    """
    Batch calculate technical indicators for all stocks in the database within the specified date range.
    Results are upserted to the feature_daily table in batches.

    Args:
        start_date (date): Start date for calculation (YYYY-MM-DD)
        end_date (date): End date for calculation (YYYY-MM-DD)

    Returns:
        dict: {
            'upserted': total number of rows upserted,
            'rows': list of calculated feature dicts for all stocks
        }

    Raises:
        HTTPException: 500 error with details if any exception occurs
    """
    try:
        logger.info(f"API etl_all_stocks called: {start_date} ~ {end_date}")
        etl = DailyFeatureETL()
        result = etl.etl_all_stocks(str(start_date), str(end_date))
        total_upserted = 0
        batch_size = 1000
        for i in range(0, len(result), batch_size):
            batch = result[i:i+batch_size]
            upserted = FeatureDaily.bulk_upsert_to_db(batch)
            total_upserted += upserted
            logger.info(f"API etl_all_stocks: upserted batch {i//batch_size+1}, {upserted} rows.")
        logger.info(f"API etl_all_stocks finished: {total_upserted} rows upserted.")
        return {"upserted": total_upserted, "rows": result}
    except Exception as e:
        logger.error(f"API etl_all_stocks error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/etl_by_stock")
def etl_by_stock(
    stock_id: str = Query(..., description="Stock ID, e.g., 'AAPL'"),
    start_date: date = Query(None, description="Start date, format YYYY-MM-DD"),
    end_date: date = Query(None, description="End date, format YYYY-MM-DD")
):
    """
    Calculate technical indicators for a single stock within the specified date range.
    Results are upserted to the feature_daily table.

    Args:
        stock_id (str): Stock symbol to calculate (e.g., 'AAPL')
        start_date (date, optional): Start date for calculation (YYYY-MM-DD)
        end_date (date, optional): End date for calculation (YYYY-MM-DD)

    Returns:
        dict: {
            'upserted': number of rows upserted,
            'rows': list of calculated feature dicts for the stock
        }

    Raises:
        HTTPException: 500 error with details if any exception occurs
    """
    try:
        logger.info(f"API etl_by_stock called: {stock_id} {start_date} ~ {end_date}")
        etl = DailyFeatureETL()
        result = etl.etl_by_stock(stock_id, str(start_date) if start_date else None, str(end_date) if end_date else None)
        upserted = FeatureDaily.bulk_upsert_to_db(result)
        logger.info(f"API etl_by_stock finished: {upserted} rows upserted.")
        return {"upserted": upserted, "rows": result}
    except Exception as e:
        logger.error(f"API etl_by_stock error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

