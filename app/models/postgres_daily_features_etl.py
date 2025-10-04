
import pandas as pd
from datetime import datetime, timedelta
import uuid
from app.models.postgres_kline import get_kline
from app.models.postgres_stock_info import get_all_stock_ids
from app.db.postgres import engine
from sqlalchemy.dialects.postgresql import insert
from app.db.postgres import SessionLocal
from app.core.logger import logger
from sqlalchemy.dialects.postgresql import UUID as PG_UUID


class DailyFeatureETL:
    """
    ETL class for calculating daily technical indicators for stocks and preparing data for bulk upsert.

    Features:
    - Fetches kline (OHLCV) data for stocks from the database
    - Calculates daily features: moving averages (MA, EMA), MACD, Bollinger Bands, breakout flags, etc.
    - Prepares results for bulk upsert to feature_daily table
    - Supports batch calculation for all stocks or single stock

    Methods:
        calculate_features(stock_id, df):
            Calculate technical indicators for a given stock's kline DataFrame.
            Features include MA5, EMA12, EMA26, MACD (DIF, DEA, OSC), Bollinger Bands (MA20, STD20, upper/lower/width/pos),
            and breakout flags (bb_break_up, bb_break_dn).
            Returns a DataFrame with all calculated features.

        etl_by_stock(stock_id, start_date, end_date):
            Fetch kline data for a single stock and date range, calculate features, and prepare dicts for upsert.
            Returns a list of dicts, each representing one day's features.

        etl_all_stocks(start_date, end_date):
            Batch process all stocks in the database for the given date range.
            Returns a list of dicts for all stocks and dates.
    """
    def __init__(self, etl_run_id=None, source_version=None, commit_sha=None):
        self.etl_run_id = etl_run_id or uuid.uuid4()
        self.source_version = source_version
        self.commit_sha = commit_sha
    pass

    def calculate_features(self, stock_id, df):
        """
        Calculate daily technical indicators for a given stock's kline DataFrame.

        Args:
            stock_id (str): Stock symbol
            df (pd.DataFrame): Kline data with columns: date, open, high, low, close, trading_volume, etc.

        Returns:
            pd.DataFrame: DataFrame with calculated features for each day

        Features calculated:
            - ma5: 5-day moving average of close price
            - ema12, ema26: 12/26-day exponential moving averages
            - macd_dif, macd_dea, macd_osc: MACD indicators
            - bb_ma20, bb_std20: 20-day Bollinger Band mean and std
            - bb_upper20, bb_lower20: Bollinger Band upper/lower
            - bb_width20: Band width
            - bb_pos20: Position of close within band
            - bb_break_up: True if close > upper band (breakout)
            - bb_break_dn: True if close < lower band (breakdown)
        """
        df = df.copy()
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd_dif'] = df['ema12'] - df['ema26']
        df['macd_dea'] = df['macd_dif'].ewm(span=9, adjust=False).mean()
        df['macd_osc'] = df['macd_dif'] - df['macd_dea']
        df['bb_ma20'] = df['close'].rolling(window=20).mean()
        df['bb_std20'] = df['close'].rolling(window=20).std()
        df['bb_upper20'] = df['bb_ma20'] + 2 * df['bb_std20']
        df['bb_lower20'] = df['bb_ma20'] - 2 * df['bb_std20']
        df['bb_width20'] = df['bb_upper20'] - df['bb_lower20']
        df['bb_pos20'] = (df['close'] - df['bb_lower20']) / (df['bb_width20'] + 1e-9)
        df['bb_break_up'] = df['close'] > df['bb_upper20']
        df['bb_break_dn'] = df['close'] < df['bb_lower20']
        return df

    def etl_by_stock(self, stock_id, start_date=None, end_date=None):
        """
        ETL for a single stock: fetch kline data, calculate features, prepare dicts for upsert.

        Args:
            stock_id (str): Stock symbol
            start_date (str): Start date (YYYY-MM-DD)
            end_date (str): End date (YYYY-MM-DD)

        Returns:
            list: List of dicts, each dict contains features for one day
        """
        max_window = 30
        if start_date == end_date and start_date is not None:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            start_date_adj = (start_dt - timedelta(days=max_window-1)).strftime("%Y-%m-%d")
            logger.info(f"etl_by_stock: Fetching kline for {stock_id} from {start_date_adj} to {end_date} for feature calculation.")
            kline_objs = get_kline(stock_id, start_date_adj, end_date)
            logger.info(f"etl_by_stock: Fetched {len(kline_objs)} kline records for {stock_id}.")
            df = pd.DataFrame([
                {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
                for obj in kline_objs
            ]) if kline_objs else pd.DataFrame()
        else:
            logger.info(f"etl_by_stock: Fetching kline for {stock_id} from {start_date} to {end_date} for feature calculation.")
            kline_objs = get_kline(stock_id, start_date, end_date)
            logger.info(f"etl_by_stock: Fetched {len(kline_objs)} kline records for {stock_id}.")
            df = pd.DataFrame([
                {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
                for obj in kline_objs
            ]) if kline_objs else pd.DataFrame()
        if df.empty:
            logger.warning(f"etl_by_stock: No kline data for {stock_id}, skipped.")
            return []
        df = self.calculate_features(stock_id, df)
        result = []
        for _, row in df.iterrows():
            result.append({
                'stock_id': stock_id,
                'trade_date': row['date'],
                'ma5': None if pd.isnull(row.get('ma5')) else row.get('ma5'),
                'ema12': None if pd.isnull(row.get('ema12')) else row.get('ema12'),
                'ema26': None if pd.isnull(row.get('ema26')) else row.get('ema26'),
                'macd_dif': None if pd.isnull(row.get('macd_dif')) else row.get('macd_dif'),
                'macd_dea': None if pd.isnull(row.get('macd_dea')) else row.get('macd_dea'),
                'macd_osc': None if pd.isnull(row.get('macd_osc')) else row.get('macd_osc'),
                'bb_ma20': None if pd.isnull(row.get('bb_ma20')) else row.get('bb_ma20'),
                'bb_std20': None if pd.isnull(row.get('bb_std20')) else row.get('bb_std20'),
                'bb_upper20': None if pd.isnull(row.get('bb_upper20')) else row.get('bb_upper20'),
                'bb_lower20': None if pd.isnull(row.get('bb_lower20')) else row.get('bb_lower20'),
                'bb_width20': None if pd.isnull(row.get('bb_width20')) else row.get('bb_width20'),
                'bb_pos20': None if pd.isnull(row.get('bb_pos20')) else row.get('bb_pos20'),
                'bb_break_up': bool(row.get('bb_break_up')) if not pd.isnull(row.get('bb_break_up')) else None,
                'bb_break_dn': bool(row.get('bb_break_dn')) if not pd.isnull(row.get('bb_break_dn')) else None,
                'etl_run_id': str(self.etl_run_id),
                'source_version': self.source_version,
                'commit_sha': self.commit_sha,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
            })
        logger.info(f"etl_by_stock: Feature calculation for {stock_id} {end_date} finished, total {len(result)} rows.")
        return result

    def etl_all_stocks(self, start_date=None, end_date=None):
        """
        Batch ETL for all stocks: fetch kline, calculate features, prepare dicts for upsert.

        Args:
            start_date (str): Start date (YYYY-MM-DD)
            end_date (str): End date (YYYY-MM-DD)

        Returns:
            list: List of dicts for all stocks and dates
        """
        stock_ids = get_all_stock_ids()
        all_results = []
        logger.info(f"etl_all_stocks: Batch feature calculation for all stocks, date range {start_date} ~ {end_date}.")
        for stock_id in stock_ids:
            result = self.etl_by_stock(stock_id, start_date, end_date)
            all_results.extend(result)
        logger.info(f"etl_all_stocks: Batch feature calculation finished, total {len(all_results)} rows.")
        return all_results
