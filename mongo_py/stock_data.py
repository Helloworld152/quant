import akshare as ak
import pandas as pd
from marketdata_db import MarketDataDB


def _normalize_kline_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    统一列名，添加 symbol，确保 datetime 为 pandas.Timestamp。
    """
    if '日期' in df.columns:
        df = df.rename(columns={
            '日期': 'datetime',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'turnover',
            '振幅': 'amplitude',
            '涨跌幅': 'pct_change',
            '涨跌额': 'change',
            '换手率': 'turnover_rate',
            '股票代码': 'stock_code'
        })
    df['symbol'] = symbol
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def _fetch_and_insert_range(db: MarketDataDB, collection: str, symbol: str, start_dt: pd.Timestamp, end_dt: pd.Timestamp, period: str) -> None:
    start_str = start_dt.strftime('%Y%m%d')
    end_str = end_dt.strftime('%Y%m%d')
    api_df = ak.stock_zh_a_hist(symbol=symbol, period=period, start_date=start_str, end_date=end_str, adjust="qfq")
    if isinstance(api_df, pd.DataFrame) and not api_df.empty:
        api_df = _normalize_kline_df(api_df, symbol)
        db.insert_data(collection, api_df)


def _get_kline(symbol: str, start_date: str, end_date: str, period: str, collection: str) -> pd.DataFrame:
    db = MarketDataDB()
    db.ensure_index(collection, unique=True)

    # 先查库
    result = db.query_data(collection, symbol=symbol, start=start_date, end=end_date)
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    if not result:
        # 库内无数据，拉全量
        _fetch_and_insert_range(db, collection, symbol, start_dt, end_dt, period)
    else:
        df_exist = pd.DataFrame(result)
        if 'datetime' in df_exist.columns:
            df_exist['datetime'] = pd.to_datetime(df_exist['datetime'])
            min_dt = df_exist['datetime'].min()
            max_dt = df_exist['datetime'].max()

            # 左端缺口
            if start_dt < min_dt:
                _fetch_and_insert_range(db, collection, symbol, start_dt, min_dt - pd.Timedelta(days=1), period)
            # 右端缺口
            if end_dt > max_dt:
                _fetch_and_insert_range(db, collection, symbol, max_dt + pd.Timedelta(days=1), end_dt, period)

    # 统一从库返回
    final = db.query_data(collection, symbol=symbol, start=start_date, end=end_date)
    df_final = pd.DataFrame(final)
    if '_id' in df_final.columns:
        df_final = df_final.drop(columns=['_id'])
    if 'datetime' in df_final.columns:
        df_final['datetime'] = pd.to_datetime(df_final['datetime'])
    return df_final

def get_stock_daily_data(symbol, start_date, end_date):
    return _get_kline(symbol, start_date, end_date, period="daily", collection='daily_kline')


def get_stock_weekly_data(symbol, start_date, end_date):
    return _get_kline(symbol, start_date, end_date, period="weekly", collection='weekly_kline')


def get_stock_monthly_data(symbol, start_date, end_date):
    return _get_kline(symbol, start_date, end_date, period="monthly", collection='monthly_kline')

if __name__ == "__main__":
    symbol = '000001'
    start_date = '20250101'
    end_date = '20250901'

    daily_df = get_stock_daily_data(symbol, start_date, end_date)
    print("daily_kline:", daily_df.shape)
    print(daily_df.head(3))

    weekly_df = get_stock_weekly_data(symbol, start_date, end_date)
    print("weekly_kline:", weekly_df.shape)
    print(weekly_df.head(3))

    monthly_df = get_stock_monthly_data(symbol, start_date, end_date)
    print("monthly_kline:", monthly_df.shape)
    print(monthly_df.head(3))


def delete_stock_data(symbol: str, start_date: str = None, end_date: str = None, period: str = "daily") -> int:
    """
    删除某标的在指定周期与时间范围内的数据；返回删除条数。
    period: daily/weekly/monthly
    """
    collection_map = {
        'daily': 'daily_kline',
        'weekly': 'weekly_kline',
        'monthly': 'monthly_kline',
    }
    collection = collection_map.get(period)
    if not collection:
        raise ValueError(f"unsupported period: {period}")
    db = MarketDataDB()
    return db.delete_data(collection, symbol=symbol, start=start_date, end=end_date)


def refresh_stock_data(symbols, start_date: str, end_date: str, period: str = "daily") -> None:
    """
    对一个 symbol 或 symbol 列表：先删除指定区间数据，再从数据源重拉并入库。
    """
    if isinstance(symbols, (str,)):
        symbols = [symbols]
    db = MarketDataDB()
    collection_map = {
        'daily': 'daily_kline',
        'weekly': 'weekly_kline',
        'monthly': 'monthly_kline',
    }
    collection = collection_map.get(period)
    if not collection:
        raise ValueError(f"unsupported period: {period}")
    for sym in symbols:
        # 先删除
        db.delete_data(collection, symbol=sym, start=start_date, end=end_date)
        # 再重拉
        _fetch_and_insert_range(db, collection, sym, pd.to_datetime(start_date), pd.to_datetime(end_date), period)