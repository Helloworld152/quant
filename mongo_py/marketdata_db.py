# market_data_db.py
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import pandas as pd
from datetime import datetime, date

class MarketDataDB:
    def __init__(self, host='localhost', port=27017, db_name='market_data', username=None, password=None):
        """
        初始化 MongoDB 连接
        """
        if username and password:
            self.client = MongoClient(host=host, port=port, username=username, password=password)
        else:
            self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]

    def ensure_index(self, collection_name, keys=[('symbol', ASCENDING), ('datetime', ASCENDING)], unique=False):
        """
        创建索引，避免重复数据，提高查询速度
        """
        collection = self.db[collection_name]
        collection.create_index(keys, unique=unique)

    def insert_data(self, collection_name, data):
        """
        写入行情数据
        data 可以是 dict 或 list[dict] 或 pandas DataFrame
        必须包含 'symbol' 和 'datetime' 字段
        """
        collection = self.db[collection_name]
        
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        elif isinstance(data, dict):
            data = [data]

        # 统一将各种时间类型转换为 Python datetime.datetime
        for d in data:
            if 'datetime' in d:
                ts = d['datetime']
                if isinstance(ts, datetime):
                    pass
                elif isinstance(ts, date):
                    d['datetime'] = datetime.combine(ts, datetime.min.time())
                else:
                    # pandas.Timestamp / numpy.datetime64 / str 等统一处理
                    d['datetime'] = pd.to_datetime(ts).to_pydatetime()

        try:
            collection.insert_many(data, ordered=False)
        except DuplicateKeyError:
            pass  # 已存在的忽略
        except Exception as e:
            print(f"插入数据失败: {e}")

    def query_data(self, collection_name, symbol=None, start=None, end=None, limit=0, sort=[('datetime', ASCENDING)]):
        """
        查询行情数据
        symbol: 交易品种，如 'BTCUSDT'
        start, end: datetime 或字符串
        limit: 返回条数，0 表示全部
        """
        collection = self.db[collection_name]
        query = {}

        if symbol:
            query['symbol'] = symbol
        if start or end:
            query['datetime'] = {}
            if start:
                query['datetime']['$gte'] = pd.to_datetime(start).to_pydatetime()
            if end:
                query['datetime']['$lte'] = pd.to_datetime(end).to_pydatetime()

        cursor = collection.find(query).sort(sort)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

# --------------------------
# 测试示例
# --------------------------
if __name__ == "__main__":
    db = MarketDataDB()
    db.ensure_index('kline', unique=True)

    # 插入示例
    sample_data = [
        {'symbol': 'BTCUSDT', 'datetime': '2025-10-10 10:00:00', 'open': 50000, 'high': 50500, 'low': 49500, 'close': 50200, 'volume': 120},
        {'symbol': 'BTCUSDT', 'datetime': '2025-10-10 10:01:00', 'open': 50200, 'high': 50600, 'low': 50000, 'close': 50400, 'volume': 150},
    ]
    db.insert_data('kline', sample_data)

    # 查询示例
    result = db.query_data('kline', symbol='BTCUSDT', start='2025-10-10 10:00:00', end='2025-10-10 10:05:00')
    for r in result:
        print(r)
