from pymongo import MongoClient
from datetime import datetime
import sys

# 配置 MongoDB 连接
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "stock_db"
COLLECTION_NAME = "stock_data"

try:
    # 设置连接超时时间为 5 秒
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    
    # 尝试 ping 数据库，确认可用
    client.admin.command('ping')
    print("✅ MongoDB 连接成功！")
    
except Exception as e:
    print("❌ MongoDB 连接失败:", e)
    sys.exit(1)  # 退出程序

# 获取数据库和集合
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# 模拟一条股票数据
stock_item = {
    "symbol": "AAPL",
    "price": 173.5,
    "volume": 12000,
    "timestamp": datetime.now()
}

try:
    result = collection.insert_one(stock_item)
    print(f"已插入数据，ID: {result.inserted_id}")
except Exception as e:
    print("❌ 数据插入失败:", e)
