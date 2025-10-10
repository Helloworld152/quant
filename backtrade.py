import os
import sys
import pandas as pd
import backtrader as bt

# 允许从 mongo_py 导入数据函数
CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(os.path.join(CURRENT_DIR, 'mongo_py'))
from stock_data import get_stock_daily_data
from strategies import DualMovingAverageStrategy


 


def load_bt_data(symbol: str, start_date: str, end_date: str) -> bt.feeds.PandasData:
    df = get_stock_daily_data(symbol, start_date, end_date)
    print(f"loaded rows: {len(df)}; head:\n{df.head(3)}")
    cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    df = df[cols].copy()
    df = df.sort_values('datetime')
    df = df.set_index('datetime')
    df = df.rename(columns={
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume',
    })
    # 强制数值化，避免字符串导致指标不触发
    for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
    data_feed = bt.feeds.PandasData(dataname=df)
    return data_feed


if __name__ == '__main__':
    symbol = '601789'
    start_date = '20200101'
    end_date = '20250901'

    cerebro = bt.Cerebro()
    cerebro.addstrategy(DualMovingAverageStrategy, fast=10, slow=30, printlog=True)

    data_feed = load_bt_data(symbol, start_date, end_date)
    cerebro.adddata(data_feed, name=symbol)

    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    # 允许按收盘价撮合，提升下单成交率
    cerebro.broker.set_coc(True)

    print('初始资金:', round(cerebro.broker.getvalue(), 2))
    cerebro.run()
    print('结束资金:', round(cerebro.broker.getvalue(), 2))
