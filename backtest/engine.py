import os
import sys
import pandas as pd
import backtrader as bt

# 允许从 mongo_py 导入数据函数（与项目根保持一致）
CURRENT_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(CURRENT_DIR, 'mongo_py'))
from stock_data import get_stock_daily_data


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


def run_backtest(
    symbols,
    start_date: str,
    end_date: str,
    strategy_cls,
    strategy_params=None,
    initial_cash: float = 100000.0,
    commission: float = 0.001,
    cheat_on_close: bool = True,
    add_analyzers: bool = True,
    verbose: bool = True,
):
    """
    通用回测封装：
    - 支持单/多标的
    - 可配置策略与参数、初始资金、手续费、是否按收盘价撮合
    - 可选添加常用分析器并返回结果
    返回：dict，包含 cerebro、results、final_value、analyzers（如启用）
    """
    strategy_params = strategy_params or {}

    cerebro = bt.Cerebro()
    cerebro.addstrategy(strategy_cls, **strategy_params)

    symbols_list = symbols if isinstance(symbols, (list, tuple)) else [symbols]
    for s in symbols_list:
        data_feed = load_bt_data(s, start_date, end_date)
        cerebro.adddata(data_feed, name=str(s))

    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=commission)
    cerebro.broker.set_coc(cheat_on_close)

    if add_analyzers:
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    if verbose:
        print('初始资金:', round(cerebro.broker.getvalue(), 2))

    results = cerebro.run()
    final_value = round(cerebro.broker.getvalue(), 2)

    if verbose:
        print('结束资金:', final_value)

    analyzers = {}
    if add_analyzers and results:
        strat = results[0]
        analyzers = {
            'sharpe': strat.analyzers.sharpe.get_analysis() if hasattr(strat.analyzers, 'sharpe') else {},
            'drawdown': strat.analyzers.drawdown.get_analysis() if hasattr(strat.analyzers, 'drawdown') else {},
            'returns': strat.analyzers.returns.get_analysis() if hasattr(strat.analyzers, 'returns') else {},
            'trades': strat.analyzers.trades.get_analysis() if hasattr(strat.analyzers, 'trades') else {},
        }

    return {
        'cerebro': cerebro,
        'results': results,
        'final_value': final_value,
        'analyzers': analyzers,
    }


