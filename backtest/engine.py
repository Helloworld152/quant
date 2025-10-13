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

        if verbose:
            _print_analyzer_summary(analyzers)

    return {
        'cerebro': cerebro,
        'results': results,
        'final_value': final_value,
        'analyzers': analyzers,
    }


def _safe_get(dct, path, default=None):
    cur = dct
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _print_analyzer_summary(analyzers: dict) -> None:
    sharpe = analyzers.get('sharpe', {})
    drawdown = analyzers.get('drawdown', {})
    returns = analyzers.get('returns', {})
    trades = analyzers.get('trades', {})

    sharpe_ratio = sharpe.get('sharperatio', None)

    max_dd_pct = _safe_get(drawdown, ['max', 'drawdown'], None)
    max_dd_money = _safe_get(drawdown, ['max', 'moneydown'], None)

    rtot = returns.get('rtot', None)
    rtot_pct = (rtot * 100.0) if isinstance(rtot, (int, float)) else None
    rnorm100 = returns.get('rnorm100', None)

    total_trades = _safe_get(trades, ['total', 'total'], 0) or 0
    won_trades = _safe_get(trades, ['won', 'total'], 0) or 0
    lost_trades = _safe_get(trades, ['lost', 'total'], 0) or 0
    win_rate = (won_trades / total_trades * 100.0) if total_trades else None

    pnl_won_total = _safe_get(trades, ['pnl', 'won', 'total'], None)
    pnl_lost_total = _safe_get(trades, ['pnl', 'lost', 'total'], None)
    avg_win = (pnl_won_total / won_trades) if (won_trades and isinstance(pnl_won_total, (int, float))) else None
    avg_loss = (pnl_lost_total / lost_trades) if (lost_trades and isinstance(pnl_lost_total, (int, float))) else None
    payoff = (avg_win / abs(avg_loss)) if (avg_win not in (None, 0) and avg_loss not in (None, 0)) else None

    print('—— 回测指标 ——')
    if sharpe_ratio is not None:
        print(f"夏普比率: {sharpe_ratio:.6f}")
    else:
        print('夏普比率: N/A')

    if max_dd_pct is not None:
        if isinstance(max_dd_pct, dict):
            # 某些版本会把 drawdown 明细嵌套在 dict
            _val = max_dd_pct.get('maxdrawdown', None)
            print(f"最大回撤: {(_val or 0):.2f}%")
        else:
            print(f"最大回撤: {float(max_dd_pct):.2f}%")
    else:
        print('最大回撤: N/A')

    if max_dd_money is not None:
        print(f"最大回撤金额: {float(max_dd_money):.2f}")

    if rtot_pct is not None:
        print(f"累计收益: {rtot_pct:.2f}%")
    else:
        print('累计收益: N/A')

    if isinstance(rnorm100, (int, float)):
        print(f"年化收益(估): {rnorm100:.2f}%")

    if total_trades:
        print(f"交易笔数: {total_trades}, 胜率: {(win_rate or 0):.2f}%")
    else:
        print('交易笔数: 0')

    if payoff is not None:
        print(f"盈亏比: {payoff:.2f}")


