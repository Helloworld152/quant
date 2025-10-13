import os
import pandas as pd
from typing import List, Sequence, Tuple, Dict, Any

from strategies import DualMovingAverageStrategy
from backtest import run_backtest


def run_ma_grid_batch(
    symbols_file: str,
    mode: str,
    start_date: str,
    end_date: str,
    fast_list: Sequence[int],
    slow_list: Sequence[int],
    initial_cash: float = 100000.0,
    commission: float = 0.001,
    cheat_on_close: bool = True,
    out_csv: str = 'ma_grid_results.csv',
) -> pd.DataFrame:
    """
    批量执行双均线参数网格回测：从 symbols.csv 读取标的，按 mode 选择批次，结果写入 CSV。
    mode='pending' 仅回测未标记；mode='all' 全量回测。
    返回中文列 DataFrame（已排序、两位小数未四舍五入，建议外部打印时 round(2)）。
    """
    # 读取/初始化 symbol 文件
    if os.path.exists(symbols_file):
        sym_df = pd.read_csv(symbols_file, dtype={'symbol': str})
        if 'tested' not in sym_df.columns:
            sym_df['tested'] = 0
        sym_df['tested'] = sym_df['tested'].fillna(0).astype(int)
    else:
        sym_df = pd.DataFrame({'symbol': [], 'tested': []})
        sym_df.to_csv(symbols_file, index=False, encoding='utf-8-sig')

    if mode == 'all':
        batch_symbols = sym_df['symbol'].astype(str).tolist()
    else:
        batch_symbols = [s for s, t in zip(sym_df['symbol'].astype(str), sym_df['tested']) if int(t) != 1]

    if not batch_symbols:
        print('无待回测标的（symbols.csv 可追加新股票）。')
        return pd.DataFrame()

    records: List[Dict[str, Any]] = []
    strategy_name = DualMovingAverageStrategy.__name__
    for sym in batch_symbols:
        for fast in fast_list:
            for slow in slow_list:
                if fast >= slow:
                    continue
                result = run_backtest(
                    symbols=sym,
                    start_date=start_date,
                    end_date=end_date,
                    strategy_cls=DualMovingAverageStrategy,
                    strategy_params=dict(fast=fast, slow=slow, printlog=False),
                    initial_cash=initial_cash,
                    commission=commission,
                    cheat_on_close=cheat_on_close,
                    add_analyzers=True,
                    verbose=False,
                )
                analyzers = result.get('analyzers', {})
                sharpe = analyzers.get('sharpe', {}).get('sharperatio')
                dd = analyzers.get('drawdown', {})
                max_dd_pct = dd.get('max', {}).get('drawdown')
                max_dd_money = dd.get('max', {}).get('moneydown')
                rets = analyzers.get('returns', {})
                rtot = rets.get('rtot')
                rnorm100 = rets.get('rnorm100')
                trades = analyzers.get('trades', {})
                total_trades = trades.get('total', {}).get('total', 0)
                won_trades = trades.get('won', {}).get('total', 0)
                win_rate = (won_trades / total_trades * 100.0) if total_trades else 0.0

                records.append(dict(
                    symbol=sym,
                    strategy=strategy_name,
                    fast=fast,
                    slow=slow,
                    start_date=start_date,
                    end_date=end_date,
                    final_value=result.get('final_value'),
                    sharpe=sharpe,
                    max_dd_pct=max_dd_pct,
                    max_dd_money=max_dd_money,
                    rtot=rtot,
                    rnorm100=rnorm100,
                    total_trades=total_trades,
                    win_rate=win_rate,
                ))

        # 标记该股票已完成回测
        sym_df.loc[sym_df['symbol'] == sym, 'tested'] = 1
        sym_df.loc[sym_df['symbol'] == sym, 'last_run'] = pd.Timestamp.now()
        sym_df.to_csv(symbols_file, index=False, encoding='utf-8-sig')

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df

    init_cash = initial_cash
    df['__cum_return'] = df['final_value'].apply(lambda v: (v / init_cash - 1) * 100 if isinstance(v, (int, float)) else None)
    df = df.sort_values(['symbol', '__cum_return'], ascending=[True, False])

    # 中文列名
    df['累计收益(%)'] = df['__cum_return']
    df['年化收益(%)'] = df['rnorm100']
    df['胜率(%)'] = df['win_rate']

    rename_map = {
        'symbol': '标的',
        'strategy': '策略',
        'start_date': '开始日期',
        'end_date': '结束日期',
        'fast': '短期均线',
        'slow': '长期均线',
        'final_value': '期末资金',
        'sharpe': '夏普比率',
        'max_dd_pct': '最大回撤(%)',
        'max_dd_money': '最大回撤金额',
        'total_trades': '交易笔数',
    }
    df_cn = df.rename(columns=rename_map)
    if '__cum_return' in df_cn.columns:
        df_cn = df_cn.drop(columns=['__cum_return'])

    cols_order = ['标的', '策略', '开始日期', '结束日期', '短期均线', '长期均线', '期末资金', '夏普比率', '最大回撤(%)', '最大回撤金额', '累计收益(%)', '年化收益(%)', '交易笔数', '胜率(%)']
    df_cn = df_cn.reindex(columns=[c for c in cols_order if c in df_cn.columns])

    # 输出 CSV
    if mode == 'all':
        df_cn.to_csv(out_csv, mode='w', header=True, index=False, encoding='utf-8-sig', float_format='%.2f')
    else:
        header_needed = not os.path.exists(out_csv)
        df_cn.to_csv(out_csv, mode='a', header=header_needed, index=False, encoding='utf-8-sig', float_format='%.2f')

    return df_cn


