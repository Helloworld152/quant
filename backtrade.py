from strategies import DualMovingAverageStrategy
from backtest import run_backtest
import pandas as pd
import os


if __name__ == '__main__':
    # 标的清单文件（用户可新增），字段: symbol, tested(0/1), last_run(可选)
    symbols_file = 'symbols.csv'
    # 回测模式：'pending' 仅回测未标记，'all' 回测全部
    mode = 'pending'
    start_date = '20200101'
    end_date = '20250901'

    fast_list = [5, 10, 20]
    slow_list = [30, 50, 100]

    # 读取/初始化 symbol 文件
    if os.path.exists(symbols_file):
        sym_df = pd.read_csv(symbols_file, dtype={'symbol': str})
        if 'tested' not in sym_df.columns:
            sym_df['tested'] = 0
        sym_df['tested'] = sym_df['tested'].fillna(0).astype(int)
    else:
        # 若文件不存在，可在此初始化示例；用户后续可直接编辑此文件新增股票
        init_symbols = ['002230', '600111']
        sym_df = pd.DataFrame({'symbol': init_symbols, 'tested': 0})
        sym_df.to_csv(symbols_file, index=False, encoding='utf-8-sig')

    if mode == 'all':
        batch_symbols = sym_df['symbol'].astype(str).tolist()
    else:
        batch_symbols = [s for s, t in zip(sym_df['symbol'].astype(str), sym_df['tested']) if int(t) != 1]
    if not batch_symbols:
        print('无待回测标的（symbols.csv 全部已标记）。可在文件追加新股票。')
        raise SystemExit(0)

    records = []
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
                    initial_cash=100000.0,
                    commission=0.001,
                    cheat_on_close=True,
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
    # 先计算累计收益，再排序：symbol 升序，累计收益 降序
    init_cash = 100000.0
    df['__cum_return'] = df['final_value'].apply(lambda v: (v / init_cash - 1) * 100 if isinstance(v, (int, float)) else None)
    df = df.sort_values(['symbol', '__cum_return'], ascending=[True, False])

    # 中文列名与百分比展示（累计收益用期末/初始计算）
    df['累计收益(%)'] = df['__cum_return']
    df['年化收益(%)'] = df['rnorm100']
    df['胜率(%)'] = df['win_rate']

    rename_map = {
        'symbol': '标的',
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

    cols_order = ['标的', '开始日期', '结束日期', '短期均线', '长期均线', '期末资金', '夏普比率', '最大回撤(%)', '最大回撤金额', '累计收益(%)', '年化收益(%)', '交易笔数', '胜率(%)']
    df_cn = df_cn.reindex(columns=[c for c in cols_order if c in df_cn.columns])

    print(df_cn.round(2).head(10))
    out_path = 'ma_grid_results.csv'
    if mode == 'all':
        df_cn.to_csv(out_path, mode='w', header=True, index=False, encoding='utf-8-sig', float_format='%.2f')
        print(f"已覆盖保存: {out_path}")
    else:
        header_needed = not os.path.exists(out_path)
        df_cn.to_csv(out_path, mode='a', header=header_needed, index=False, encoding='utf-8-sig', float_format='%.2f')
        print(f"已追加保存: {out_path}")
