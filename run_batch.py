from backtest import run_ma_grid_batch


if __name__ == '__main__':
    symbols_file = 'symbols.csv'
    mode = 'all'  # 'pending' 仅未回测，'all' 全量重测
    start_date = '20200101'
    end_date = '20250901'
    fast_list = [5, 10, 20]
    slow_list = [30, 50, 100]

    df = run_ma_grid_batch(
        symbols_file=symbols_file,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
        fast_list=fast_list,
        slow_list=slow_list,
        initial_cash=100000.0,
        commission=0.001,
        cheat_on_close=True,
        out_csv='batch_results.csv',
    )

    if df is not None and not df.empty:
        print(df.round(2).head(20))
    else:
        print('无输出（可能 symbols.csv 无待回测标的）')


