from strategies import DualMovingAverageStrategy
from backtest import run_backtest


if __name__ == '__main__':
    symbol = '601789'
    start_date = '20200101'
    end_date = '20250901'
    _ = run_backtest(
        symbols=symbol,
        start_date=start_date,
        end_date=end_date,
        strategy_cls=DualMovingAverageStrategy,
        strategy_params=dict(fast=10, slow=30, printlog=True),
        initial_cash=100000.0,
        commission=0.001,
        cheat_on_close=True,
        add_analyzers=True,
        verbose=True,
    )
