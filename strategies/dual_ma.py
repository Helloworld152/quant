import backtrader as bt


class DualMovingAverageStrategy(bt.Strategy):
    params = dict(
        fast=5,
        slow=20,
        printlog=False,
    )

    def __init__(self):
        self.sma_fast = bt.indicators.SMA(self.data.close, period=self.params.fast)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=self.params.slow)
        self.crossover = bt.indicators.CrossOver(self.sma_fast, self.sma_slow)
        self._dbg_count = 0
        self.order = None

    def log(self, txt):
        if self.params.printlog:
            dt = self.data.datetime.date(0)
            print(f"{dt} - {txt}")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            action = 'BUY' if order.isbuy() else 'SELL'
            self.log(f"ORDER {action} EXECUTED, price={order.executed.price:.4f}, size={order.executed.size}")
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"ORDER {order.getstatusname()}")
        # 当前订单处理完毕/失败，允许下一次提交
        self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f"TRADE PnL gross={trade.pnl:.2f} net={trade.pnlcomm:.2f}")

    def next(self):
        ready = len(self) >= max(self.params.fast, self.params.slow)
        if self.params.printlog and self._dbg_count < 15:
            self.log(f"ready={ready}, close={float(self.data.close[0]):.4f}, sma_fast={float(self.sma_fast[0]) if ready else float('nan'):.4f}, sma_slow={float(self.sma_slow[0]) if ready else float('nan'):.4f}")
            self._dbg_count += 1

        if self.order:
            return

        if not self.position:
            if ready and self.sma_fast[0] > self.sma_slow[0]:
                # 使用 99% 现金计算可买股数，预留手续费避免保证金不足
                cash = self.broker.get_cash()
                price = float(self.data.close[0])
                size = int((cash * 0.99) / price)
                if size > 0:
                    self.order = self.buy(size=size)
        else:
            if self.crossover < 0:
                # 清仓
                self.order = self.close()


