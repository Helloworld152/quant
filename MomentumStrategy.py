import backtrader as bt

# 2. 动量策略
class MomentumStrategy(bt.Strategy):
    params = (
        ("momentum_period", 5),       # 动量计算天数
        ("take_profit", 0.05),
        ("stop_loss", 0.02),
        ("risk_per_trade", 0.1),      # 每笔交易占总资金比例
        ("base_position", 0.2),       # 底仓比例
    )

    def __init__(self):
        self.order = None
        self.momentum = bt.indicators.Momentum(self.data.close, period=self.params.momentum_period)
        self.last_buy_price = None

    def next(self):
        cash = self.broker.get_cash()
        current_price = self.data.close[0]
        position_value = self.position.size * current_price
        total_value = self.broker.get_value()

        # 计算可以买的股数
        max_buy_size = int(cash * self.params.risk_per_trade / current_price)

        # --- 买入逻辑 ---
        if self.momentum[0] > 0:  # 动量为正
            # 买入至底仓以上
            target_position_value = total_value * self.params.base_position
            if position_value < target_position_value:
                size = min(max_buy_size, int((target_position_value - position_value)/current_price))
                if size > 0:
                    self.order = self.buy(size=size)
                    self.last_buy_price = current_price

        # --- 卖出逻辑 ---
        elif self.position.size > 0:
            # 止盈/止损
            avg_price = self.last_buy_price
            if avg_price is not None:
                if current_price >= avg_price * (1 + self.params.take_profit):
                    self.order = self.sell(size=self.position.size)
                elif current_price <= avg_price * (1 - self.params.stop_loss):
                    self.order = self.sell(size=self.position.size)

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f"{self.data.datetime.date(0)} 买入: {order.executed.price:.2f}, 股数: {order.executed.size}")
            elif order.issell():
                print(f"{self.data.datetime.date(0)} 卖出: {order.executed.price:.2f}, 股数: {order.executed.size}")
        self.order = None