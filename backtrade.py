import pandas as pd
import backtrader as bt

# -----------------------
# 读取 CSV 数据
# -----------------------
df_all = pd.read_csv("all_stocks.csv")
df_all["Date"] = pd.to_datetime(df_all["Date"])

# 按股票分组生成 Backtrader 数据
datas = []
for sym, df_sym in df_all.groupby("Symbol"):
    df_sym = df_sym[["Date","Open","High","Low","Close","Volume"]].copy()
    df_sym.sort_values("Date", inplace=True)
    df_sym.set_index("Date", inplace=True)
    data_feed = bt.feeds.PandasData(dataname=df_sym, name=sym)
    datas.append(data_feed)

# -----------------------
# 策略定义
# -----------------------
class MomentumStrategy(bt.Strategy):
    params = dict(
        lookback=5,  # 动量计算天数
    )

    def __init__(self):
        # 保存每只股票的移动收益率
        self.mom_dict = {}
        for d in self.datas:
            mom = bt.indicators.RateOfChange(d.close, period=self.params.lookback)
            self.mom_dict[d._name] = mom

        self.current_stock = None  # 当前持仓股票

    def next(self):
        # 计算每只股票动量
        mom_values = {}
        for d in self.datas:
            if len(d) >= self.params.lookback:
                mom_values[d._name] = self.mom_dict[d._name][0]

        if not mom_values:
            return

        # 找动量最大的股票
        best_stock_name = max(mom_values, key=mom_values.get)
        best_data = [d for d in self.datas if d._name == best_stock_name][0]

        # 如果当前持仓不是最佳股票，则换股
        if self.current_stock != best_data:
            # 卖出当前持仓
            if self.current_stock is not None and self.getposition(self.current_stock).size > 0:
                pos = self.getposition(self.current_stock)
                self.sell(data=self.current_stock, size=pos.size)
                print(f"{self.data.datetime.date(0)} 卖出: {self.current_stock._name}, 股数: {pos.size}")

            # 买入新股票，全仓
            cash = self.broker.get_cash()
            price = best_data.close[0]
            size = int(cash / price)
            if size > 0:
                self.buy(data=best_data, size=size)
                self.current_stock = best_data
                print(f"{self.data.datetime.date(0)} 买入: {best_stock_name}, 股数: {size}")

# -----------------------
# 回测
# -----------------------
cerebro = bt.Cerebro()
cerebro.addstrategy(MomentumStrategy)

for d in datas:
    cerebro.adddata(d)

cerebro.broker.setcash(100000)
cerebro.broker.setcommission(commission=0.001)

print("初始资金:", cerebro.broker.getvalue())
cerebro.run(runonce=False, stdstats=False)
print("结束资金:", cerebro.broker.getvalue())
