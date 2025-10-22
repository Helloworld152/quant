import pandas as pd
import akshare as ak
import re
import os
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from datetime import datetime, date

def parse_trade_log(log_file_path):
    """解析交易日志文件"""
    # 用正则匹配调仓行（形如 6位数字+.SZ/.SH 开头）
    pattern = re.compile(r"^\d{6}\.(SZ|SH)\|")
    
    # 读取日志文件，提取调仓行
    lines = []
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if pattern.match(line):
                    lines.append(line)
    except FileNotFoundError:
        print(f"日志文件未找到: {log_file_path}")
        return None
    
    if not lines:
        print("未找到调仓记录")
        return None
    
    # 转成 DataFrame
    df = pd.DataFrame([l.split("|") for l in lines],
                      columns=["symbol", "delta", "date", "time", "v1", "v2", "price"])
    
    # 转数字
    df["delta"] = pd.to_numeric(df["delta"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    
    # 计算金额和方向
    df["side"] = df["delta"].apply(lambda x: "buy" if x > 0 else "sell")
    df["amount"] = abs(df["delta"]) * df["price"]
    
    return df

def generate_trade_report(df, output_date):
    """生成交易报告并保存为CSV"""
    if df is None or df.empty:
        return
    
    # 当前仓位汇总
    pos_summary = df.groupby("symbol")["delta"].sum().reset_index()
    pos_summary.columns = ["symbol", "position"]
    pos_summary = pos_summary[pos_summary["position"] != 0]  # 只保留非零仓位
    
    # 买入/卖出明细
    buy_df = df[df["delta"] > 0].sort_values("amount", ascending=False)
    sell_df = df[df["delta"] < 0].sort_values("amount", ascending=False)
    
    # 输出到CSV文件
    output_dir = "trade_reports"
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存仓位汇总
    pos_file = f"{output_dir}/positions_{output_date}.csv"
    pos_summary.to_csv(pos_file, index=False, encoding='utf-8-sig')
    print(f"仓位汇总已保存到: {pos_file}")
    
    # 保存买入明细
    buy_file = f"{output_dir}/buy_trades_{output_date}.csv"
    buy_df[["symbol", "delta", "price", "amount", "date", "time"]].to_csv(buy_file, index=False, encoding='utf-8-sig')
    print(f"买入明细已保存到: {buy_file}")
    
    # 保存卖出明细
    sell_file = f"{output_dir}/sell_trades_{output_date}.csv"
    sell_df[["symbol", "delta", "price", "amount", "date", "time"]].to_csv(sell_file, index=False, encoding='utf-8-sig')
    print(f"卖出明细已保存到: {sell_file}")
    
    # 打印统计信息
    print(f"\n===== {output_date} 交易统计 =====")
    print(f"总交易笔数: {len(df)}")
    print(f"买入笔数: {len(buy_df)}")
    print(f"卖出笔数: {len(sell_df)}")
    print(f"买入金额: {buy_df['amount'].sum():.2f}")
    print(f"卖出金额: {sell_df['amount'].sum():.2f}")
    print(f"净流入: {buy_df['amount'].sum() - sell_df['amount'].sum():.2f}")
    print(f"持仓股票数: {len(pos_summary)}")

def plot_position_chart(df, output_date):
    """绘制持仓占比图"""
    if df is None or df.empty:
        return
    
    # 获取调仓后的净持仓数据（日志中每只股票只有一条记录，表示最终持仓）
    pos_summary = df.groupby("symbol")["delta"].sum().reset_index()
    pos_summary.columns = ["symbol", "position"]
    pos_summary = pos_summary[pos_summary["position"] != 0]
    
    if pos_summary.empty:
        print("没有持仓数据，无法绘制图表")
        return
    
    # 获取股票价格
    prices = {}
    print("正在获取股票价格...")
    
    for symbol in pos_summary["symbol"]:
        try:
            code = symbol[:6]
            # 获取实时价格
            df_daily = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20250101", end_date=output_date, adjust="")
            if not df_daily.empty:
                latest_price = df_daily.iloc[-1]["收盘"]
                prices[symbol] = latest_price
            else:
                # 如果获取不到历史数据，使用交易日志中的价格
                symbol_trades = df[df["symbol"] == symbol]
                if not symbol_trades.empty:
                    prices[symbol] = symbol_trades["price"].iloc[-1]
        except Exception as e:
            print(f"获取 {symbol} 价格失败: {e}")
            # 使用交易日志中的价格作为备选
            symbol_trades = df[df["symbol"] == symbol]
            if not symbol_trades.empty:
                prices[symbol] = symbol_trades["price"].iloc[-1]
    
    # 计算市值
    pos_summary["price"] = pos_summary["symbol"].map(prices)
    pos_summary = pos_summary.dropna(subset=["price"])
    pos_summary["market_value"] = abs(pos_summary["position"]) * pos_summary["price"]
    
    # 只显示市值大于0的股票
    pos_summary = pos_summary[pos_summary["market_value"] > 0]
    
    if pos_summary.empty:
        print("没有有效的持仓数据，无法绘制图表")
        return
    
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 创建图表
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. 持仓占比饼图（Top 10）
    top_10 = pos_summary.nlargest(10, "market_value")
    other_value = pos_summary.iloc[10:]["market_value"].sum() if len(pos_summary) > 10 else 0
    
    labels = []
    sizes = []
    colors = []
    
    for _, row in top_10.iterrows():
        symbol_short = row["symbol"][:6]  # 只显示股票代码，不显示后缀
        labels.append(f"{symbol_short}\n({row['position']:+.0f}股)")
        sizes.append(row["market_value"])
        # 根据买卖方向设置颜色
        colors.append("#ff6b6b" if row["position"] > 0 else "#4ecdc4")
    
    if other_value > 0:
        labels.append("其他")
        sizes.append(other_value)
        colors.append("#95a5a6")
    
    # 饼图
    wedges, texts, autotexts = ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', 
                                       startangle=90, textprops={'fontsize': 8})
    ax1.set_title(f"{output_date} 调仓后持仓市值占比 (Top 10)", fontsize=14, fontweight='bold')
    
    # 2. 持仓金额条形图（Top 15）
    top_15 = pos_summary.nlargest(15, "market_value")
    
    symbol_labels = [row["symbol"][:6] for _, row in top_15.iterrows()]
    market_values = top_15["market_value"].values
    position_values = top_15["position"].values
    
    # 根据买卖方向设置颜色
    bar_colors = ["#ff6b6b" if pos > 0 else "#4ecdc4" for pos in position_values]
    
    bars = ax2.bar(range(len(symbol_labels)), market_values, color=bar_colors, alpha=0.8)
    ax2.set_xlabel('股票代码', fontsize=12)
    ax2.set_ylabel('市值 (元)', fontsize=12)
    ax2.set_title(f"{output_date} 调仓后持仓市值排行 (Top 15)", fontsize=14, fontweight='bold')
    ax2.set_xticks(range(len(symbol_labels)))
    ax2.set_xticklabels(symbol_labels, rotation=45, ha='right')
    
    # 在条形图上添加数值标签
    for i, (bar, value, pos) in enumerate(zip(bars, market_values, position_values)):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{value:,.0f}\n({pos:+.0f}股)', ha='center', va='bottom', fontsize=8)
    
    # 添加图例
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor='#ff6b6b', label='买入持仓'),
                      Patch(facecolor='#4ecdc4', label='卖出持仓')]
    ax2.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    
    # 保存图片
    output_dir = "trade_reports"
    chart_file = f"{output_dir}/position_chart_{output_date}.png"
    plt.savefig(chart_file, dpi=300, bbox_inches='tight')
    print(f"持仓占比图已保存到: {chart_file}")
    
    # 显示图表
    plt.show()
    
    # 打印详细统计
    total_market_value = pos_summary["market_value"].sum()
    print(f"\n===== 持仓统计 =====")
    print(f"总持仓市值: {total_market_value:,.2f} 元")
    print(f"持仓股票数: {len(pos_summary)}")
    
    buy_positions = pos_summary[pos_summary["position"] > 0]
    sell_positions = pos_summary[pos_summary["position"] < 0]
    
    if not buy_positions.empty:
        print(f"买入持仓市值: {buy_positions['market_value'].sum():,.2f} 元")
        print(f"买入股票数: {len(buy_positions)}")
    
    if not sell_positions.empty:
        print(f"卖出持仓市值: {sell_positions['market_value'].sum():,.2f} 元")
        print(f"卖出股票数: {len(sell_positions)}")

def main():
    """主函数：处理今天的日志文件"""
    import sys
    
    # 检查是否指定了日期参数
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = date.today().strftime("%Y%m%d")
    
    # 查找指定日期的日志文件
    log_dir = "trade_log"
    log_files = [f for f in os.listdir(log_dir) if f.startswith(f"ta_{target_date}")]
    
    if not log_files:
        print(f"未找到 {target_date} 的日志文件")
        print(f"请在 {log_dir} 目录下查找包含 'ta_{target_date}' 的文件")
        print("可用文件:")
        for f in os.listdir(log_dir):
            if f.startswith("ta_"):
                print(f"  {f}")
        
        # 如果有其他日期的文件，询问是否处理最新的
        all_log_files = [f for f in os.listdir(log_dir) if f.startswith("ta_")]
        if all_log_files:
            latest_file = max(all_log_files, key=lambda x: os.path.getmtime(os.path.join(log_dir, x)))
            print(f"\n是否处理最新文件 {latest_file}? (y/n)")
            choice = input().lower()
            if choice == 'y':
                log_file = latest_file
                # 从文件名提取日期
                target_date = log_file.split('_')[1][:8]  # 提取ta_20251017_140114中的日期部分
            else:
                return
        else:
            return
    else:
        # 如果有多个文件，取最新的
        log_file = max(log_files, key=lambda x: os.path.getmtime(os.path.join(log_dir, x)))
    
    log_path = os.path.join(log_dir, log_file)
    print(f"处理日志文件: {log_path}")
    
    # 解析日志
    df = parse_trade_log(log_path)
    
    # 生成报告
    generate_trade_report(df, target_date)
    
    # 绘制持仓占比图
    plot_position_chart(df, target_date)

if __name__ == "__main__":
    main()
