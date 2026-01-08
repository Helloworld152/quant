import pandas as pd
import re
import os
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
    df["v1"] = pd.to_numeric(df["v1"], errors="coerce")
    
    # 计算金额和方向
    df["side"] = df["delta"].apply(lambda x: "buy" if x > 0 else "sell")
    df["amount"] = abs(df["delta"]) * df["price"]
    
    # v1是交易前持仓，计算交易后的持仓
    df["position_after"] = df["v1"] + df["delta"]
    
    return df

def generate_trade_report(df, output_date):
    """生成交易报告并保存为CSV"""
    if df is None or df.empty:
        return
    
    # 当前仓位汇总
    pos_summary = df.groupby("symbol")["delta"].sum().reset_index()
    pos_summary.columns = ["symbol", "position"]
    pos_summary = pos_summary[pos_summary["position"] != 0]  # 只保留非零仓位
    
    # 使用日志中的最新价格计算市值
    prices = {}
    for symbol in pos_summary["symbol"]:
        symbol_trades = df[df["symbol"] == symbol]
        if not symbol_trades.empty:
            prices[symbol] = symbol_trades["price"].iloc[-1]  # 使用最后一次交易的价格
    
    # 计算市值
    pos_summary["current_price"] = pos_summary["symbol"].map(prices)
    pos_summary = pos_summary.dropna(subset=["current_price"])
    pos_summary["market_value"] = abs(pos_summary["position"]) * pos_summary["current_price"]
    
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
    
    # 合并买入和卖出记录到一个CSV，买入在上，卖出在下
    all_trades = pd.concat([buy_df, sell_df], ignore_index=True)
    # 不按金额排序，保持买入在上卖出在下的顺序
    
    # 为每笔交易计算交易后的市值
    all_trades["current_position"] = all_trades["position_after"]
    all_trades["current_price"] = all_trades["price"]  # 使用交易价格作为当前价格
    all_trades["current_market_value"] = abs(all_trades["position_after"]) * all_trades["price"]
    
    # 选择要输出的列
    output_columns = ["symbol", "side", "delta", "price", "amount", "current_position", "current_price", "current_market_value"]
    trade_with_position = all_trades[output_columns]
    
    # 格式化数值列保留三位小数
    numeric_columns = ['price', 'amount', 'current_price', 'current_market_value']
    for col in numeric_columns:
        if col in trade_with_position.columns:
            trade_with_position[col] = trade_with_position[col].round(3)
    
    # 保存合并的交易记录
    trades_file = f"{output_dir}/all_trades_{output_date}.csv"
    trade_with_position.to_csv(trades_file, index=False, encoding='utf-8-sig')
    print(f"合并交易记录已保存到: {trades_file}")
    
    # 打印统计信息
    print(f"\n===== {output_date} 交易统计 =====")
    print(f"总交易笔数: {len(df)}")
    print(f"买入笔数: {len(buy_df)}")
    print(f"卖出笔数: {len(sell_df)}")
    print(f"买入金额: {buy_df['amount'].sum():.2f}")
    print(f"卖出金额: {sell_df['amount'].sum():.2f}")
    print(f"净流入: {buy_df['amount'].sum() - sell_df['amount'].sum():.2f}")
    print(f"持仓股票数: {len(pos_summary)}")
    print(f"总持仓市值: {pos_summary['market_value'].sum():,.2f} 元")

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

if __name__ == "__main__":
    main()
