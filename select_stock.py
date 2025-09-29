import akshare as ak
import pandas as pd
from datetime import datetime

def pick_stocks(top_n=None):
    # 获取当天A股行情（东方财富 A 股实时行情）
    stock_df = ak.stock_zh_a_spot_em()

    # 基础过滤条件
    df = stock_df[
        (stock_df["量比"] > 2.5) &                      # 量比放大
        (stock_df["换手率"] > 5) & (stock_df["换手率"] < 20) &   # 换手适中
        (stock_df["流通市值"] < 500_00000000) & (stock_df["流通市值"] > 40_00000000) &        # 流通市值小于500亿
        (stock_df["涨跌幅"] > 1) & (stock_df["涨跌幅"] < 5)   # 涨幅合理，排除一字板
    ].copy()

    if df.empty:
        print("今日无符合条件的股票")
        return pd.DataFrame()

    # 添加选股时间
    current_time = datetime.now()
    df["选股时间"] = current_time.strftime("%Y-%m-%d %H:%M:%S")
    df["选股日期"] = current_time.strftime("%Y-%m-%d")

    # 简单打分系统：各指标归一化后求和
    # 注意：市值小更好，所以这里取倒数
    df["score"] = (
        df["量比"].rank(pct=True) * 0.3 +
        df["换手率"].rank(pct=True) * 0.2 +
        (1 - df["流通市值"].rank(pct=True)) * 0.3 +
        df["涨跌幅"].rank(pct=True) * 0.2
    )

    # 按得分排序，取TopN
    df = df.sort_values("score", ascending=False).head(top_n)

    return df[["选股时间","选股日期","代码","名称","最新价","涨跌幅","量比","换手率","流通市值","成交额","score"]]


if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    result = pick_stocks()
    if not result.empty:
        # 输出到CSV文件，按时间命名
        csv_filename = f"selected_stocks_{current_time}.csv"
        result.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        print(f"\n选股结果已保存到文件: {csv_filename}")
        
        # 同时追加到历史记录文件
        history_filename = "selected_stocks_history.csv"
        try:
            # 尝试读取现有历史文件
            history_df = pd.read_csv(history_filename, encoding="utf-8-sig")
            # 合并新结果
            combined_df = pd.concat([history_df, result], ignore_index=True)
        except FileNotFoundError:
            # 如果历史文件不存在，直接使用当前结果
            combined_df = result
        
        # 按选股时间排序（最新的在前）
        combined_df = combined_df.sort_values("选股时间", ascending=False)
        # 保存历史记录
        combined_df.to_csv(history_filename, index=False, encoding="utf-8-sig")
        print(f"历史记录已更新: {history_filename}")
    else:
        print("今日无符合条件的股票，未生成CSV文件")
