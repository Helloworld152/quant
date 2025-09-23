# download_all_a_stock.py
import akshare as ak
import pandas as pd
import os

# -----------------------------
# 保存路径
# -----------------------------
data_file = "all_a_stocks.csv"

# -----------------------------
# 获取股票列表
# -----------------------------
def get_stock_list(limit=None):
    df_list = ak.stock_zh_a_spot_em()
    stock_list = df_list["代码"].tolist()
    if limit:
        stock_list = stock_list[:limit]  # 测试用，可限制数量
    return stock_list

# -----------------------------
# 下载单只股票数据
# -----------------------------
def download_stock_data(symbol, start_date="20250101", end_date="20250901"):
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date)
        df = df[["日期","开盘","收盘","最高","最低","成交量"]]
        df.columns = ["Date","Open","Close","High","Low","Volume"]
        df["Date"] = pd.to_datetime(df["Date"])
        df["Symbol"] = symbol
        return df
    except Exception as e:
        print(f"{symbol} 数据获取失败: {e}")
        return None

# -----------------------------
# 下载所有股票
# -----------------------------
if __name__ == "__main__":
    stock_list = get_stock_list(limit=100)  # 演示用，实际可去掉 limit
    all_data = []
    for symbol in stock_list:
        df = download_stock_data(symbol)
        if df is not None:
            all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(data_file, index=False)
        print(f"所有股票数据已保存到 {data_file}")
    else:
        print("没有下载到数据")
