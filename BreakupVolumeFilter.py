import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time

def get_recent_limit_up_stocks_with_data(days=20):
    """获取最近 days 日有过涨停的股票，并返回数据缓存"""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")  # 多留几天确保有交易日

    try:
        all_stocks = ak.stock_zh_a_spot_em()["代码"].tolist()
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        # 使用预定义的测试股票列表
        all_stocks = ["000001", "000002", "000858", "002415", "300059", "600036", "600519", "000858"]
    
    candidates = {}  # 改为字典，同时保存股票代码和数据
    print(f"开始处理 {len(all_stocks)} 只股票...")

    for i, code in enumerate(all_stocks):
        if i % 100 == 0:
            print(f"已处理 {i}/{len(all_stocks)} 只股票")
        
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if df.empty:
                continue
            df = df.sort_values("日期")
            df["pct"] = (df["收盘"] - df["收盘"].shift(1)) / df["收盘"].shift(1) * 100
            if (df.tail(days)["pct"] >= 9.5).any():  # 最近 days 日有涨停
                candidates[code] = df  # 保存数据，避免重复获取
                print(f"找到候选股票: {code}")
        except Exception as e:
            if "Connection" in str(e):
                print(f"网络错误，等待重试...")
                time.sleep(1)  # 网络错误时等待1秒
            continue
        
        # 避免请求过快
        time.sleep(0.1)
        
    return candidates

def check_down_after_limit(symbol, df_cache=None, days=20):
    """判断候选股票最近 days 日是否出现涨停后放量下跌"""
    # 如果有缓存数据，直接使用，避免重复API调用
    if df_cache is not None:
        df = df_cache
    else:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        except:
            return pd.DataFrame()

    if df.empty or len(df) < 2:
        return pd.DataFrame()

    df = df.sort_values("日期")
    df["prev_close"] = df["收盘"].shift(1)
    df["prev_pct"] = (df["收盘"] - df["prev_close"]) / df["prev_close"] * 100
    df["prev_limit_up"] = df["prev_pct"] >= 9.5
    df["volume_ratio"] = df["成交量"] / df["成交量"].rolling(5).mean()
    df["down_after_limit"] = (df["prev_limit_up"]) & (df["收盘"] < df["prev_close"]) & (df["volume_ratio"] > 1.5)

    result = df[df["down_after_limit"]]
    if not result.empty:
        result["代码"] = symbol
        return result[["代码","日期","开盘","最高","最低","收盘","成交量","volume_ratio","prev_close","prev_pct"]]
    return pd.DataFrame()

if __name__ == "__main__":
    print("正在筛选最近20日涨停的股票...")
    candidates_with_data = get_recent_limit_up_stocks_with_data(days=20)
    print(f"候选股票数量: {len(candidates_with_data)}")

    final_df = pd.DataFrame()
    for code, df_data in candidates_with_data.items():
        res = check_down_after_limit(code, df_cache=df_data, days=20)
        if not res.empty:
            final_df = pd.concat([final_df, res], ignore_index=True)

    if not final_df.empty:
        today = datetime.now().strftime("%Y%m%d")
        print(f"=== {today} 涨停后放量下跌股票 ===")
        print(final_df)
        # 保存到 CSV
        final_df.to_csv(f"down_after_limit_{today}.csv", index=False, encoding="utf-8-sig")
    else:
        print("今日无符合条件的股票")
