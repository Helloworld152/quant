import akshare as ak
import polars as pl
from datetime import datetime

def fetch_and_process_stock(symbol: str, start_date: str, end_date: str):
    # --- 1. 获取数据 (IO 密集型) ---
    print(f"🚀 正在从 AkShare 获取 {symbol} 的数据...")
    
    # stock_zh_a_hist 是获取 A 股日线最常用的接口
    # adjust="qfq" 表示前复权 (量化回测通常用前复权)
    df_pd = ak.stock_zh_a_hist(
        symbol=symbol, 
        period="daily", 
        start_date=start_date, 
        end_date=end_date, 
        adjust="qfq"
    )

    if df_pd.empty:
        print("⚠️ 未获取到数据，请检查代码或日期范围")
        return None

    # --- 2. 转换为 Polars 并清洗 (CPU 密集型) ---
    print("⚡ 正在使用 Polars 进行高性能处理...")
    
    # 2.1 转换 Pandas -> Polars
    df = pl.from_pandas(df_pd)

    # 2.2 定义列名映射 (中文 -> 英文，方便后续代码编写)
    # AkShare 返回的列名通常是中文
    column_mapping = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_chg_ak", # AkShare 自带的涨跌幅，我们后面自己算一个验证
        "涨跌额": "change",
        "换手率": "turnover",
    }

    # 2.3 核心处理逻辑 (链式调用)
    processed_df = (
        df
        # [Rename] 重命名列
        .rename(column_mapping)
        
        # [Select] 只保留需要的列，去除多余的
        .select(["date", "open", "high", "low", "close", "volume"])
        
        # [Type Cast] 类型转换：日期字符串 -> Date 对象, 数值 -> Float64
        .with_columns([
            pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
            pl.col("open").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
        ])
        
        # [Sort] 确保按时间排序 (虽然接口通常是排好的，但保险起见)
        .sort("date")
        
        # --- 3. 量化因子计算 (Vectorized Operations) ---
        .with_columns([
            # 3.1 移动平均线 (MA)
            pl.col("close").rolling_mean(window_size=5).alias("ma_5"),
            pl.col("close").rolling_mean(window_size=20).alias("ma_20"),
            
            # 3.2 对数收益率 (Log Returns) -> ln(Pt / Pt-1)
            # 在金融数学中，对数收益率比简单百分比更优，具有可加性
            (pl.col("close") / pl.col("close").shift(1)).log().alias("log_return"),
            
            # 3.3 真实波动率 (ATR 的简化版 - 仅做演示，计算 20日标准差)
            pl.col("close")
              .rolling_std(window_size=20)
              .alias("volatility_20")
        ])
        
        # [Filter] 去除前面因为 rolling 计算产生的 Null 值 (前20行)
        .drop_nulls()
    )

    return processed_df

if __name__ == "__main__":
    # 获取 2023年至今的数据
    symbol = "600519" # 贵州茅台
    start_date = "20230101"
    end_date = datetime.now().strftime("%Y%m%d")

    df = fetch_and_process_stock(symbol, start_date, end_date)

    if df is not None:
        # 设置 Polars 显示格式，防止中间被省略
        pl.Config.set_tbl_rows(10) 
        print(f"\n📊 {symbol} 处理结果预览:")
        print(df)
        
        # 还可以直接转为 Parquet 存盘，速度极快
        df.write_parquet("kline_data.parquet")
        df.write_csv("kline_data.csv")