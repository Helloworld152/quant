import akshare as ak
import polars as pl
import time
from tqdm import tqdm
from pathlib import Path
from datetime import datetime, timedelta

def fetch_and_save_data(update_mode="full", start_date="20200101", end_date=None, data_path="../data/stocks.parquet"):
    """
    获取并保存股票数据
    
    参数:
        update_mode: "full" 全量更新 或 "incremental" 增量更新
        start_date: 起始日期，格式 "YYYYMMDD"
        end_date: 结束日期，格式 "YYYYMMDD"，None 则使用今天
        data_path: 数据保存路径
    """
    # 设置结束日期
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    
    # 1. 获取A股所有股票代码
    print("获取股票列表...")
    stock_info = ak.stock_zh_a_spot_em()
    # 示例：只取前50只做演示 (实盘请去掉 .head(50))
    codes = stock_info['代码'].head(50).to_list() 
    
    # 增量模式：读取现有数据，获取每只股票的最新日期
    existing_data = None
    code_max_dates = {}
    if update_mode == "incremental":
        data_file = Path(data_path)
        if data_file.exists():
            print("读取现有数据...")
            try:
                existing_data = pl.read_parquet(data_path)
                # 获取每只股票的最新日期
                code_max_dates = (
                    existing_data
                    .group_by("code")
                    .agg(pl.col("date").max().alias("max_date"))
                    .to_dict(as_series=False)
                )
                code_max_dates = dict(zip(code_max_dates["code"], code_max_dates["max_date"]))
                print(f"找到 {len(code_max_dates)} 只股票的历史数据")
            except Exception as e:
                print(f"读取现有数据失败: {e}，将执行全量更新")
                update_mode = "full"
    
    data_list = []
    
    # 2. 循环下载 (网络IO是瓶颈，Polars在这里帮不上忙，只能等)
    print(f"开始下载 {len(codes)} 只股票数据...")
    for code in tqdm(codes):
        try:
            # 增量模式：计算该股票的起始日期
            fetch_start_date = start_date
            if update_mode == "incremental" and code in code_max_dates:
                # 获取该股票最新日期的下一天
                max_date = code_max_dates[code]
                next_date = max_date + timedelta(days=1)
                fetch_start_date = next_date.strftime("%Y%m%d")
                # 如果最新日期已经等于或晚于结束日期，跳过
                end_date_obj = datetime.strptime(end_date, "%Y%m%d").date()
                if max_date >= end_date_obj:
                    continue
            
            # AkShare 返回的是 Pandas DF，需要转换
            # qfq: 前复权
            df_pandas = ak.stock_zh_a_hist(symbol=code, adjust="qfq", start_date=fetch_start_date, end_date=end_date)
            
            if df_pandas.empty:
                continue

            # Pandas -> Polars (零拷贝转换如果可能的话，但这里会有拷贝)
            df_pl = pl.from_pandas(df_pandas)
            
            # 添加股票代码列 (这是长表的关键 Key)
            df_pl = df_pl.with_columns(pl.lit(code).alias("code"))
            
            # 简单改名方便后续引用
            df_pl = df_pl.rename({
                "日期": "date", "开盘": "open", "收盘": "close", 
                "最高": "high", "最低": "low", "成交量": "volume",
                "换手率": "turnover"
            })
            
            # 选需要的列，并确保 date 列是字符串类型（便于后续统一处理）
            df_pl = df_pl.select(["date", "code", "open", "high", "low", "close", "volume", "turnover"])
            # 确保 date 列是字符串类型
            if df_pl["date"].dtype == pl.Date:
                df_pl = df_pl.with_columns(pl.col("date").cast(pl.Utf8))
            elif df_pl["date"].dtype != pl.Utf8:
                df_pl = df_pl.with_columns(pl.col("date").cast(pl.Utf8))
            data_list.append(df_pl)
            
        except Exception as e:
            print(f"Error fetching {code}: {e}")
            continue

    # 3. 合并与清洗 (Polars 的强项)
    if not data_list:
        if update_mode == "incremental" and existing_data is not None:
            print("无新数据，保持现有数据不变")
            return
        print("无数据下载")
        return

    print("正在合并与清洗...")
    # Lazy Evaluation 模式
    df_all = pl.concat(data_list)
    
    # 数据类型转换与清洗 Pipeline
    # 此时所有 date 列都应该是字符串类型，统一转换为日期
    df_clean = (
        df_all.lazy()
        .with_columns([
            # 日期处理：字符串转日期（AkShare 返回格式通常是 "YYYY-MM-DD"）
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
            pl.col("close").cast(pl.Float32),       # 32位浮点够用了，省一半内存
            pl.col("volume").cast(pl.Float32),
            pl.col("turnover").cast(pl.Float32)
        ])
        .sort(["code", "date"]) # 必须排序，为了后续计算 rolling
    ).collect() # 执行
    
    # 增量模式：合并新旧数据并去重
    if update_mode == "incremental" and existing_data is not None:
        print("合并新旧数据...")
        df_combined = pl.concat([existing_data, df_clean])
        # 按 (code, date) 去重，保留最后一条（新数据优先）
        df_clean = (
            df_combined
            .sort(["code", "date", "close"], descending=[False, False, True])
            .unique(subset=["code", "date"], keep="first")
            .sort(["code", "date"])
        )
    
    # 4. 存为 Parquet (极速读写)
    # 确保目录存在
    Path(data_path).parent.mkdir(parents=True, exist_ok=True)
    df_clean.write_parquet(data_path)
    print(f"数据入库完成，共 {df_clean.height} 行")

if __name__ == "__main__":
    # 全量更新示例
    # fetch_and_save_data(update_mode="full", start_date="20200101", end_date="20231231")
    
    # 增量更新示例
    fetch_and_save_data(update_mode="incremental", start_date="20200101")