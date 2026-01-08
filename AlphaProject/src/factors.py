import polars as pl

def compute_factors_lazy(data_path="../data/stocks.parquet"):
    """
    计算因子（Lazy模式）
    
    参数:
        data_path: 数据文件路径
    """
    # 使用 scan_parquet 开启 Lazy 模式 (不一次性读入内存，构建计算图)
    # 这对于几千只股票的数据处理至关重要
    q = pl.scan_parquet(data_path)

    # 定义因子计算表达式
    factors = q.with_columns([
        # 1. 收益率 (Label)：下期收益率 (Shift -1)
        # 这里的 over('code') 保证了 shift 是在每只股票内部进行的
        pl.col("close").pct_change().shift(-1).over("code").alias("next_ret"),

        # 2. 因子：Log成交量 (代表资金关注度)
        pl.col("volume").log().alias("factor_log_vol"),

        # 3. 因子：换手率 (直接有)
        pl.col("turnover").alias("factor_turnover"),

        # 4. 因子：量比 (复杂 Rolling 计算)
        # rolling_mean 需要先按时间排序，AkShare数据下载时若已排序则不需要重排
        (
            pl.col("volume") / 
            pl.col("volume").rolling_mean(window_size=5).over("code")
        ).alias("factor_vol_ratio"),
        
        # 5. 因子：波动率 (过去20天收盘价标准差)
        pl.col("close").rolling_std(window_size=20).over("code").alias("factor_volatility")
    ])

    # 过滤掉计算产生的 Null (比如前5天没有MA)
    factors = factors.filter(pl.col("factor_vol_ratio").is_not_null())

    return factors  # 返回的是 LazyFrame，还没计算

def process_cross_section(lf: pl.LazyFrame):
    # 定义去极值表达式 (Clip at 1% and 99% quantile)
    # 注意：这里是在 "date" 维度上 over，即每天处理所有股票
    def winsorize(col_name):
        return (
            pl.col(col_name)
            .clip(
                min_val=pl.col(col_name).quantile(0.01).over("date"),
                max_val=pl.col(col_name).quantile(0.99).over("date")
            )
        )
    
    # 定义标准化表达式 (Z-Score)
    def standardize(col_name):
        return (
            (pl.col(col_name) - pl.col(col_name).mean().over("date")) /
            (pl.col(col_name).std().over("date") + 1e-6) # 防止除零
        )

    # 应用处理
    # 假设我们合成一个 simple_score = 0.5*量比 + 0.5*换手率 (追热点策略)
    processed = lf.with_columns([
        standardize("factor_vol_ratio").alias("z_vol_ratio"),
        standardize("factor_turnover").alias("z_turnover")
    ])

    # 因子合成
    final_score = processed.with_columns(
        (pl.col("z_vol_ratio") * 0.5 + pl.col("z_turnover") * 0.5).alias("score")
    )
    
    return final_score

if __name__ == "__main__":
    lf = compute_factors_lazy()
    # 可以在这里做 .collect() 落盘，或者直接传给回测
    # 为了演示，我们先打印 Schema
    print(lf.schema)