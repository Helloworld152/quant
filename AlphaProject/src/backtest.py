import polars as pl

# 支持相对导入和绝对导入
try:
    from .factors import compute_factors_lazy, process_cross_section
except ImportError:
    from factors import compute_factors_lazy, process_cross_section

def run_backtest(data_path="../data/stocks.parquet", top_n=30):
    """
    运行回测
    
    参数:
        data_path: 数据文件路径
        top_n: 选股数量
    """
    print("构建计算图...")
    # 1. 拿到带有因子的 LazyFrame
    raw_factors = compute_factors_lazy(data_path)
    
    # 2. 清洗并打分
    scored_data = process_cross_section(raw_factors)

    # 3. 选股逻辑
    # 每天按 score 降序排名
    # rank(method="ordinal") 类似于 argsort
    strategy_df = (
        scored_data
        .with_columns(
            pl.col("score").rank(method="ordinal", descending=True).over("date").alias("rank")
        )
        # 只保留排名前 top_n 的
        .filter(pl.col("rank") <= top_n)
        # 计算当天的策略收益 = 持仓股票 next_ret 的平均值
        .group_by("date")
        .agg([
            pl.col("next_ret").mean().alias("strategy_ret")
        ])
        .sort("date")
        .collect() # 此时才真正触发所有计算！Rust 引擎全速运转
    )

    # 4. 计算净值 (Polars 处理起来很快，但 cumprod 还是 Python 侧做图方便)
    # 处理可能的 Null
    df_res = strategy_df.fill_null(0)
    
    # 转回 Pandas 计算净值 (此时数据量只有 日期数 行，非常小)
    pdf_res = df_res.to_pandas()
    pdf_res["cum_nav"] = (1 + pdf_res["strategy_ret"]).cumprod()
    
    print("回测完成，最后净值:", pdf_res["cum_nav"].iloc[-1])
    
    return pdf_res

if __name__ == "__main__":
    run_backtest()