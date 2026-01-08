"""
量化因子化流程主程序
整合数据获取、因子计算、横截面处理和回测的完整流程
"""
import argparse
from pathlib import Path
from src.data_fetch import fetch_and_save_data
from src.factors import compute_factors_lazy, process_cross_section
from src.backtest import run_backtest
import polars as pl


def run_full_pipeline(
    update_data=False,
    update_mode="incremental",
    start_date="20200101",
    end_date=None,
    data_path="./data/stocks.parquet",
    top_n=30,
    save_results=True
):
    """
    运行完整的因子化流程
    
    参数:
        update_data: 是否更新数据
        update_mode: "full" 全量更新 或 "incremental" 增量更新
        start_date: 起始日期，格式 "YYYYMMDD"
        end_date: 结束日期，格式 "YYYYMMDD"，None 则使用今天
        data_path: 数据保存路径
        top_n: 选股数量（排名前N只）
        save_results: 是否保存回测结果
    """
    # 步骤1: 数据获取（可选）
    if update_data:
        print("=" * 60)
        print("步骤1: 数据获取")
        print("=" * 60)
        fetch_and_save_data(
            update_mode=update_mode,
            start_date=start_date,
            end_date=end_date,
            data_path=data_path
        )
        print()
    else:
        # 检查数据文件是否存在
        if not Path(data_path).exists():
            print(f"警告: 数据文件 {data_path} 不存在，将执行数据获取...")
            fetch_and_save_data(
                update_mode="full",
                start_date=start_date,
                end_date=end_date,
                data_path=data_path
            )
            print()
    
    # 步骤2: 因子计算
    print("=" * 60)
    print("步骤2: 因子计算")
    print("=" * 60)
    print("构建因子计算图...")
    raw_factors = compute_factors_lazy(data_path)
    print(f"因子计算图构建完成，Schema: {raw_factors.schema}")
    print()
    
    # 步骤3: 横截面处理
    print("=" * 60)
    print("步骤3: 横截面处理（去极值、标准化、因子合成）")
    print("=" * 60)
    scored_data = process_cross_section(raw_factors)
    print("横截面处理完成")
    print()
    
    # 步骤4: 回测
    print("=" * 60)
    print("步骤4: 回测执行")
    print("=" * 60)
    print(f"选股策略: 每日选择排名前 {top_n} 只股票")
    
    # 选股逻辑
    strategy_df = (
        scored_data
        .with_columns(
            pl.col("score").rank(method="ordinal", descending=True).over("date").alias("rank")
        )
        .filter(pl.col("rank") <= top_n)
        .group_by("date")
        .agg([
            pl.col("next_ret").mean().alias("strategy_ret"),
            pl.col("code").count().alias("stock_count")
        ])
        .sort("date")
        .collect()
    )
    
    # 计算净值
    df_res = strategy_df.fill_null(0)
    pdf_res = df_res.to_pandas()
    pdf_res["cum_nav"] = (1 + pdf_res["strategy_ret"]).cumprod()
    
    # 计算统计指标
    total_return = pdf_res["cum_nav"].iloc[-1] - 1
    annual_return = (pdf_res["cum_nav"].iloc[-1] ** (252 / len(pdf_res)) - 1) if len(pdf_res) > 0 else 0
    sharpe_ratio = pdf_res["strategy_ret"].mean() / pdf_res["strategy_ret"].std() * (252 ** 0.5) if pdf_res["strategy_ret"].std() > 0 else 0
    max_drawdown = ((pdf_res["cum_nav"] / pdf_res["cum_nav"].cummax()) - 1).min()
    
    print(f"\n回测结果:")
    print(f"  总收益率: {total_return:.2%}")
    print(f"  年化收益率: {annual_return:.2%}")
    print(f"  夏普比率: {sharpe_ratio:.2f}")
    print(f"  最大回撤: {max_drawdown:.2%}")
    print(f"  最终净值: {pdf_res['cum_nav'].iloc[-1]:.4f}")
    print(f"  回测天数: {len(pdf_res)}")
    print()
    
    # 保存结果
    if save_results:
        results_path = Path(data_path).parent / "backtest_results.parquet"
        df_res.write_parquet(results_path)
        print(f"回测结果已保存至: {results_path}")
        
        # 保存净值曲线数据
        nav_path = Path(data_path).parent / "nav_curve.csv"
        pdf_res[["date", "strategy_ret", "cum_nav", "stock_count"]].to_csv(nav_path, index=False)
        print(f"净值曲线已保存至: {nav_path}")
        print()
    
    print("\n" + "=" * 60)
    print("流程完成！")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="量化因子化流程")
    parser.add_argument("--update-data", action="store_true", help="是否更新数据")
    parser.add_argument("--update-mode", choices=["full", "incremental"], default="incremental", 
                       help="更新模式: full(全量) 或 incremental(增量)")
    parser.add_argument("--start-date", type=str, default="20200101", 
                       help="起始日期，格式: YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, 
                       help="结束日期，格式: YYYYMMDD，默认今天")
    parser.add_argument("--data-path", type=str, default="./data/stocks.parquet",
                       help="数据文件路径")
    parser.add_argument("--top-n", type=int, default=30,
                       help="选股数量（排名前N只）")
    parser.add_argument("--no-save", action="store_true",
                       help="不保存回测结果")
    
    args = parser.parse_args()
    
    run_full_pipeline(
        update_data=args.update_data,
        update_mode=args.update_mode,
        start_date=args.start_date,
        end_date=args.end_date,
        data_path=args.data_path,
        top_n=args.top_n,
        save_results=not args.no_save
    )


if __name__ == "__main__":
    # 如果直接运行，使用默认参数
    # 可以通过命令行参数自定义
    import sys
    if len(sys.argv) > 1:
        main()
    else:
        # 默认运行：不更新数据，使用增量模式（如果数据不存在则全量获取）
        run_full_pipeline(
            update_data=False,
            update_mode="incremental",
            start_date="20200101",
            end_date=None,
            top_n=30,
            save_results=True
        )
