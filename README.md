# Python 回测框架说明文档

一个基于 Backtrader 的轻量回测框架，支持：
- 策略参数网格回测（以双均线策略为例）
- 多标的批量回测与断点续跑
- 结果表格中文输出与 CSV 导出（两位小数）
- 选择回测模式（仅未回测/全部重测）

## 目录结构
- `backtest/engine.py`：回测引擎与数据加载
- `strategies/dual_ma.py`：示例策略（双均线）
- `backtrade.py`：回测入口脚本（参数网格、批量回测、导出）
- `mongo_py/stock_data.py`：数据源封装（AkShare + 本地DB）
- `symbols.csv`：标的清单（用户可编辑）

## 安装依赖
```bash
pip install -r requirements.txt
```

## 数据源
- 通过 `mongo_py/stock_data.py` 从 AkShare 获取并写入本地数据库，支持按日期段增量填充。
- 回测时由 `backtest/engine.py` 的 `load_bt_data` 读取、清洗（去重、数值化、过滤非正价格、修正高低价、裁剪开收盘）。

## 回测引擎
入口函数：`backtest.engine.run_backtest`
- 参数：
  - `symbols`：单标的或列表
  - `start_date, end_date`：日期（形如 `YYYYMMDD`）
  - `strategy_cls, strategy_params`：策略类与参数
  - `initial_cash`：初始资金（默认 100000）
  - `commission`：手续费（默认 0.001）
  - `cheat_on_close`：按收盘撮合（默认 True）
  - `add_analyzers`：是否添加指标分析器（默认 True）
  - `verbose`：是否打印过程与指标（默认 True）
- 返回：`{cerebro, results, final_value, analyzers}`
- 指标：夏普、最大回撤（金额/百分比）、累计收益、年化收益、交易笔数/胜率等

## 标的清单与回测模式
- `symbols.csv` 列：`symbol,tested,last_run`
  - `tested`：0=未回测，1=已回测
  - 用户可以直接在该文件追加新股票代码
- 在 `backtrade.py`：
  - `mode='pending'`：仅回测 `tested != 1` 的标的（结果追加写入）
  - `mode='all'`：回测全部标的（结果覆盖写入）

## 回测入口（参数网格 + 批量）
- 在 `backtrade.py` 中配置：
  - 回测区间：`start_date, end_date`
  - 参数网格：`fast_list, slow_list`（自动跳过 `fast >= slow`）
  - 读取 `symbols.csv`，根据 `mode` 选择回测批次
  - 每只标的完成后写回 `symbols.csv`（标记 `tested=1`，记录 `last_run`）

## 结果输出
- 控制台：中文表格预览（两位小数）
- CSV：`ma_grid_results.csv`
  - `mode='pending'`：追加写入（首行带表头）
  - `mode='all'`：覆盖写入
- 主要列：
  - `标的, 开始日期, 结束日期, 短期均线, 长期均线, 期末资金, 夏普比率, 最大回撤(%), 最大回撤金额, 累计收益(%), 年化收益(%), 交易笔数, 胜率(%)`
- 排序：先按 `symbol` 升序，再按 “累计收益(%)” 降序

## 自定义策略
1. 在 `strategies/` 新增策略类，继承 `bt.Strategy`
2. 在 `strategies/__init__.py` 导出策略类
3. 在 `backtrade.py` 中将 `strategy_cls` 指向你的策略，并设置 `strategy_params`

## 常见问题
- 负价格/异常值：引擎已进行数据清洗，若仍有异常，请反馈具体日期以排查来源。
- 累计收益计算：使用资金口径 `(期末资金/初始资金-1)*100`。
- 指标偏差：短样本或波动大时，`rnorm100` 与直觉年化可能略有偏差。
