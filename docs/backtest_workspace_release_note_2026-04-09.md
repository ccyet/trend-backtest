# Backtest Workspace Release Note

## Summary

本次改造聚焦 Gap_test 回测工作台的页面减负、信号轨迹解释、结果区下钻和数据准备页分离，不改回测内核语义。

## Main Changes

- 左侧栏减负：回测页仅保留范围、日期、数据源、周期、启动入口和数据准备页入口。
- 页面拆分：新增 `回测工作台` / `数据准备页` / `交易配置说明` 三个导航分支。
- 主区重排：改成 `配置摘要 -> 入场策略 -> 基础风控 -> 高级配置 -> 结果区`。
- 策略周期强约束：通过 `config/strategy_capability.py` 收口无效组合。
- 高级配置折叠：过滤、复杂离场、分批止盈、参数扫描与字段映射默认折叠。

## Signal Trace Layer

新增统一的 signal trace 语义层，结果区不再直接依赖零散统计字段猜测漏斗含义。

当前信号轨迹字段覆盖：

- `setup_pass`
- `setup_reason`
- `trigger_pass`
- `trigger_reason`
- `filter_pass`
- `ma_filter_pass`
- `atr_filter_pass`
- `board_ma_filter_pass`
- `imported_filter_pass`
- `trade_closed`
- `reject_reason_chain`
- `execution_skip_reason`

结果区漏斗统一展示：

1. 形态成立
2. 真实触发
3. 过滤后放行
4. 过滤链拦截
5. 成交模拟失败
6. 形成平仓交易
7. 实际执行交易

## Results Area

结果区新增或强化：

- `被拦截信号`
- `成交模拟失败信号`
- `信号轨迹下钻`
- `决策链速览`

`信号轨迹下钻` 当前支持按以下维度筛选：

- 轨迹状态
- 轨迹因子
- 轨迹股票

并支持导出 `signal_trace.csv`。

## Secondary Take Profit

本次把“次级固定止盈”的执行边界明确化：

- 仅在未启用分批止盈时生效
- 位于全仓止损、分批退出、板块均线整笔离场、导入指标整笔离场之后
- 在旧版整笔退出链中，排在利润回撤、均线离场、ATR 跟踪止盈之后
- 一旦触发，对剩余整笔仓位按固定止盈价一次性平仓

## Structural Changes

新增模块：

- `config/strategy_capability.py`
- `pages/backtest.py`
- `pages/data_prep.py`
- `ui/components/summary_cards.py`
- `ui/components/strategy_form.py`
- `ui/components/risk_form.py`
- `ui/components/advanced_panels.py`
- `ui/components/results_view.py`

## Validation

本轮重点回归通过：

- 更大范围 pytest 回归：`139 passed`
- 展示层 unittest：通过

## Known Follow-ups

- 可继续把剩余结果区旧逻辑从 `app.py` 迁到组件模块
- 可进一步为 signal trace 增加字段级过滤快照或更细粒度导出
- 可为无成交场景追加更多 UI 回归测试
