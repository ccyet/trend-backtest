# Gap_test（A 股日线策略研究回测）

一个基于 **Streamlit + Pandas** 的 A 股日线研究型回测工作台。  
项目定位是：**离线可复现、语义明确、方便做参数研究**，而不是高频撮合级仿真。

---

## 1. 目前支持的核心能力

- **离线行情更新**（AKShare 仅用于下载层，回测核心不直接依赖）
- **多数据源回测输入**：本地 Parquet / SQLite / 上传 Excel/CSV
- **单账户单持仓回测框架**（研究型 long/short 镜像）
- **五类入场因子**
  - `gap`（跳空）
  - `trend_breakout`（趋势突破）
  - `volatility_contraction_breakout`（波动收缩突破）
  - `candle_run`（连续K线追势）
  - `candle_run_acceleration`（连续K线加速追势）
- **退出风控体系**
  - 全仓止损
  - 分批退出（2~3 批，按优先级）
  - 旧版整笔退出（仅在未启用分批时）
  - 时间退出（strict / force_close）
- **交易成本与滑点**
- **参数敏感性扫描**（最多 2 维，组合上限可控）
- **交易配置说明页**（工作台 / 说明页切换）
- **结果导出**（明细 / 日汇总 / 净值 / 扫描结果）

---

## 1.1 本次新增方案的落地选择

围绕“连续阳线追涨 / 连续阴线追空”这条扩展线，本次最终**落地 2 个方案**：

1. `candle_run`
   - 面向连续同向 K 线组合的基础追势方案
   - 关注连续根数、单根最小实体幅度、组合累计涨跌幅
2. `candle_run_acceleration`
   - 在 `candle_run` 基础上增加“实体强度不递减”的加速约束
   - 适合研究更强势的连续推进场景

这两个方案都已完成参数接线、UI 暴露、信号生成、策略统计与测试覆盖；`30m / 15m` 当前仍作为周期插座保留，未宣称已完成完整多周期回测。

---

## 2. 关键执行语义（务必先看）

### 2.1 严禁未来函数（No Lookahead）

所有入场触发依据都使用 **T-1 及更早**数据构建；T 日只用于判定是否触发成交。

### 2.2 五类入场因子

#### A) `gap`（兼容原行为）

- `strict_break`
  - long: `open > prev_high * (1 + gap_ratio)`
  - short: `open < prev_low * (1 - gap_ratio)`
- `open_vs_prev_close_threshold`
  - long: `gap_pct_vs_prev_close >= gap_pct`
  - short: `gap_pct_vs_prev_close <= -gap_pct`

> 默认仍是 `entry_factor="gap"`，保持老配置可直接运行。

#### B) `trend_breakout`

- 基于过去窗口（不含当日）的高/低点生成突破触发价
- long 触发价：`rolling_max(high.shift(1), lookback)`
- short 触发价：`rolling_min(low.shift(1), lookback)`

#### C) `volatility_contraction_breakout`

- 先做收缩门槛：`prior_range = high.shift(1) - low.shift(1)`
- 收缩成立：`prior_range == rolling_min(prior_range, vcb_range_lookback)`
- 再结合突破触发价（同样使用 shift(1) 窗口）

#### D) `candle_run`

- 基于 **T-1 及更早** 的连续同向 K 线组合生成信号
- long：要求前序连续 `N` 根阳线；short：要求前序连续 `N` 根阴线
- 可配置约束：
  - `candle_run_length`：连续根数
  - `candle_run_min_body_pct`：单根最小实体幅度
  - `candle_run_total_move_pct`：组合最小累计涨跌幅
- 信号成立后，按 **下一根 K 线开盘**追入/追空

#### E) `candle_run_acceleration`

- 在 `candle_run` 基础上，额外要求前序连续 K 线的实体强度 **不递减**
- long / short 方向与 `candle_run` 镜像一致
- 同样按 **下一根 K 线开盘**执行

### 2.3 成交模型（按因子区分）

突破类（`trend_breakout` / `volatility_contraction_breakout`）采用 stop-entry：

- **long**
  1. `open >= trigger` → 按 `open` 成交
  2. `open < trigger <= high` → 按 `trigger` 成交
  3. 否则不成交（`entry_not_filled`）
- **short**（镜像）
  1. `open <= trigger` → 按 `open` 成交
  2. `open > trigger >= low` → 按 `trigger` 成交
  3. 否则不成交

成交价再叠加买卖滑点。

连续K线类（`candle_run` / `candle_run_acceleration`）不使用 trigger 价：

- 信号由前序组合完成后，在下一根 K 线按 `open` 直接追入 / 追空
- 明细中的 `entry_fill_type` 为 `open`
- `entry_trigger_price` 为空

### 2.4 不可成交过滤

突破类额外过滤：

- `volume <= 0`
- 一字价条（`open == high == low == close`）

缺失 `volume` 不再直接视为不可成交，便于指数 / ETF 等缺失成交量的数据继续按价格语义回测；
明确非正成交量和一字价条仍会记为 `locked_bar_unfillable` 并跳过。

### 2.5 开仓当日不退出

退出检查从持有第 1 天开始（`holding_days >= 1`），避免日线下不可判定的日内先后顺序歧义。

---

## 3. 参数扫描设计（研究友好 + 可控）

### 3.1 固定规则

- 扫描维度：**1~2 维**
- 字段类型：**仅数值字段**
- 组合方式：笛卡尔积
- 组合上限：`max_combinations`

### 3.2 因子感知扫描字段

扫描字段会按当前 `entry_factor` 动态约束，避免无效组合。

- `gap`：可扫 `gap_pct` / `max_gap_filter_pct` 及公共风险参数
- `trend_breakout`：可扫 `trend_breakout_lookback` 及公共风险参数
- `volatility_contraction_breakout`：可扫 `vcb_range_lookback` / `vcb_breakout_lookback` 及公共风险参数
- `candle_run` / `candle_run_acceleration`：可扫 `candle_run_length` / `candle_run_min_body_pct` / `candle_run_total_move_pct` 及公共风险参数

### 3.3 结果保留策略

- 扫描表保留全组合统计
- 明细/日汇总/净值仅保留最优组合 payload

---

## 4. 项目结构（核心模块）

```text
app.py            # Streamlit 界面与参数组装
models.py         # 参数模型、扫描配置、参数校验
rules.py          # 入场筛选、成交模拟、退出语义
analyzer.py       # 候选交易、策略交易、净值与扫描汇总
exporter.py       # Excel 导出

scripts/update_data.py
data/providers/akshare_provider.py
data/services/local_data_service.py
```

---

## 5. 数据流

### 5.1 离线更新流

1. UI 或 CLI 调用 `scripts/update_data.py`
2. `data/providers/akshare_provider.py` 拉取日线（股票 / ETF / 指数分别选择可用源）
3. 清洗标准化后落地 parquet
4. 更新日志到 `data/market/metadata/update_log.parquet`

补充说明：

- ETF 优先走 `fund_etf_hist_em`
- 指数优先走 `index_zh_a_hist`
- 标准化结果统一保留 `volume` / `amount` 字段

### 5.2 回测流

1. UI 提交参数
2. `models.py` 构建并校验 `AnalysisParams`
3. `analyzer.py -> rules.py` 执行信号筛选和交易模拟
4. 输出明细、日统计、净值、扫描结果并可导出

补充说明：

- 当前执行周期默认为 `1d`
- `timeframe` 已预留 `1d / 30m / 15m` 插座，并已贯通到本地加载路径
- 现阶段 `30m / 15m` 仍作为保留能力，参数校验会明确提示“后续扩展”

---

## 6. 快速开始

### 6.1 安装依赖

```bash
pip install -r requirements.txt
```

### 6.2 启动 Web

```bash
streamlit run app.py
```

### 6.3 命令行更新数据（可选）

```bash
python scripts/update_data.py --start-date 2024-01-01 --end-date 2024-12-31 --adjust qfq
```

常用参数：

- `--symbols 000001.SZ,600519.SH`
- `--refresh-symbols`
- `--export-excel`

---

## 7. 结果输出说明

默认输出：

1. 交易明细
2. 按开仓日汇总
3. 净值曲线

其中明细包含 fill 级别信息、以及新入场元数据：

- `entry_factor`
- `entry_trigger_price`
- `entry_fill_type`

扫描统计中额外包含跳过原因计数：

- `skipped_entry_not_filled`
- `skipped_locked_bar_unfillable`

---

## 8. 测试

运行全部测试：

```bash
pytest -q
```

如需仅验证 Streamlit 展示层的表格数字格式，可运行：

```bash
python -m unittest tests.test_app_display_formatters_unittest -v
```

本项目重点覆盖：

- 入场因子与信号语义（含默认 gap 兼容）
- 突破类 stop-entry 成交与不可成交过滤
- 连续K线追势 / 加速追势的 long/short 信号与策略级统计
- 分批退出、总利润回撤、时间退出
- 参数扫描边界与导出
- Streamlit 展示表的数字格式回归
- Streamlit 工作台中的新策略选项 / 周期插座 UI 回归

---

## 9. 额外文档

- `docs/ux_correction_plan.md`：当前工作台交互减负修正方案

---

## 10. 已知边界与使用建议

- 当前为**日线研究工具**，不是逐笔撮合回测器。
- `30m / 15m` 当前仅为预留插座，尚未开放完整多周期回测执行。
- short 方向仅用于研究镜像，不代表可直接实盘融券执行。
- 一字板/锁死成交等仅能在日线层做保守近似过滤。
- 建议优先做“粗扫→细扫”，避免一次性多轴爆炸。

---

## 11. 开发原则

- 下载层与回测层解耦
- 策略语义优先稳定（先保证可复现，再扩展）
- 尽量通过参数扩展，不做无必要重写

---

如需新增因子，建议沿用当前 contract：

1. 触发价来源必须可证明不看未来
2. 明确 stop-entry 成交规则
3. 明确不可成交规则
4. 加入因子感知扫描白名单
5. 先补测试再放 UI
