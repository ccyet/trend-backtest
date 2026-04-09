# Gap_test（A 股研究型回测工作台）

一个基于 **Streamlit + Pandas** 的 A 股研究型回测工作台。  
项目定位是：**离线可复现、语义明确、方便做参数研究**，而不是高频撮合级仿真。

---

## 0. 当前能力一页读懂

- 支持 **本地 Parquet / SQLite / Excel/CSV** 作为回测输入
- 支持 **TDX 量化能力接入**：历史 K 线更新、通达信公式导入本地指标 parquet
- 当前共有 6 类入场因子：
  - `gap`
  - `trend_breakout`
  - `volatility_contraction_breakout`
  - `candle_run`
  - `candle_run_acceleration`
  - `early_surge_high_base`
- 导入指标当前可用于：
  - 开仓过滤
  - 整笔离场
  - 分批止盈（导入指标阈值止盈）
- `early_surge_high_base` 已跑通 **30m 形态 + 5m 执行**
- `1m / 5m / 15m / 30m / 1d` 已支持离线更新；但**通用多周期策略工作台尚未全部开放**

---

## 1. 目前支持的核心能力

- **离线行情更新**（支持按周期选择 AKShare / TDX 数据源）
- **通达信量化（TdxQuant）能力接入**
  - 支持通过本机通达信量化终端获取历史 **日线 / 1m / 5m** K 线数据
  - 支持调用通达信公式并将结果落地为本地 parquet 指标文件
- **多数据源回测输入**：本地 Parquet / SQLite / 上传 Excel/CSV
- **单账户单持仓回测框架**（研究型 long/short 镜像）
- **六类入场因子**
  - `gap`（跳空）
  - `trend_breakout`（趋势突破）
  - `volatility_contraction_breakout`（波动收缩突破）
  - `candle_run`（连续K线追势）
  - `candle_run_acceleration`（连续K线加速追势）
  - `early_surge_high_base`（早盘冲高高位横盘突破）
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

### 1.1.1 指标怎么用

导入指标当前走这条最短路径：

1. 用脚本或工作台中的“离线指标管理”导入指标数据
2. 指标会落地到本地 parquet，并进入 registry / availability
3. 在规则区按能力使用：
   - `allow_filter=True`：可用于开仓过滤
   - `allow_exit=True`：可用于整笔离场
   - 已导入指标也可用于 **分批止盈中的导入指标阈值止盈**

工作台里最常见的指标用法：

- 开仓过滤：指标 + 输出列 + 比较方向 + 阈值
- 整笔离场：指标 + 输出列 + 比较方向 + 阈值
- 分批止盈：在某一批退出方式中选择“导入指标阈值止盈”

说明：

- 指标能否在 UI 中出现，取决于 registry 的能力位（`allow_filter` / `allow_exit`）
- 不同周期下，指标是否真正生效还取决于对应时间粒度数据是否已导入并成功对齐

---

## 2. 关键执行语义（务必先看）

### 2.1 严禁未来函数（No Lookahead）

所有入场触发依据都使用 **T-1 及更早**数据构建；T 日只用于判定是否触发成交。

### 2.2 六类入场因子

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

#### F) `early_surge_high_base`

- 当前是仓库里唯一已跑通的“小级别介入”链路
- 语义为：**30m 形态筛选 + 5m 执行触发**
- 当前边界：
  - 仅支持 `timeframe=30m`
  - 仅支持 `local_parquet`
  - 仅支持 `gap_direction=up`

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

`early_surge_high_base` 当前使用：

- 30m 上生成 setup 和 `entry_trigger_price`
- 5m 上等待突破触发
- 触发后按执行链路继续成交与退出

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
app.py            # Streamlit 界面、参数组装、结果展示
models.py         # 参数模型、扫描配置、参数校验
rules.py          # 入场筛选、成交模拟、退出语义
analyzer.py       # 候选交易、策略交易、净值/扫描汇总、run_backtest 结果层 API
exporter.py       # Excel 导出

scripts/update_data.py
data/providers/akshare_provider.py
data/services/local_data_service.py
```

---

## 5. 数据流

### 5.1 离线更新流

1. UI 或 CLI 调用 `scripts/update_data.py`
2. 按周期解析更新源（`AKShare` / `TDX`）
3. 对应 provider 拉取历史 K 线并清洗标准化后落地 parquet
4. 更新日志到 `data/market/metadata/update_log.parquet`

补充说明：

- ETF 优先走 `fund_etf_hist_em`
- 指数优先走 `index_zh_a_hist`
- 当前默认配置为：`1d/30m/15m -> AKShare`，`5m/1m -> TDX`
- 当前更新链路已支持 `1d / 30m / 15m / 5m / 1m` 按周期切换 `AKShare / TDX` 数据源；默认值仍保持现有较稳妥配置
- 标准化结果统一保留 `volume` / `amount` 字段

### 5.1.1 通达信量化（TdxQuant）补充说明

- 当前仓库已接入 **通达信量化平台** 本地能力，重点用于：
  1. 获取历史 **日线 / 1m / 5m** K 线数据
  2. 调用通达信公式，生成本地指标 parquet
- 运行前提：本机需安装并登录支持 TQ 功能的通达信终端，并可通过 `TDX_TQCENTER_PATH` 暴露 `PYPlugins/user`
- `scripts/import_tdx_local_indicators.py` 当前内置 `board_ma` 指标，也支持手动指定：
  - `--formula-name`
  - `--output-map`
- 当前未宣称仓库已具备“通达信公式自动枚举”能力；未命中内置指标时，按手动公式名称与输出映射导入

### 5.2 回测流

1. UI 提交参数
2. `models.py` 构建并校验 `AnalysisParams`
3. `analyzer.run_backtest(...)` 统一调度组合回测 / 逐股回测 / 参数扫描
4. `analyzer.py -> rules.py` 执行信号筛选和交易模拟，并组装结果层 bundle
5. UI 输出明细、日统计、净值、扫描结果并可导出

补充说明：

- 当前默认执行周期仍是 `1d`
- `early_surge_high_base` 是一个例外：使用 `30m` 做 setup，并自动加载 `5m` 执行数据
- 导入指标已经接入：开仓过滤、整笔离场、分批止盈
- `run_parameter_scan(...)` 当前会复用共享扫描上下文（按股票分组、必要时一次性预载 ESHB 5m 执行数据），减少重复准备开销

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
- `--timeframe 1d --timeframe 30m`
- `--provider 1d=tdx --provider 5m=akshare`（按周期覆盖默认更新源）

如需通过通达信量化能力更新分钟级数据，可使用：

```bash
set TDX_TQCENTER_PATH=C:\path\to\TdxInstall\PYPlugins\user
python scripts/update_data.py --symbols 000001.SZ --start-date 2024-01-01 --end-date 2024-01-31 --adjust qfq --timeframe 1m --timeframe 5m
```

如需按周期混合使用 AKShare / TDX，可使用：

```bash
set TDX_TQCENTER_PATH=C:\path\to\TdxInstall\PYPlugins\user
python scripts/update_data.py --symbols 000001.SZ --start-date 2024-01-01 --end-date 2024-01-31 --adjust qfq --timeframe 1d --timeframe 5m --provider 1d=akshare --provider 5m=tdx
```

`30m / 15m` 也可使用同样的 `--provider timeframe=source` 方式切换更新源。

如需调用通达信公式并导入本地指标，可使用：

```bash
set TDX_TQCENTER_PATH=C:\path\to\TdxInstall\PYPlugins\user
python scripts/import_tdx_local_indicators.py --indicator board_ma --symbols 000001.SZ --start-date 2024-01-01 --end-date 2024-01-31 --adjust qfq
```

手动公式模式示例：

```bash
set TDX_TQCENTER_PATH=C:\path\to\TdxInstall\PYPlugins\user
python scripts/import_tdx_local_indicators.py --indicator board_ma --symbols 000001.SZ --start-date 2024-01-01 --end-date 2024-01-31 --adjust qfq --formula-name 板块均线 --output-map board_ma_ratio_20=NOTEXT1,board_ma_ratio_50=NOTEXT2
```

导入完成后，指标会进入工作台规则区：

1. 开仓过滤：用于筛掉不满足阈值的候选信号
2. 整笔离场：对剩余仓位做整笔退出
3. 分批止盈：在某一批退出方式中选择“导入指标阈值止盈”

使用建议：

- 先确认指标已导入成功，并在 registry / availability 中可见
- 再配置指标、输出列、比较方向与阈值
- 如果规则不触发，优先检查该指标是否与当前回测时间粒度匹配

---

## 7. 结果输出说明

默认输出：

1. 交易明细
2. 按开仓日汇总
3. 净值曲线

其中明细包含 fill 级别信息，以及入场/离场解释字段：

- `entry_factor`
- `entry_trigger_price`
- `entry_fill_type`
- `entry_reason`
- `exit_reason`
- `partial_indicator_rule_label`
- `partial_indicator_trigger_value`

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
- 混合更新源与按周期 provider 路由
- 参数扫描共享上下文复用
- Streamlit 展示表的数字格式回归
- Streamlit 工作台中的新策略选项 / 周期插座 UI 回归

---

## 9. 额外文档

- `docs/ux_correction_plan.md`：当前工作台交互减负修正方案
- `docs/price_morphology_research_framework.md`：日线 / 30m 价格形态数学化研究提纲、字段表、因子表与标签设计

---

## 10. 已知边界与使用建议

- 当前为**日线研究工具**，不是逐笔撮合回测器。
- `30m / 15m` 不是通用多周期回测模板；当前真正跑通的小级别链路是 `early_surge_high_base` 的 `30m setup + 5m execution`。
- `1m / 5m` 当前主要用于离线数据更新、通达信量化接入与局部执行链路；并不代表通用分钟级策略工作台已全部开放。
- short 方向仅用于研究镜像，不代表可直接实盘融券执行。
- 一字板/锁死成交等仅能在日线层做保守近似过滤。
- 建议优先做“粗扫→细扫”，避免一次性多轴爆炸。
- 导入指标跨周期使用时，是否真正生效取决于该时间粒度的数据是否已导入并成功对齐；不匹配时规则可能不触发。

### 10.1 退出能力边界

- 启用分批止盈时：分批规则优先按批次和优先级执行
- 分批执行后，整笔导入指标离场 / 板块均线离场仍可对剩余仓位生效
- 旧版整笔退出主要用于未启用分批的场景

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
