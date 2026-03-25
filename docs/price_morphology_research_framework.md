# 价格形态数学化研究提纲（1d / 30m）

## 1. 文档目标

本文档用于把“形态学”压缩成适合本仓库的研究问题。

目标不是做盘口级微观结构建模，而是在 **日线为主、30m 为辅** 的数据条件下，把常见形态拆成：

1. 可验证的数学问题
2. 可落地的字段与因子表
3. 可回测的标签体系
4. 可逐步接线到现有 `entry_factor` 体系的研究路线

本文遵循仓库当前边界：

- 当前正式研究主周期仍是 `1d`
- `30m` 适合作为后续确认层，不假设已完成完整多周期执行
- 严禁未来函数，所有定义必须能用 `T-1` 及更早数据构造
- 重点是 **离线可复现、语义明确、方便做参数研究**

---

## 2. 核心观点

对本仓库而言，形态不应先定义成几何图案，而应先定义成 **中尺度状态切换**：

> 价格形态 = 趋势、波动、位置、量能、结构在一段时间内共同作用后，表现为“突破 / 延续 / 失败 / 反转”的轨迹。

因此：

- 日线负责定义背景结构
- 30m 负责定义触发与确认
- 回测负责验证该结构是否真的改变未来收益分布

这比直接研究“杯柄 / 旗形 / 三角形”的命名更稳定，也更容易接到本仓库已有的：

- `trend_breakout`
- `volatility_contraction_breakout`
- `candle_run`
- `candle_run_acceleration`

---

## 3. 概念框架图

```text
日线结构背景
（趋势方向、前高前低、箱体、压缩区、关键位置）
   ↓
30m 触发确认
（放量突破、回踩不破、收盘强弱、局部加速）
   ↓
事件定义
（突破 / 假突破 / 延续 / 失败回归 / 反转）
   ↓
未来收益分布
（horizon return / MFE / MAE / holding outcome）
   ↓
策略动作
（追入 / 回踩确认 / 过滤假突破 / 条件退出）
```

换句话说，仓库中的“形态学研究”可以统一成一句话：

> 先用日线定义结构，再用 30m 定义触发，最后用收益分布验证该事件是否具有统计优势。

---

## 4. 研究问题分层

### 4.1 第一层：趋势推进问题

研究目标：判断当前价格推进是否具有延续性。

数学形式：

\[
P(r_{t,t+h} > 0 \mid Trend, Volatility, Volume, Position)
\]

关注问题：

- 当前上涨/下跌是有效推进还是噪声摆动
- 趋势是在加速、减速还是衰竭
- 创新高/新低后，未来收益分布是否改善

适合仓库语义：

- `trend_breakout`
- `candle_run`
- `candle_run_acceleration`

### 4.2 第二层：关口失效问题

研究目标：判断某个关键关口在未来窗口内是否会被有效突破。

这里的“关口”指：

- 前高 / 前低
- 箱体边界
- 密集成交区边缘
- 通道边界
- 整数位

数学形式：

\[
P(Breakout_{t,h}(p^*) = 1 \mid Position, Compression, Trend, Volume)
\]

核心解释：

> 关口突破不是单笔大单推动，而是价格在一段时间内对阻力区的持续消化，最终让阻力失效。

适合仓库语义：

- `trend_breakout`
- `volatility_contraction_breakout`
- 后续可扩展的 `structure_breakout`

### 4.3 第三层：真假突破问题

研究目标：突破发生后，它会延续还是回落。

数学形式：

\[
P(Continuation \mid Breakout, Trend, Volume, PostBreakStrength)
\]

关注问题：

- 突破后是否快速跌回区间内
- 突破后是否继续创新高 / 新低
- 突破后的回踩是否明显变浅
- 日线背景与 30m 确认是否一致

### 4.4 第四层：状态切换问题

研究目标：把传统“形态识别”降维成状态转移问题。

可先使用简单状态集：

- `range`：震荡
- `compression`：波动压缩
- `breakout_setup`：临界待突破
- `expansion`：突破扩张
- `failure_or_revert`：失败回归

数学形式：

\[
P(S_{t+h} = j \mid S_t = i, X_t)
\]

这里的意义在于：

> 很多“形态”只是状态切换在价格上的投影，不必先绑定到某个图形名称。

---

## 5. 变量定义表（研究字段）

以下字段按 **日线背景层 / 30m 触发层 / 结构层 / 标签层** 拆分，尽量贴近本仓库现有命名风格。

### 5.1 日线背景字段

| 字段名 | 含义 | 示例定义 |
|---|---|---|
| `trend_slope_20` | 20 日价格回归斜率 | 对最近 20 根收盘做线性回归斜率 |
| `trend_efficiency_20` | 20 日趋势效率 | `abs(close-close_20d_ago) / sum(abs(diff(close)))` |
| `atr_ratio_14` | 波动标准化比例 | `ATR(14) / close` |
| `compression_ratio_20` | 当前压缩程度 | 当前 20 日波幅 / 历史均值波幅 |
| `range_position_20` | 区间相对位置 | `(close-LL20) / (HH20-LL20)` |
| `distance_to_20d_high_atr` | 距离 20 日高点 | `(HH20 - close) / ATR(14)` |
| `distance_to_60d_high_atr` | 距离 60 日高点 | `(HH60 - close) / ATR(14)` |
| `hh_count_20` | 20 日内创新高次数 | 滚动统计 |
| `hl_count_20` | 20 日内高低点抬升次数 | 基于 swing low 或简化高低点规则 |
| `relative_volume_20` | 相对成交量 | `volume / ma(volume,20)` |

### 5.2 30m 触发字段

| 字段名 | 含义 | 示例定义 |
|---|---|---|
| `breakout_above_prev_n_high` | 是否突破前序局部高点 | `close > rolling_max(high.shift(1), n)` |
| `breakout_distance_atr_30m` | 突破距离标准化 | `(close - trigger_price) / ATR_30m` |
| `intraday_rel_volume` | 日内相对量能 | `volume / ma(volume,n)` |
| `intraday_range_expansion` | 日内波幅扩张 | 当前 bar 波幅 / 历史均值波幅 |
| `retest_holds_flag` | 回踩是否守住 | 回踩 trigger 后未明显跌破 |
| `close_near_high_flag` | 收盘是否接近日内高位 | `(high-close)/(high-low+eps)` 足够小 |
| `post_break_pullback_pct` | 突破后回撤幅度 | 相对 trigger 或突破 bar 实体计算 |

### 5.3 结构字段

| 字段名 | 含义 | 示例定义 |
|---|---|---|
| `box_width_ratio` | 箱体宽度占比 | `(HH_n - LL_n) / close` |
| `pullback_depth_mean` | 平均回撤深度 | 最近几次回撤幅度均值 |
| `pullback_depth_change` | 回撤深度变化 | 最近回撤相对前次是变浅还是变深 |
| `swing_high_slope` | 局部高点斜率 | 基于 swing high 序列回归 |
| `swing_low_slope` | 局部低点斜率 | 基于 swing low 序列回归 |
| `base_length` | 整理平台长度 | 平台区间持续 bar 数 |
| `base_tightness` | 平台紧致度 | 平台宽度 / ATR 或平台宽度 / 均价 |

### 5.4 派生研究因子

| 因子名 | 解释 | 示例用途 |
|---|---|---|
| `trend_pressure_score` | 趋势推进强度 | 过滤“弱突破” |
| `compression_score` | 压缩蓄势强度 | 寻找待扩张结构 |
| `structure_break_score` | 结构失效强度 | 预测关口突破概率 |
| `retest_quality_score` | 回踩质量 | 区分真突破与假突破 |
| `continuation_score` | 突破后延续概率 | 持仓或加仓参考 |

---

## 6. 字段表 / 因子表 / 标签表（repo-native 草案）

本节用于回答“后续如果在本仓库继续扩展，字段该怎么命名、按什么层级组织”。

### 6.1 输入字段表（基础行情）

沿用仓库当前 canonical schema：

| 字段名 | 说明 |
|---|---|
| `date` | 交易日期 / bar 时间 |
| `stock_code` | 标的代码 |
| `open` | 开盘价 |
| `high` | 最高价 |
| `low` | 最低价 |
| `close` | 收盘价 |
| `volume` | 成交量 |
| `amount` | 成交额（如可用） |
| `timeframe` | `1d` / `30m` |

### 6.2 日线特征表（建议）

| 字段名 | 类型 | 说明 |
|---|---|---|
| `trend_slope_20` | float | 20 日斜率 |
| `trend_efficiency_20` | float | 20 日趋势效率 |
| `atr_ratio_14` | float | ATR 标准化波动 |
| `compression_ratio_20` | float | 波动压缩程度 |
| `range_position_20` | float | 区间位置 |
| `distance_to_20d_high_atr` | float | 距 20 日高点的 ATR 距离 |
| `distance_to_60d_high_atr` | float | 距 60 日高点的 ATR 距离 |
| `relative_volume_20` | float | 相对量能 |
| `box_width_ratio_20` | float | 20 日箱体宽度占比 |
| `base_tightness_20` | float | 20 日平台紧致度 |

### 6.3 30m 确认特征表（建议）

| 字段名 | 类型 | 说明 |
|---|---|---|
| `breakout_trigger_price` | float | 当期触发价 |
| `breakout_distance_atr_30m` | float | 穿越 trigger 的强度 |
| `intraday_rel_volume_20` | float | 日内相对量能 |
| `intraday_range_expansion_20` | float | 日内波动扩张 |
| `retest_holds_flag` | bool | 回踩是否守住 |
| `close_near_high_flag` | bool | 收盘是否强势 |
| `post_break_pullback_pct` | float | 突破后回撤幅度 |

### 6.4 因子表（建议的研究家族）

| entry_factor 候选 | 问题定义 | 与现有因子关系 |
|---|---|---|
| `structure_breakout` | 平台/箱体/前高突破 | 可视作 `trend_breakout` 的结构化扩展 |
| `compression_breakout` | 压缩后突破 | 与 `volatility_contraction_breakout` 一脉相承 |
| `retest_breakout` | 突破后回踩确认再入场 | 可作为 30m 确认层研究方向 |
| `trend_continuation_breakout` | 趋势中继而非底部启动 | 与 `candle_run` / `trend_breakout` 有交集 |

这些名称当前是 **研究词汇**，不代表已接线实现。

### 6.5 标签表（回测研究）

| 标签名 | 类型 | 说明 |
|---|---|---|
| `y_breakout_h5` | bool/int | 未来 5 个 bar 内是否有效突破 |
| `y_breakout_h10` | bool/int | 未来 10 个 bar 内是否有效突破 |
| `y_continuation_h10` | bool/int | 突破后未来 10 个 bar 是否延续 |
| `y_false_break_h5` | bool/int | 突破后未来 5 个 bar 是否快速跌回 |
| `y_return_h5` | float | 未来 5 个 bar 收益 |
| `y_return_h10` | float | 未来 10 个 bar 收益 |
| `y_mfe_h10` | float | 未来 10 个 bar 最大有利波动 |
| `y_mae_h10` | float | 未来 10 个 bar 最大不利波动 |

注：`h5 / h10` 只是占位写法，实际窗口需要按 `1d` 与 `30m` 分别定义。

---

## 7. 回测标签设计

### 7.1 关键原则

1. **先定义关口，再定义突破**
2. **关口必须在时点 t 可知，不能后验画线**
3. **标签要拆层，不要只做一个模糊的“突破成功”标签**
4. **样本点应集中在临近关口的决策时刻，而不是每个 bar 都打点**

### 7.2 关口定义

建议优先使用可机械化的两类：

1. 前高 / 前低
   - `rolling_max(high.shift(1), lookback)`
   - `rolling_min(low.shift(1), lookback)`
2. 箱体边界
   - 最近 `n` 根高低区间上沿 / 下沿

这些定义最贴近仓库现有 `trend_breakout` 语言。

### 7.3 突破标签

设关口为 `p*`，未来窗口为 `h`。

有效向上突破可定义为：

\[
\max(P_{t:t+h}) > p^* + \delta
\]

且满足以下条件之一：

- 窗口末收盘仍在 `p*` 上方
- 突破后未快速跌回 `p*` 下方
- 突破后的最大回撤不超过阈值

### 7.4 延续标签

突破确认后，未来 `h2` 窗口满足：

\[
\max(P_{post}) - P_{confirm} > \eta \cdot ATR
\]

则记为延续成功。

### 7.5 假突破标签

若价格曾上穿 `p* + \delta`，但随后快速跌回 `p*` 下方，或窗口内 `MFE` 很小而 `MAE` 很大，则记为假突破。

### 7.6 三段式标签体系（推荐）

把“突破”拆成三道题：

1. `Y1`：是否击穿关口
2. `Y2`：击穿后是否站稳
3. `Y3`：站稳后是否延续

这比一个单一标签更适合研究因子作用位置。

---

## 8. 建模路线（按复杂度递进）

### 阶段 1：纯日线研究

先不引入 30m，只研究：

- 趋势推进因子
- 波动压缩因子
- 临界位置因子
- 突破 / 延续 / 假突破标签

目标：确认“接近关键关口 + 波动压缩 + 趋势效率高”是否真的提高有效突破概率。

### 阶段 2：日线背景 + 30m 触发

日线负责筛背景：

- 接近前高 / 平台上沿
- 过去窗口内波动收缩
- 趋势未破坏

30m 负责定义触发：

- 放量突破局部高点
- 回踩不破
- 收盘仍强

这一层最适合后续扩展成研究型多周期条件，而不是立刻宣称已支持完整多周期回测。

### 阶段 3：状态模型

若前两阶段稳定，再研究：

- `compression -> breakout_setup`
- `breakout_setup -> expansion`
- `expansion -> continuation`
- `expansion -> failure_or_revert`

开始时可用规则状态机，后续再考虑 HMM 或 Markov switching。

---

## 9. 与本仓库现有体系的对接建议

### 9.1 命名风格

继续沿用：

- `snake_case`
- `*_lookback`
- `*_pct`
- `*_period`
- `*_ratio`

### 9.2 输出风格

若后续进入实现阶段，建议继续沿用：

- `is_signal`
- `entry_trigger_price`
- `entry_factor`
- `entry_reason`

例如：

- `entry_factor="structure_breakout"`
- `entry_reason="structure_breakout.long"`

### 9.3 研究与实现边界

本文档只定义：

- 研究问题
- 变量与标签
- 文档级命名建议

本文档不代表：

- 已修改 `models.py`
- 已新增 `entry_factor`
- 已开放 `30m` 完整执行
- 已加入 UI、导出或测试

---

## 10. 推荐的最小研究顺序

如果只做最小闭环，建议按下面顺序推进：

1. **先做日线关口样本筛选**
   - 只抽取接近前高 / 箱体上沿的样本
2. **再做压缩 + 趋势效率特征**
   - 检验对突破概率的提升
3. **然后拆分突破标签**
   - 击穿 / 站稳 / 延续
4. **最后再加 30m 触发确认**
   - 检查是否明显优于纯日线

对应可优先回答的三道研究题：

1. `compression + near_resistance` 是否提升突破概率
2. `trend_efficiency` 是否提升突破后延续概率
3. `daily_setup + 30m_confirmation` 是否优于单日线触发

---

## 11. 一页结论

本仓库更适合的“形态学数学化”表述是：

> 形态不是盘口微观冲击的直接建模，而是日线/30m 上“趋势、波动、位置、量能、结构”共同作用导致的状态切换问题。

因此最实用的研究框架是：

- **日线看结构**
- **30m 看确认**
- **标签拆成击穿 / 站稳 / 延续**
- **因子围绕趋势推进、压缩蓄势、关口失效、回踩质量组织**

这样既贴近交易直觉，也贴近本仓库当前的研究型回测定位。
