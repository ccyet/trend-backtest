# Al Brooks 价格行为三类入场体系与 trend-backtest 实现映射

## 资料依据

- Brooks 官方缩写表：H1/H2/L1/L2、FBO、MTR、TR、EMA、BSB/SSB 等定义。
  <https://www.brookstradingcourse.com/brooks-price-action-abbreviations/>
- Brooks 官方“10 best price action trading patterns”：H2/L2、交易区间反转、主要趋势反转属于核心价格行为形态。
  <https://www.brookstradingcourse.com/price-action/10-best-price-action-trading-patterns/>
- Brooks 课程索引与 Ask Al：用于确认趋势、突破、跟随、失败突破与 Always In 语境。
  <https://www.brookstradingcourse.com/trade-price-action/>
  <https://www.brookstradingcourse.com/ask-al/breakouts/>

本仓库做的是 A 股日线研究型回测，不复刻 Brooks 的逐笔盘口语境。实现目标是把三类价格行为转成可测试、可参数扫描、无未来函数的日线规则。

## 策略名称与内部值

| 用户可见策略名 | 内部 entry_factor | 方向 |
|---|---|---|
| Brooks 趋势回撤 H2/L2 | `brooks_trend_pullback` | 牛趋势 H2 回撤买入 / 熊趋势 L2 回撤卖出 |
| Brooks 交易区间失败突破 | `brooks_trading_range_reversal` | 区间下沿失败跌破买入 / 区间上沿失败突破卖出 |
| Brooks 主要趋势反转 | `brooks_major_trend_reversal` | 熊转牛主要反转 / 牛转熊主要反转 |

内部英文值不改，保证旧配置、历史导出和参数扫描兼容；UI、README、结果摘要和 Notion 文档使用中文策略名。

## 通用记号与基础公式

以下公式使用 `t` 表示当前执行 K 线，`t-1` 表示信号棒，`i` 表示历史 K 线索引。所有 setup 只使用 `t-1` 及更早的数据；`t` 只用于判断 stop-entry 是否触发。

参数记号：

- `L`：对应策略的 lookback 参数。
- `M`：对应策略的均线周期。
- `N`：趋势回撤要求的最少逆势 K 数。
- `D`：趋势回撤允许的最大回撤深度百分比。
- `B`：交易区间失败突破缓冲，`B = brooks_range_break_buffer_pct / 100`。
- `W`：交易区间最小宽度，`W = brooks_range_min_width_pct`。
- `R`：主要趋势反转回测缓冲，`R = brooks_mtr_retest_buffer_pct / 100`。

均线与信号棒质量：

```text
ma_t = mean(close_{t-M+1} ... close_t)

close_pos_t = (close_t - low_t) / (high_t - low_t)

bull_signal_t =
    close_t > open_t
    and close_pos_t >= 0.6

bear_signal_t =
    close_t < open_t
    and close_pos_t <= 0.4
```

如果 `high_t - low_t <= EPSILON`，`close_pos_t` 视为无效，牛/熊信号棒均不成立。

通用 stop-entry 成交：

```text
long_trigger_t = high_{t-1}
long_trigger_pass_t = high_t >= long_trigger_t

short_trigger_t = low_{t-1}
short_trigger_pass_t = low_t <= short_trigger_t
```

long 成交价：

```text
if open_t >= long_trigger_t:
    fill_price_t = open_t
elif open_t < long_trigger_t <= high_t:
    fill_price_t = long_trigger_t
else:
    entry_not_filled
```

short 成交价：

```text
if open_t <= short_trigger_t:
    fill_price_t = open_t
elif open_t > short_trigger_t >= low_t:
    fill_price_t = short_trigger_t
else:
    entry_not_filled
```

## Brooks 趋势回撤 H2/L2

### 交易思想

Brooks 的 H2/L2 不是“任意两根逆势 K 线”。本仓库将它拆成四个机械条件：

1. 趋势背景成立。
2. 回撤窗口里有足够逆势 K 线。
3. 回撤后出现两次顺趋势尝试。
4. `t-1` 是合格顺趋势信号棒，`t` 突破信号棒高/低点。

### 牛趋势 H2 回撤买入

参数：

```text
L = brooks_pullback_lookback
M = brooks_pullback_ma_period
N = brooks_pullback_min_countertrend_bars
D = brooks_pullback_max_depth_pct
```

趋势背景：

```text
trend_context_t =
    close_{t-L-1} > ma_{t-L-1}
    and ma_{t-1} > ma_{t-L-1}
```

逆势回撤计数：

```text
countertrend_i = close_i < open_i
countertrend_count_t = sum(countertrend_i, i=t-L..t-1)
```

H2 尝试计数：

```text
long_attempt_i =
    bull_signal_i
    and high_i > high_{i-1}

attempt_count_t = sum(long_attempt_i, i=t-L..t-1)
```

回撤深度：

```text
pullback_high_t = max(high_i, i=t-L..t-1)
pullback_low_t = min(low_i, i=t-L..t-1)
depth_pct_t = (pullback_high_t / pullback_low_t - 1) * 100
```

信号棒与触发：

```text
signal_bar_quality_t =
    bull_signal_{t-1}
    and high_{t-1} > high_{t-2}

trigger_t = high_{t-1}
trigger_pass_t = high_t >= trigger_t
```

最终信号：

```text
brooks_h2_long_t =
    trend_context_t
    and signal_bar_quality_t
    and attempt_count_t >= N
    and countertrend_count_t >= N
    and depth_pct_t <= D
    and trigger_t is not null
    and trigger_pass_t
```

### 熊趋势 L2 回撤卖出

趋势背景：

```text
trend_context_t =
    close_{t-L-1} < ma_{t-L-1}
    and ma_{t-1} < ma_{t-L-1}
```

逆势回撤计数：

```text
countertrend_i = close_i > open_i
countertrend_count_t = sum(countertrend_i, i=t-L..t-1)
```

L2 尝试计数：

```text
short_attempt_i =
    bear_signal_i
    and low_i < low_{i-1}

attempt_count_t = sum(short_attempt_i, i=t-L..t-1)
```

回撤深度：

```text
pullback_high_t = max(high_i, i=t-L..t-1)
pullback_low_t = min(low_i, i=t-L..t-1)
depth_pct_t = (pullback_high_t / pullback_low_t - 1) * 100
```

信号棒与触发：

```text
signal_bar_quality_t =
    bear_signal_{t-1}
    and low_{t-1} < low_{t-2}

trigger_t = low_{t-1}
trigger_pass_t = low_t <= trigger_t
```

最终信号：

```text
brooks_l2_short_t =
    trend_context_t
    and signal_bar_quality_t
    and attempt_count_t >= N
    and countertrend_count_t >= N
    and depth_pct_t <= D
    and trigger_t is not null
    and trigger_pass_t
```

## Brooks 交易区间失败突破

### 交易思想

失败突破只在交易区间极端有效。仓库实现先用 `t-2` 及更早的 K 线确认区间，再检查 `t-1` 是否突破区间边界后收回区间内，最后用 `t` 判断是否突破信号棒高/低点。

参数：

```text
L = brooks_range_lookback
B = brooks_range_break_buffer_pct / 100
W = brooks_range_min_width_pct
```

区间定义：

```text
range_high_t = max(high_i, i=t-L-1..t-2)
range_low_t = min(low_i, i=t-L-1..t-2)
range_width_pct_t = (range_high_t / range_low_t - 1) * 100

range_ready_t =
    range_high_t is not null
    and range_low_t is not null
    and range_width_pct_t >= W
```

### 区间下沿失败跌破买入

失败突破：

```text
failed_downside_breakout_t =
    low_{t-1} <= range_low_t * (1 - B)
    and close_{t-1} > range_low_t
```

信号棒与触发：

```text
signal_bar_quality_t = bull_signal_{t-1}
trigger_t = high_{t-1}
trigger_pass_t = high_t >= trigger_t
```

最终信号：

```text
brooks_range_long_t =
    range_ready_t
    and failed_downside_breakout_t
    and signal_bar_quality_t
    and trigger_t is not null
    and trigger_pass_t
```

### 区间上沿失败突破卖出

失败突破：

```text
failed_upside_breakout_t =
    high_{t-1} >= range_high_t * (1 + B)
    and close_{t-1} < range_high_t
```

信号棒与触发：

```text
signal_bar_quality_t = bear_signal_{t-1}
trigger_t = low_{t-1}
trigger_pass_t = low_t <= trigger_t
```

最终信号：

```text
brooks_range_short_t =
    range_ready_t
    and failed_upside_breakout_t
    and signal_bar_quality_t
    and trigger_t is not null
    and trigger_pass_t
```

## Brooks 主要趋势反转

### 交易思想

主要趋势反转不是单根反转 K 线，而是“旧趋势成立 -> 破坏旧趋势结构 -> 回测旧极端失败 -> 反向信号棒触发”。仓库实现使用均线方向表示旧趋势，用收盘穿越均线近似通道破坏，用旧极端点附近回测失败表示 MTR 的二次确认。

参数：

```text
L = brooks_mtr_lookback
M = brooks_mtr_ma_period
R = brooks_mtr_retest_buffer_pct / 100
```

### 熊转牛主要趋势反转

旧趋势：

```text
old_bear_trend_t = ma_{t-2} < ma_{t-L}
```

旧低点：

```text
prior_low_t = min(low_i, i=t-L-1..t-2)
```

破坏旧趋势结构：

```text
trendline_break_t = any(close_i > ma_i, i=t-L-1..t-2)
```

回测旧低点失败：

```text
retest_failed_t = low_{t-1} <= prior_low_t * (1 + R)
```

信号棒与触发：

```text
signal_bar_quality_t = bull_signal_{t-1}
trigger_t = high_{t-1}
trigger_pass_t = high_t >= trigger_t
```

最终信号：

```text
brooks_mtr_long_t =
    prior_low_t is not null
    and old_bear_trend_t
    and trendline_break_t
    and retest_failed_t
    and signal_bar_quality_t
    and trigger_t is not null
    and trigger_pass_t
```

### 牛转熊主要趋势反转

旧趋势：

```text
old_bull_trend_t = ma_{t-2} > ma_{t-L}
```

旧高点：

```text
prior_high_t = max(high_i, i=t-L-1..t-2)
```

破坏旧趋势结构：

```text
trendline_break_t = any(close_i < ma_i, i=t-L-1..t-2)
```

回测旧高点失败：

```text
retest_failed_t = high_{t-1} >= prior_high_t * (1 - R)
```

信号棒与触发：

```text
signal_bar_quality_t = bear_signal_{t-1}
trigger_t = low_{t-1}
trigger_pass_t = low_t <= trigger_t
```

最终信号：

```text
brooks_mtr_short_t =
    prior_high_t is not null
    and old_bull_trend_t
    and trendline_break_t
    and retest_failed_t
    and signal_bar_quality_t
    and trigger_t is not null
    and trigger_pass_t
```

## 与代码字段的对应关系

| 文档变量 | 代码来源 |
|---|---|
| `close_pos_t` | `_close_position_in_bar(...)` |
| `bull_signal_t` | `_bull_signal_bar_mask(...)` |
| `bear_signal_t` | `_bear_signal_bar_mask(...)` |
| `attempt_count_t` | `brooks_pullback_attempt_count` |
| `signal_bar_quality_t` | `brooks_signal_bar_quality` |
| `trigger_t` | `entry_trigger_price` |

## A 股日线适配边界

- 当前仅开放日线回测，不扩展到分钟级。
- setup 使用 `t-1` 及更早数据，`t` 只判断是否触发成交。
- Brooks 原体系依赖上下文判断，本仓库用均线、区间、信号棒质量和 stop-entry 做机械化近似。
- 回测结果用于比较策略假设，不应解释为 Brooks 主观读盘体系的完整复制。

## 验证合同

- H2/L2：普通两根逆势 K 线不触发；两段回撤 + 合格信号棒 + stop-entry 才触发。
- 交易区间：弱失败突破信号棒不触发；区间极端失败突破且反向信号棒合格才触发。
- MTR：缺少旧趋势、破通道、回测失败或强信号棒任一条件都不触发。
- 中文展示：用户界面和文档使用中文策略名，内部枚举保持英文兼容。
