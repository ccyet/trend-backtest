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

## 规则映射

### Brooks 趋势回撤 H2/L2

Brooks 的 H2/L2 不是“任意两根逆势 K 线”。本仓库按以下条件近似：

1. 先确认趋势背景：均线方向与回撤前价格位置支持牛/熊趋势。
2. 回撤窗口内必须有足够逆势 K 线，避免把趋势中普通小停顿当作回撤。
3. 必须出现两次顺趋势尝试：牛趋势用 H2，熊趋势用 L2。
4. 信号棒必须顺趋势收盘，且收盘位置不能太弱。
5. 下一根 K 线突破信号棒高/低点后，才按 stop-entry 成交。

### Brooks 交易区间失败突破

交易区间失败突破只交易区间极端，不交易区间中部噪声：

1. 先用回看窗口确认区间高低点和最小宽度。
2. 信号棒必须突破区间上沿或下沿后收回区间内。
3. 信号棒必须具备反向质量：下沿失败跌破需要牛信号棒，上沿失败突破需要熊信号棒。
4. 下一根突破信号棒高/低点才成交。

### Brooks 主要趋势反转

MTR 不是单根反转 K 线。本仓库按四段结构近似：

1. 旧趋势已经成立。
2. 价格先破坏原趋势的均线/通道结构。
3. 再回测旧极端点失败。
4. 出现反向强信号棒，并在下一根突破信号棒高/低点后成交。

## A 股日线适配边界

- 当前仅开放日线回测，不扩展到分钟级。
- 规则保持无未来函数：setup 使用 T-1 及更早数据，T 日只判断是否触发成交。
- Brooks 原体系依赖上下文判断，本仓库用均线、区间、信号棒质量和 stop-entry 做机械化近似。
- 回测结果用于比较策略假设，不应解释为 Brooks 主观读盘体系的完整复制。

## 验证合同

- H2/L2：普通两根逆势 K 线不触发；两段回撤 + 合格信号棒 + stop-entry 才触发。
- 交易区间：弱失败突破信号棒不触发；区间极端失败突破且反向信号棒合格才触发。
- MTR：缺少旧趋势、破通道、回测失败或强信号棒任一条件都不触发。
- 中文展示：用户界面和文档使用中文策略名，内部枚举保持英文兼容。
