from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyCapability:
    supported_timeframes: tuple[str, ...]
    summary: str
    execution_hint: str | None = None


STRATEGY_CAPABILITIES: dict[str, StrategyCapability] = {
    "gap": StrategyCapability(
        supported_timeframes=("1d",),
        summary="跳空策略当前仅开放日线回测。",
    ),
    "trend_breakout": StrategyCapability(
        supported_timeframes=("1d",),
        summary="趋势突破当前仅开放日线回测。",
    ),
    "volatility_contraction_breakout": StrategyCapability(
        supported_timeframes=("1d",),
        summary="波动收缩突破当前仅开放日线回测。",
    ),
    "candle_run": StrategyCapability(
        supported_timeframes=("1d",),
        summary="连续K线追势当前仅开放日线回测。",
    ),
    "candle_run_acceleration": StrategyCapability(
        supported_timeframes=("1d",),
        summary="连续K线加速追势当前仅开放日线回测。",
    ),
    "early_surge_high_base": StrategyCapability(
        supported_timeframes=("30m",),
        summary="早盘冲高高位横盘突破仅支持 30m 形态识别。",
        execution_hint="系统会自动挂接 5m 执行链路。",
    ),
}


def get_supported_strategy_timeframes(entry_factor: str) -> tuple[str, ...]:
    capability = STRATEGY_CAPABILITIES.get(entry_factor)
    if capability is None:
        return ("1d",)
    return capability.supported_timeframes


def get_strategy_capability_summary(entry_factor: str) -> str:
    capability = STRATEGY_CAPABILITIES.get(entry_factor)
    if capability is None:
        return ""
    if capability.execution_hint:
        return f"{capability.summary}{capability.execution_hint}"
    return capability.summary
