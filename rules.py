from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, cast

import pandas as pd

from models import (
    EPSILON,
    AnalysisParams,
    ImportedIndicatorRule,
    PartialExitRule,
    TradeFill,
)


SIGNAL_COLUMNS = [
    "date",
    "stock_code",
    "prev_close",
    "prev_high",
    "prev_low",
    "open",
    "close",
    "volume",
    "gap_pct_vs_prev_close",
]

BREAKOUT_ENTRY_FACTORS = frozenset(
    {"trend_breakout", "volatility_contraction_breakout"}
)
SEQUENCE_ENTRY_FACTORS = frozenset({"candle_run", "candle_run_acceleration"})
ESHB_ENTRY_FACTOR = "early_surge_high_base"
BOARD_MA_COLUMNS = {"20": "board_ma_ratio_20", "50": "board_ma_ratio_50"}


@dataclass(frozen=True)
class NumericThresholdRule:
    column_name: str
    operator: str
    threshold: float
    value_column_name: str


def _column(stock_df: pd.DataFrame, column_name: str) -> pd.Series:
    return cast(pd.Series, stock_df[column_name])


def _is_missing_scalar(value: object) -> bool:
    return bool(pd.isna(cast(Any, value)))


def _float_scalar(value: object) -> float:
    return float(cast(Any, value))


def _compute_breakout_trigger_price(
    stock_df: pd.DataFrame, params: AnalysisParams
) -> pd.Series:
    if params.entry_factor == "trend_breakout":
        lookback = params.trend_breakout_lookback
    else:
        lookback = params.vcb_breakout_lookback

    if params.gap_direction == "down":
        return pd.Series(
            _column(stock_df, "low").shift(1).rolling(lookback).min(),
            index=stock_df.index,
        )
    return pd.Series(
        _column(stock_df, "high").shift(1).rolling(lookback).max(),
        index=stock_df.index,
    )


def _compute_true_range(stock_df: pd.DataFrame) -> pd.Series:
    high_series = _column(stock_df, "high")
    low_series = _column(stock_df, "low")
    prev_close = _column(stock_df, "close").shift(1)
    tr_components = pd.concat(
        [
            high_series - low_series,
            (high_series - prev_close).abs(),
            (low_series - prev_close).abs(),
        ],
        axis=1,
    )
    return pd.Series(tr_components.max(axis=1), index=stock_df.index)


def _compute_atr_series(stock_df: pd.DataFrame, period: int) -> pd.Series:
    return pd.Series(
        _compute_true_range(stock_df).rolling(period).mean(), index=stock_df.index
    )


def _board_ma_column_name(line: str) -> str:
    return BOARD_MA_COLUMNS.get(str(line), BOARD_MA_COLUMNS["20"])


def _board_ma_series(stock_df: pd.DataFrame, line: str) -> pd.Series:
    column_name = _board_ma_column_name(line)
    if column_name not in stock_df.columns:
        return pd.Series(math.nan, index=stock_df.index, dtype=float)
    return cast(pd.Series, pd.to_numeric(stock_df[column_name], errors="coerce"))


def _compare_board_ma(value: float, operator: str, threshold: float) -> bool:
    if operator == "<=":
        return value <= threshold
    return value >= threshold


def _compare_threshold(value: float, operator: str, threshold: float) -> bool:
    if operator == "<=":
        return value <= threshold
    return value >= threshold


def _apply_numeric_threshold_filter(
    stock_df: pd.DataFrame,
    signal_mask: pd.Series,
    *,
    column_name: str,
    operator: str,
    threshold: float,
    value_column_name: str,
) -> pd.Series:
    if column_name not in stock_df.columns:
        stock_df[value_column_name] = math.nan
        return signal_mask & False
    filter_series = cast(
        pd.Series, pd.to_numeric(stock_df[column_name], errors="coerce")
    )
    stock_df[value_column_name] = filter_series
    return (
        signal_mask
        & filter_series.notna()
        & filter_series.apply(
            lambda value: (
                _compare_threshold(float(value), operator, threshold)
                if pd.notna(value)
                else False
            )
        )
    )


def _resolve_numeric_threshold_exit(
    day_row: pd.Series,
    *,
    column_name: str,
    operator: str,
    threshold: float,
) -> float | None:
    if column_name not in day_row.index:
        return None
    resolved = pd.to_numeric(pd.Series([day_row[column_name]]), errors="coerce").iloc[0]
    if pd.isna(resolved):
        return None
    resolved_value = float(resolved)
    if not _compare_threshold(resolved_value, operator, threshold):
        return None
    return resolved_value


def _resolve_partial_indicator_threshold_exit(
    day_row: pd.Series,
    rule: PartialExitRule,
) -> float | None:
    column_name = str(rule.indicator_column or "").strip()
    operator = str(rule.indicator_operator or ">=").strip() or ">="
    if rule.indicator_threshold is None:
        return None
    threshold = float(rule.indicator_threshold)
    if not column_name:
        return None
    return _resolve_numeric_threshold_exit(
        day_row,
        column_name=column_name,
        operator=operator,
        threshold=threshold,
    )


def _build_board_ma_filter_rule(params: AnalysisParams) -> NumericThresholdRule:
    return NumericThresholdRule(
        column_name=_board_ma_column_name(params.board_ma_filter_line),
        operator=params.board_ma_filter_operator,
        threshold=params.board_ma_filter_threshold,
        value_column_name="board_ma_signal_value",
    )


def _build_imported_indicator_filter_rule(
    params: AnalysisParams,
) -> NumericThresholdRule:
    return NumericThresholdRule(
        column_name=params.imported_indicator_filter_column,
        operator=params.imported_indicator_filter_operator,
        threshold=params.imported_indicator_filter_threshold,
        value_column_name="imported_indicator_filter_value",
    )


def _build_board_ma_exit_rule(params: AnalysisParams) -> NumericThresholdRule:
    return NumericThresholdRule(
        column_name=_board_ma_column_name(params.board_ma_exit_line),
        operator=params.board_ma_exit_operator,
        threshold=params.board_ma_exit_threshold,
        value_column_name="board_ma_value",
    )


def _build_imported_indicator_exit_rule(params: AnalysisParams) -> NumericThresholdRule:
    return NumericThresholdRule(
        column_name=params.imported_indicator_exit_column,
        operator=params.imported_indicator_exit_operator,
        threshold=params.imported_indicator_exit_threshold,
        value_column_name="imported_indicator_exit_value",
    )


def _build_numeric_rule(
    rule: ImportedIndicatorRule, value_column_name: str
) -> NumericThresholdRule:
    return NumericThresholdRule(
        column_name=rule.column,
        operator=rule.operator,
        threshold=rule.threshold,
        value_column_name=value_column_name,
    )


def _build_entry_reason(params: AnalysisParams, entry_factor: str) -> str:
    direction_text = "up" if params.gap_direction == "up" else "down"
    if entry_factor == "gap":
        return f"gap.{params.gap_entry_mode}.{direction_text}"
    return f"{entry_factor}.{direction_text}"


def _build_setup_reason(params: AnalysisParams) -> str:
    if params.entry_factor == "gap":
        return (
            "gap 形态成立：开盘跳空达到阈值"
            if params.gap_entry_mode == "open_vs_prev_close_threshold"
            else "gap 形态成立：开盘相对前高/前低形成严格跳空"
        )
    if params.entry_factor == "trend_breakout":
        return "趋势突破 setup 成立：已生成过去窗口突破触发价"
    if params.entry_factor == "volatility_contraction_breakout":
        return "波动收缩突破 setup 成立：收缩结构和突破触发价均已形成"
    if params.entry_factor == "candle_run":
        return "连续K线追势 setup 成立：前序连续K线组合满足要求"
    if params.entry_factor == "candle_run_acceleration":
        return "连续K线加速追势 setup 成立：前序连续K线且实体强度不递减"
    if params.entry_factor == ESHB_ENTRY_FACTOR:
        return "早盘冲高高位横盘 setup 成立：30m 形态已确认"
    return f"{params.entry_factor} setup 成立"


def _build_trigger_reason(params: AnalysisParams) -> str:
    if params.entry_factor == "gap":
        return "gap 真实触发：当日开盘即为触发点"
    if params.entry_factor == "trend_breakout":
        return "趋势突破真实触发：当根价格突破 trigger 价"
    if params.entry_factor == "volatility_contraction_breakout":
        return "波动收缩突破真实触发：收缩后当根价格突破 trigger 价"
    if params.entry_factor == "candle_run":
        return "连续K线追势真实触发：下一根K线进入执行点"
    if params.entry_factor == "candle_run_acceleration":
        return "连续K线加速追势真实触发：下一根K线进入执行点"
    if params.entry_factor == ESHB_ENTRY_FACTOR:
        return "早盘冲高高位横盘真实触发：5m 突破 base high"
    return f"{params.entry_factor} 真实触发"


def _directional_ma_filter_mask(stock_df: pd.DataFrame, params: AnalysisParams) -> pd.Series:
    mask = stock_df["fast_ma"].notna() & stock_df["slow_ma"].notna()
    if params.gap_direction == "down":
        mask &= stock_df["open"] < stock_df["fast_ma"]
        mask &= stock_df["open"] < stock_df["slow_ma"]
    else:
        mask &= stock_df["open"] > stock_df["fast_ma"]
        mask &= stock_df["open"] > stock_df["slow_ma"]
    return mask.fillna(False)


def _append_reject_reason(
    stock_df: pd.DataFrame, base_mask: pd.Series, pass_mask: pd.Series, reason_label: str
) -> None:
    reject_mask = base_mask & (~pass_mask.fillna(False))
    if not reject_mask.any():
        return
    existing = stock_df.loc[reject_mask, "reject_reason_chain"].fillna("").astype(str)
    stock_df.loc[reject_mask, "reject_reason_chain"] = existing.map(
        lambda text: reason_label if not text else f"{text} -> {reason_label}"
    )


def _build_breakout_trigger_pass_mask(
    stock_df: pd.DataFrame, params: AnalysisParams
) -> pd.Series:
    trigger_price = stock_df["entry_trigger_price"]
    trigger_mask = trigger_price.notna()
    if params.gap_direction == "down":
        trigger_mask &= (
            (_column(stock_df, "open") <= trigger_price)
            | (_column(stock_df, "low") <= trigger_price)
        )
    else:
        trigger_mask &= (
            (_column(stock_df, "open") >= trigger_price)
            | (_column(stock_df, "high") >= trigger_price)
        )
    return trigger_mask.fillna(False)


def _atr_trailing_stop_price(
    reference_price: float,
    atr_value: float | None,
    atr_multiplier: float | None,
    direction: str,
) -> float | None:
    if atr_value is None or atr_multiplier is None or pd.isna(atr_value):
        return None
    if direction == "short":
        return reference_price + float(atr_value) * float(atr_multiplier)
    return reference_price - float(atr_value) * float(atr_multiplier)


def _exit_reason_label(exit_type: str) -> str:
    labels = {
        "stop_loss": "stop_loss: 全仓止损触发",
        "take_profit": "take_profit: 固定止盈触发",
        "profit_drawdown": "profit_drawdown: 分批利润回撤触发",
        "profit_drawdown_exit": "profit_drawdown_exit: 整笔利润回撤触发",
        "board_ma_exit": "board_ma_exit: 板块均线离场触发",
        "imported_indicator_exit": "imported_indicator_exit: 导入指标离场触发",
        "indicator_threshold": "indicator_threshold: 导入指标阈值分批止盈触发",
        "ma_exit": "ma_exit: 均线离场触发",
        "atr_trailing": "atr_trailing: ATR 跟踪止盈触发",
        "time_exit": "time_exit: 时间退出触发",
        "force_close": "force_close: 数据结束强制平仓",
    }
    return labels.get(exit_type, exit_type)


def _compute_candle_run_body_pct(
    stock_df: pd.DataFrame, direction: str
) -> tuple[pd.Series, pd.Series]:
    open_series = _column(stock_df, "open")
    close_series = _column(stock_df, "close")
    if direction == "down":
        body_pct = (open_series / close_series - 1.0) * 100.0
        is_directional = close_series < open_series
    else:
        body_pct = (close_series / open_series - 1.0) * 100.0
        is_directional = close_series > open_series
    return pd.Series(body_pct, index=stock_df.index), pd.Series(
        is_directional, index=stock_df.index
    )


def _has_non_decreasing_body_strength(values: pd.Series) -> float:
    sequence = list(values)
    if any(pd.isna(value) for value in sequence):
        return 0.0
    return float(
        all(
            float(sequence[idx]) <= float(sequence[idx + 1])
            for idx in range(len(sequence) - 1)
        )
    )


def _build_candle_run_signal_mask(
    stock_df: pd.DataFrame, params: AnalysisParams
) -> pd.Series:
    direction = params.gap_direction
    run_length = params.candle_run_length
    body_pct, is_directional = _compute_candle_run_body_pct(stock_df, direction)
    prior_body_pct = body_pct.shift(1)
    prior_directional = is_directional.shift(1).eq(True)

    all_directional = pd.Series(
        prior_directional.rolling(run_length).sum(), index=stock_df.index
    ).eq(float(run_length))
    min_body_ok = (
        prior_body_pct.rolling(run_length).min().ge(params.candle_run_min_body_pct)
    )

    first_open = _column(stock_df, "open").shift(run_length)
    last_close = _column(stock_df, "close").shift(1)
    if direction == "down":
        total_move_pct = (first_open / last_close - 1.0) * 100.0
    else:
        total_move_pct = (last_close / first_open - 1.0) * 100.0
    total_move_ok = pd.Series(total_move_pct, index=stock_df.index).ge(
        params.candle_run_total_move_pct
    )

    signal_mask = all_directional & min_body_ok & total_move_ok
    if params.entry_factor == "candle_run_acceleration":
        acceleration_ok = (
            prior_body_pct.rolling(run_length)
            .apply(
                _has_non_decreasing_body_strength,
                raw=False,
            )
            .eq(1.0)
        )
        signal_mask &= acceleration_ok
    return signal_mask.fillna(False)


def _build_eshb_setup_frame(
    stock_df: pd.DataFrame, params: AnalysisParams
) -> pd.DataFrame:
    setup_columns = pd.DataFrame(
        {
            "eshb_anchor_high": math.nan,
            "eshb_anchor_low": math.nan,
            "eshb_base_high": math.nan,
            "eshb_base_low": math.nan,
            "eshb_base_bars": 0,
            "eshb_surge_pct": math.nan,
            "eshb_open_volume_ratio": math.nan,
            "eshb_anchor_break_count": 0,
            "eshb_anchor_break_depth_pct": math.nan,
            "eshb_breakout_volume_ratio": math.nan,
        },
        index=stock_df.index,
    )

    if params.gap_direction != "up":
        return setup_columns

    for idx in range(len(stock_df)):
        if idx < 2:
            continue
        window_start = max(0, idx - params.eshb_open_window_bars)
        anchor_window = stock_df.iloc[window_start:idx]
        if anchor_window.empty:
            continue

        anchor_idx = int(anchor_window["high"].idxmax())
        if anchor_idx >= idx:
            continue

        base_start = anchor_idx + 1
        base_slice = stock_df.iloc[base_start:idx]
        base_bars = len(base_slice)
        if (
            base_bars < params.eshb_base_min_bars
            or base_bars > params.eshb_base_max_bars
        ):
            continue

        anchor_high = float(stock_df.iloc[anchor_idx]["high"])
        anchor_low = float(stock_df.iloc[anchor_idx]["low"])
        opening_ref = float(stock_df.iloc[window_start]["open"])
        if opening_ref <= 0:
            continue
        surge_pct = (anchor_high / opening_ref - 1.0) * 100.0
        if surge_pct < params.eshb_surge_min_pct:
            continue

        base_high = float(base_slice["high"].max())
        base_low = float(base_slice["low"].min())
        if base_high <= 0 or anchor_high <= 0 or anchor_low <= 0:
            continue
        base_pullback_pct = (anchor_high - base_low) / anchor_high * 100.0
        base_range_pct = (base_high - base_low) / base_high * 100.0
        if base_pullback_pct > params.eshb_max_base_pullback_pct:
            continue
        if base_range_pct > params.eshb_max_base_range_pct:
            continue

        anchor_break_count = int((base_slice["low"] < anchor_low).sum())
        anchor_break_depth_pct = (
            (anchor_low - float(base_slice["low"].min())) / anchor_low * 100.0
            if anchor_break_count > 0
            else 0.0
        )
        if anchor_break_count > params.eshb_max_anchor_breaks:
            continue
        if anchor_break_depth_pct > params.eshb_max_anchor_break_depth_pct:
            continue

        prior_volume_window = stock_df.iloc[
            max(0, anchor_idx - params.eshb_open_window_bars) : anchor_idx
        ]
        baseline_volume = float(prior_volume_window["volume"].mean())
        anchor_volume = float(stock_df.iloc[anchor_idx]["volume"])
        open_volume_ratio = (
            anchor_volume / baseline_volume
            if baseline_volume > EPSILON and not pd.isna(anchor_volume)
            else math.nan
        )
        if (
            pd.isna(open_volume_ratio)
            or open_volume_ratio < params.eshb_min_open_volume_ratio
        ):
            continue

        setup_columns.loc[idx, "eshb_anchor_high"] = anchor_high
        setup_columns.loc[idx, "eshb_anchor_low"] = anchor_low
        setup_columns.loc[idx, "eshb_base_high"] = base_high
        setup_columns.loc[idx, "eshb_base_low"] = base_low
        setup_columns.loc[idx, "eshb_base_bars"] = int(base_bars)
        setup_columns.loc[idx, "eshb_surge_pct"] = surge_pct
        setup_columns.loc[idx, "eshb_open_volume_ratio"] = open_volume_ratio
        setup_columns.loc[idx, "eshb_anchor_break_count"] = int(anchor_break_count)
        setup_columns.loc[idx, "eshb_anchor_break_depth_pct"] = float(
            anchor_break_depth_pct
        )

    return setup_columns


def _trade_timestamp_str(value: object, params: AnalysisParams) -> str:
    parsed = pd.to_datetime(cast(Any, value), errors="coerce")
    if bool(cast(Any, pd.isna(parsed))):
        return ""
    timestamp = cast(pd.Timestamp, parsed)
    if params.timeframe == "1d":
        return str(timestamp.date())
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def _trade_date_value(value: object, params: AnalysisParams) -> object:
    parsed = pd.to_datetime(cast(Any, value), errors="coerce")
    if bool(cast(Any, pd.isna(parsed))):
        return pd.NaT
    timestamp = cast(pd.Timestamp, parsed)
    if params.timeframe == "1d":
        return timestamp.date()
    return timestamp


def _resolve_entry_execution(
    signal_row: pd.Series,
    params: AnalysisParams,
    direction: str,
) -> tuple[float | None, float, str | None, str | None, str]:
    entry_factor = str(signal_row.get("entry_factor", params.entry_factor))
    if entry_factor == ESHB_ENTRY_FACTOR:
        trigger_raw = signal_row.get("entry_trigger_price", math.nan)
        trigger_value = (
            math.nan if _is_missing_scalar(trigger_raw) else _float_scalar(trigger_raw)
        )
        return (
            _float_scalar(signal_row["open"]),
            trigger_value,
            "open",
            None,
            entry_factor,
        )
    if entry_factor not in BREAKOUT_ENTRY_FACTORS:
        return _float_scalar(signal_row["open"]), math.nan, "open", None, entry_factor

    trigger_price_raw = signal_row["entry_trigger_price"]
    trigger_price = (
        math.nan
        if _is_missing_scalar(trigger_price_raw)
        else _float_scalar(trigger_price_raw)
    )
    if pd.isna(trigger_price):
        return None, math.nan, None, "entry_not_filled", entry_factor

    day_open = _float_scalar(signal_row["open"])
    day_high = _float_scalar(signal_row["high"])
    day_low = _float_scalar(signal_row["low"])
    day_close = _float_scalar(signal_row["close"])

    reference_buy_price: float | None = None
    entry_fill_type: str | None = None
    if direction == "short":
        if day_open <= trigger_price:
            reference_buy_price = day_open
            entry_fill_type = "open"
        elif day_open > trigger_price >= day_low:
            reference_buy_price = trigger_price
            entry_fill_type = "trigger"
    else:
        if day_open >= trigger_price:
            reference_buy_price = day_open
            entry_fill_type = "open"
        elif day_open < trigger_price <= day_high:
            reference_buy_price = trigger_price
            entry_fill_type = "trigger"

    if reference_buy_price is None or entry_fill_type is None:
        return None, trigger_price, None, "entry_not_filled", entry_factor

    volume = signal_row["volume"] if "volume" in signal_row else math.nan
    is_one_price_bar = day_open == day_high == day_low == day_close
    has_nonpositive_volume = (not _is_missing_scalar(volume)) and _float_scalar(
        volume
    ) <= 0.0
    if has_nonpositive_volume or is_one_price_bar:
        return None, trigger_price, None, "locked_bar_unfillable", entry_factor

    return reference_buy_price, trigger_price, entry_fill_type, None, entry_factor


def apply_gap_filters(df: pd.DataFrame, params: AnalysisParams) -> pd.DataFrame:
    stock_df = df.sort_values("date").reset_index(drop=True).copy()

    for column_name in BOARD_MA_COLUMNS.values():
        if column_name not in stock_df.columns:
            stock_df[column_name] = math.nan

    stock_df["prev_close"] = _column(stock_df, "close").shift(1)
    stock_df["prev_high"] = _column(stock_df, "high").shift(1)
    stock_df["prev_low"] = _column(stock_df, "low").shift(1)
    stock_df["gap_pct_vs_prev_close"] = (
        stock_df["open"] / stock_df["prev_close"] - 1.0
    ) * 100.0

    if params.use_ma_filter:
        stock_df["fast_ma"] = pd.Series(
            _column(stock_df, "close").rolling(params.fast_ma_period).mean(),
            index=stock_df.index,
        ).shift(1)
        stock_df["slow_ma"] = pd.Series(
            _column(stock_df, "close").rolling(params.slow_ma_period).mean(),
            index=stock_df.index,
        ).shift(1)
    else:
        stock_df["fast_ma"] = math.nan
        stock_df["slow_ma"] = math.nan

    stock_df["entry_factor"] = params.entry_factor
    stock_df["entry_trigger_price"] = math.nan
    stock_df["is_contraction"] = False
    stock_df["atr_filter_value"] = math.nan
    stock_df["atr_filter_pct"] = math.nan
    stock_df["eshb_anchor_high"] = math.nan
    stock_df["eshb_anchor_low"] = math.nan
    stock_df["eshb_base_high"] = math.nan
    stock_df["eshb_base_low"] = math.nan
    stock_df["eshb_base_bars"] = 0
    stock_df["eshb_surge_pct"] = math.nan
    stock_df["eshb_open_volume_ratio"] = math.nan
    stock_df["eshb_anchor_break_count"] = 0
    stock_df["eshb_anchor_break_depth_pct"] = math.nan
    stock_df["eshb_breakout_volume_ratio"] = math.nan
    stock_df["setup_pass"] = False
    stock_df["trigger_pass"] = False
    stock_df["setup_reason"] = ""
    stock_df["trigger_reason"] = ""
    stock_df["core_signal_pass"] = False
    stock_df["filter_pass"] = False
    stock_df["ma_filter_pass"] = pd.NA
    stock_df["atr_filter_pass"] = pd.NA
    stock_df["board_ma_filter_pass"] = pd.NA
    stock_df["imported_filter_pass"] = pd.NA
    stock_df["reject_reason_chain"] = ""

    signal_mask = (
        stock_df["prev_close"].notna()
        & stock_df["prev_high"].notna()
        & stock_df["prev_low"].notna()
    )

    setup_mask = signal_mask.copy()
    trigger_mask = signal_mask.copy()

    if params.entry_factor == "gap":
        if params.gap_direction == "up":
            if params.gap_entry_mode == "open_vs_prev_close_threshold":
                setup_mask &= stock_df["gap_pct_vs_prev_close"] >= params.gap_pct
            else:
                setup_mask &= stock_df["open"] > stock_df["prev_high"] * (
                    1.0 + params.gap_ratio
                )
            setup_mask &= (
                stock_df["gap_pct_vs_prev_close"] <= params.max_gap_filter_pct
            )
        else:
            if params.gap_entry_mode == "open_vs_prev_close_threshold":
                setup_mask &= stock_df["gap_pct_vs_prev_close"] <= -params.gap_pct
            else:
                setup_mask &= stock_df["open"] < stock_df["prev_low"] * (
                    1.0 - params.gap_ratio
                )
            setup_mask &= (
                stock_df["gap_pct_vs_prev_close"] >= -params.max_gap_filter_pct
            )
        trigger_mask = setup_mask.copy()
    elif params.entry_factor in BREAKOUT_ENTRY_FACTORS:
        stock_df["entry_trigger_price"] = _compute_breakout_trigger_price(
            stock_df, params
        )
        if params.entry_factor == "volatility_contraction_breakout":
            prior_range = _column(stock_df, "high").shift(1) - _column(
                stock_df, "low"
            ).shift(1)
            contraction_floor = prior_range.rolling(params.vcb_range_lookback).min()
            stock_df["is_contraction"] = prior_range.eq(contraction_floor)
            setup_mask &= stock_df["is_contraction"]
        setup_mask &= stock_df["entry_trigger_price"].notna()
        trigger_mask = setup_mask & _build_breakout_trigger_pass_mask(stock_df, params)
    elif params.entry_factor == ESHB_ENTRY_FACTOR:
        setup_frame = _build_eshb_setup_frame(stock_df, params)
        for column in setup_frame.columns:
            stock_df[column] = setup_frame[column]
        stock_df["entry_trigger_price"] = stock_df["eshb_base_high"] * (
            1.0 + params.eshb_trigger_buffer_pct / 100.0
        )
        setup_mask &= stock_df["eshb_base_high"].notna()
        setup_mask &= stock_df["entry_trigger_price"].notna()
        trigger_mask = setup_mask.copy()
    else:
        setup_mask &= _build_candle_run_signal_mask(stock_df, params)
        trigger_mask = setup_mask.copy()

    stock_df["setup_pass"] = setup_mask.fillna(False)
    stock_df["trigger_pass"] = trigger_mask.fillna(False)
    stock_df.loc[stock_df["setup_pass"], "setup_reason"] = _build_setup_reason(params)
    stock_df.loc[stock_df["trigger_pass"], "trigger_reason"] = _build_trigger_reason(params)
    stock_df["core_signal_pass"] = stock_df["trigger_pass"]
    signal_mask = trigger_mask.copy()

    if params.use_ma_filter:
        ma_filter_mask = _directional_ma_filter_mask(stock_df, params)
        stock_df["ma_filter_pass"] = ma_filter_mask.fillna(False)
        _append_reject_reason(stock_df, signal_mask, ma_filter_mask, "快慢线过滤未通过")
        signal_mask &= ma_filter_mask
    else:
        stock_df["ma_filter_pass"] = pd.NA

    if params.enable_atr_filter:
        stock_df["atr_filter_value"] = _compute_atr_series(
            stock_df, params.atr_filter_period
        ).shift(1)
        stock_df["atr_filter_pct"] = (
            stock_df["atr_filter_value"] / stock_df["prev_close"]
        ) * 100.0
        atr_filter_mask = stock_df["atr_filter_pct"].notna()
        atr_filter_mask &= stock_df["atr_filter_pct"] >= params.min_atr_filter_pct
        atr_filter_mask &= stock_df["atr_filter_pct"] <= params.max_atr_filter_pct
        stock_df["atr_filter_pass"] = atr_filter_mask.fillna(False)
        _append_reject_reason(stock_df, signal_mask, atr_filter_mask, "ATR过滤未通过")
        signal_mask &= atr_filter_mask
    else:
        stock_df["atr_filter_pass"] = pd.NA

    if params.enable_board_ma_filter:
        board_ma_filter_rule = _build_board_ma_filter_rule(params)
        prior_signal_mask = signal_mask.copy()
        signal_mask = _apply_numeric_threshold_filter(
            stock_df,
            signal_mask,
            column_name=board_ma_filter_rule.column_name,
            operator=board_ma_filter_rule.operator,
            threshold=board_ma_filter_rule.threshold,
            value_column_name=board_ma_filter_rule.value_column_name,
        )
        stock_df["board_ma_filter_pass"] = signal_mask.where(prior_signal_mask, pd.NA)
        _append_reject_reason(stock_df, prior_signal_mask, signal_mask, "板块均线过滤未通过")
    else:
        stock_df["board_ma_signal_value"] = math.nan
        stock_df["board_ma_filter_pass"] = pd.NA

    imported_filter_rules = params.effective_imported_indicator_filters
    if imported_filter_rules:
        collected_filter_values: list[pd.Series] = []
        for rule_index, rule in enumerate(imported_filter_rules, start=1):
            numeric_rule = _build_numeric_rule(
                rule, f"imported_indicator_filter_value_{rule_index}"
            )
            prior_signal_mask = signal_mask.copy()
            signal_mask = _apply_numeric_threshold_filter(
                stock_df,
                signal_mask,
                column_name=numeric_rule.column_name,
                operator=numeric_rule.operator,
                threshold=numeric_rule.threshold,
                value_column_name=numeric_rule.value_column_name,
            )
            _append_reject_reason(stock_df, prior_signal_mask, signal_mask, f"导入指标过滤规则{rule_index}未通过")
            collected_filter_values.append(stock_df[numeric_rule.value_column_name])
        stock_df["imported_indicator_filter_value"] = collected_filter_values[0]
        stock_df["imported_filter_pass"] = signal_mask.where(stock_df["trigger_pass"].fillna(False), pd.NA)
    else:
        stock_df["imported_indicator_filter_value"] = math.nan
        stock_df["imported_filter_pass"] = pd.NA

    stock_df["filter_pass"] = signal_mask.fillna(False)
    stock_df["is_signal"] = signal_mask
    return stock_df


def _build_partial_ma_series(
    stock_df: pd.DataFrame, rules: list[PartialExitRule]
) -> dict[int, pd.Series]:
    periods = {
        rule.ma_period
        for rule in rules
        if rule.mode == "ma_exit" and rule.ma_period is not None
    }
    close_series = _column(stock_df, "close")
    return {
        period: pd.Series(close_series.rolling(period).mean(), index=stock_df.index)
        for period in periods
    }


def _build_partial_atr_series(
    stock_df: pd.DataFrame, rules: list[PartialExitRule]
) -> dict[int, pd.Series]:
    periods = {
        int(rule.atr_period)
        for rule in rules
        if rule.mode == "atr_trailing" and rule.atr_period is not None
    }
    return {
        period: cast(pd.Series, _compute_atr_series(stock_df, period).shift(1))
        for period in periods
    }


def _apply_entry_slippage(
    reference_price: float, params: AnalysisParams, direction: str
) -> float:
    if direction == "short":
        return reference_price * (1.0 - params.sell_slippage_ratio)
    return reference_price * (1.0 + params.buy_slippage_ratio)


def _apply_exit_slippage(
    reference_price: float, params: AnalysisParams, direction: str
) -> float:
    if direction == "short":
        return reference_price * (1.0 + params.buy_slippage_ratio)
    return reference_price * (1.0 - params.sell_slippage_ratio)


def _position_profit_ratio(
    reference_entry_price: float, mark_price: float, direction: str
) -> float:
    if direction == "short":
        return (reference_entry_price - mark_price) / reference_entry_price
    return mark_price / reference_entry_price - 1.0


def _total_trade_profit_ratio(
    realized_profit_ratio: float,
    remaining_weight: float,
    reference_entry_price: float,
    mark_price: float,
    direction: str,
) -> float:
    return realized_profit_ratio + remaining_weight * _position_profit_ratio(
        reference_entry_price, mark_price, direction
    )


def _update_peak_total_profit_ratio(
    peak_total_profit_ratio: float,
    realized_profit_ratio: float,
    remaining_weight: float,
    reference_entry_price: float,
    favorable_price: float,
    direction: str,
) -> float:
    candidate_peak = _total_trade_profit_ratio(
        realized_profit_ratio,
        remaining_weight,
        reference_entry_price,
        favorable_price,
        direction,
    )
    return max(peak_total_profit_ratio, candidate_peak)


def _compute_profit_drawdown_ratio(
    peak_total_profit_ratio: float,
    current_total_profit_ratio: float,
) -> float:
    if peak_total_profit_ratio <= EPSILON:
        return math.nan
    return (
        peak_total_profit_ratio - current_total_profit_ratio
    ) / peak_total_profit_ratio


def _price_from_position_profit_ratio(
    reference_entry_price: float,
    position_profit_ratio: float,
    direction: str,
) -> float:
    if direction == "short":
        return reference_entry_price * (1.0 - position_profit_ratio)
    return reference_entry_price * (1.0 + position_profit_ratio)


def _profit_drawdown_trigger_price(
    peak_total_profit_ratio: float,
    realized_profit_ratio: float,
    remaining_weight: float,
    reference_entry_price: float,
    drawdown_ratio: float | None,
    min_profit_to_activate_drawdown_ratio: float,
    direction: str,
) -> float | None:
    if drawdown_ratio is None or remaining_weight <= EPSILON:
        return None
    if peak_total_profit_ratio < min_profit_to_activate_drawdown_ratio:
        return None
    target_total_profit_ratio = peak_total_profit_ratio * (1.0 - drawdown_ratio)
    remaining_position_profit_ratio = (
        target_total_profit_ratio - realized_profit_ratio
    ) / remaining_weight
    return _price_from_position_profit_ratio(
        reference_entry_price, remaining_position_profit_ratio, direction
    )


def _resolve_exit_trigger_execution(
    day_row: pd.Series,
    trigger_price: float | None,
    direction: str,
) -> float | None:
    if trigger_price is None:
        return None
    day_open = _float_scalar(day_row["open"])
    day_high = _float_scalar(day_row["high"])
    day_low = _float_scalar(day_row["low"])
    if direction == "short":
        if day_open >= trigger_price:
            return day_open
        if day_open < trigger_price <= day_high:
            return trigger_price
        return None
    if day_open <= trigger_price:
        return day_open
    if day_open > trigger_price >= day_low:
        return trigger_price
    return None


def _rule_triggered(
    rule: PartialExitRule,
    day_row: pd.Series,
    buy_price: float,
    ma_series: dict[int, pd.Series],
    day_idx: int,
    direction: str,
    peak_total_profit_ratio: float,
    current_total_profit_ratio: float,
    trailing_stop_price: float | None = None,
) -> bool:
    day_close = _float_scalar(day_row["close"])
    day_high = _float_scalar(day_row["high"])
    day_low = _float_scalar(day_row["low"])

    if rule.mode == "fixed_tp":
        if rule.target_profit_ratio is None:
            return False
        if direction == "short":
            return day_low <= buy_price * (1.0 - rule.target_profit_ratio)
        return day_high >= buy_price * (1.0 + rule.target_profit_ratio)

    if rule.mode == "ma_exit":
        if rule.ma_period is None:
            return False
        day_ma = ma_series.get(rule.ma_period)
        if day_ma is None:
            return False
        ma_value = day_ma.iloc[day_idx]
        if pd.isna(ma_value):
            return False
        return (
            day_close > float(ma_value)
            if direction == "short"
            else day_close < float(ma_value)
        )

    if rule.mode == "profit_drawdown":
        return False

    if rule.mode == "atr_trailing":
        if trailing_stop_price is None:
            return False
        return (
            day_high >= trailing_stop_price
            if direction == "short"
            else day_low <= trailing_stop_price
        )

    if rule.mode == "indicator_threshold":
        return _resolve_partial_indicator_threshold_exit(day_row, rule) is not None

    return False


def simulate_trade(
    stock_df: pd.DataFrame,
    signal_idx: int,
    params: AnalysisParams,
    direction: str = "long",
) -> tuple[dict[str, Any] | None, str | None]:
    if direction not in {"long", "short"}:
        return None, "invalid_direction"

    if signal_idx + params.time_stop_days >= len(stock_df):
        return None, "insufficient_future"

    signal_row = stock_df.iloc[signal_idx]
    (
        reference_buy_price,
        entry_trigger_price,
        entry_fill_type,
        entry_skip_reason,
        entry_factor,
    ) = _resolve_entry_execution(signal_row, params, direction)
    if reference_buy_price is None:
        return None, entry_skip_reason

    buy_price = _apply_entry_slippage(reference_buy_price, params, direction)
    buy_date = pd.Timestamp(signal_row["date"])

    stop_loss_price = (
        reference_buy_price * (1.0 + params.stop_loss_ratio)
        if direction == "short"
        else reference_buy_price * (1.0 - params.stop_loss_ratio)
    )
    take_profit_price = (
        reference_buy_price * (1.0 - params.take_profit_ratio)
        if direction == "short"
        else reference_buy_price * (1.0 + params.take_profit_ratio)
    )
    exit_ma_series = (
        pd.Series(
            _column(stock_df, "close").rolling(params.exit_ma_period).mean(),
            index=stock_df.index,
        )
        if params.enable_ma_exit
        else None
    )
    atr_trailing_series = (
        _compute_atr_series(stock_df, params.atr_trailing_period).shift(1)
        if params.enable_atr_trailing_exit
        else None
    )

    fills: list[TradeFill] = []
    remaining_weight = 1.0
    max_holding_days = len(stock_df) - signal_idx - 1
    realized_trigger_profit_ratio = 0.0
    peak_total_profit_ratio = 0.0

    partial_rules = (
        sorted(
            [rule for rule in params.partial_exit_rules if rule.enabled],
            key=lambda item: item.priority,
        )
        if params.partial_exit_enabled
        else []
    )
    partial_ma_series = (
        _build_partial_ma_series(stock_df, partial_rules) if partial_rules else {}
    )
    partial_atr_series = (
        _build_partial_atr_series(stock_df, partial_rules) if partial_rules else {}
    )
    triggered_rule_priority: set[int] = set()

    triggered_profit_drawdown_ratio = math.nan
    triggered_exit_ma_value = math.nan
    triggered_board_ma_value = math.nan
    triggered_imported_indicator_exit_value = math.nan
    triggered_partial_indicator_value = math.nan
    triggered_partial_indicator_label = ""
    trailing_reference_price = (
        float(signal_row["low"]) if direction == "short" else float(signal_row["high"])
    )

    for holding_days in range(1, max_holding_days + 1):
        day_idx = signal_idx + holding_days
        day_row = stock_df.iloc[day_idx]
        day_date = pd.Timestamp(day_row["date"])
        day_close = float(day_row["close"])
        day_high = float(day_row["high"])
        day_low = float(day_row["low"])
        favorable_price = day_low if direction == "short" else day_high
        prior_peak_total_profit_ratio = peak_total_profit_ratio
        bar_start_realized_trigger_profit_ratio = realized_trigger_profit_ratio
        bar_start_remaining_weight = remaining_weight

        # 1) 全仓止损
        stop_hit = (
            day_high >= stop_loss_price
            if direction == "short"
            else day_low <= stop_loss_price
        )
        if stop_hit and remaining_weight > EPSILON:
            fills.append(
                TradeFill(
                    _trade_timestamp_str(day_date, params),
                    _apply_exit_slippage(stop_loss_price, params, direction),
                    remaining_weight,
                    "stop_loss",
                    holding_days,
                )
            )
            remaining_weight = 0.0
            break

        # 2) 分批退出
        if params.partial_exit_enabled and remaining_weight > EPSILON:
            for rule in partial_rules:
                if rule.priority in triggered_rule_priority:
                    continue
                current_peak_total_profit_ratio = _update_peak_total_profit_ratio(
                    peak_total_profit_ratio,
                    realized_trigger_profit_ratio,
                    remaining_weight,
                    reference_buy_price,
                    favorable_price,
                    direction,
                )
                current_total_profit_ratio = _total_trade_profit_ratio(
                    realized_trigger_profit_ratio,
                    remaining_weight,
                    reference_buy_price,
                    day_close,
                    direction,
                )
                trailing_stop_price: float | None = None
                profit_drawdown_fill_price: float | None = None
                profit_drawdown_peak_total_profit_ratio: float | None = None
                if rule.mode == "atr_trailing" and rule.atr_period is not None:
                    atr_series = partial_atr_series.get(int(rule.atr_period))
                    atr_value = (
                        None
                        if atr_series is None or pd.isna(atr_series.iloc[day_idx])
                        else float(atr_series.iloc[day_idx])
                    )
                    trailing_stop_price = _atr_trailing_stop_price(
                        trailing_reference_price,
                        atr_value,
                        rule.atr_multiplier,
                        direction,
                    )
                if rule.mode == "profit_drawdown":
                    prior_trigger_price = _profit_drawdown_trigger_price(
                        prior_peak_total_profit_ratio,
                        bar_start_realized_trigger_profit_ratio,
                        bar_start_remaining_weight,
                        reference_buy_price,
                        rule.drawdown_ratio,
                        rule.min_profit_to_activate_drawdown_ratio,
                        direction,
                    )
                    profit_drawdown_fill_price = _resolve_exit_trigger_execution(
                        day_row, prior_trigger_price, direction
                    )
                    if profit_drawdown_fill_price is not None:
                        profit_drawdown_peak_total_profit_ratio = (
                            prior_peak_total_profit_ratio
                        )
                    if profit_drawdown_fill_price is None:
                        continue
                elif not _rule_triggered(
                    rule,
                    day_row,
                    reference_buy_price,
                    partial_ma_series,
                    day_idx,
                    direction,
                    current_peak_total_profit_ratio,
                    current_total_profit_ratio,
                    trailing_stop_price=trailing_stop_price,
                ):
                    continue

                rule_weight = min(remaining_weight, rule.weight_ratio)
                if rule_weight <= EPSILON:
                    triggered_rule_priority.add(rule.priority)
                    continue

                fill_price = _apply_exit_slippage(day_close, params, direction)
                reference_fill_price = day_close
                fill_exit_type = rule.mode
                if rule.mode == "fixed_tp" and rule.target_profit_ratio is not None:
                    target_price = (
                        reference_buy_price * (1.0 - rule.target_profit_ratio)
                        if direction == "short"
                        else reference_buy_price * (1.0 + rule.target_profit_ratio)
                    )
                    reference_fill_price = target_price
                    fill_price = _apply_exit_slippage(target_price, params, direction)
                if rule.mode == "ma_exit" and rule.ma_period is not None:
                    ma_value = partial_ma_series[rule.ma_period].iloc[day_idx]
                    if pd.notna(ma_value):
                        triggered_exit_ma_value = float(ma_value)
                if rule.mode == "profit_drawdown":
                    if (
                        profit_drawdown_fill_price is None
                        or profit_drawdown_peak_total_profit_ratio is None
                    ):
                        continue
                    reference_fill_price = profit_drawdown_fill_price
                    fill_price = _apply_exit_slippage(
                        profit_drawdown_fill_price, params, direction
                    )
                    execution_total_profit_ratio = _total_trade_profit_ratio(
                        bar_start_realized_trigger_profit_ratio,
                        bar_start_remaining_weight,
                        reference_buy_price,
                        reference_fill_price,
                        direction,
                    )
                    triggered_profit_drawdown_ratio = _compute_profit_drawdown_ratio(
                        float(profit_drawdown_peak_total_profit_ratio),
                        execution_total_profit_ratio,
                    )
                if rule.mode == "atr_trailing" and trailing_stop_price is not None:
                    reference_fill_price = trailing_stop_price
                    fill_price = _apply_exit_slippage(
                        trailing_stop_price, params, direction
                    )
                if rule.mode == "indicator_threshold":
                    fill_exit_type = "indicator_threshold"
                    triggered_partial_indicator_value = float(
                        _resolve_partial_indicator_threshold_exit(day_row, rule)
                        or math.nan
                    )
                    indicator_label = (
                        str(rule.indicator_key or "").strip() or "导入指标"
                    )
                    indicator_column = str(rule.indicator_column or "").strip()
                    threshold_text = (
                        ""
                        if rule.indicator_threshold is None
                        else f" {rule.indicator_operator} {float(rule.indicator_threshold):g}"
                    )
                    triggered_partial_indicator_label = f"第{rule.priority}批 {indicator_label}.{indicator_column}{threshold_text}"

                fills.append(
                    TradeFill(
                        _trade_timestamp_str(day_date, params),
                        float(fill_price),
                        float(rule_weight),
                        fill_exit_type,
                        holding_days,
                    )
                )
                remaining_weight -= rule_weight
                realized_trigger_profit_ratio += rule_weight * _position_profit_ratio(
                    reference_buy_price, reference_fill_price, direction
                )
                triggered_rule_priority.add(rule.priority)
                peak_total_profit_ratio = _update_peak_total_profit_ratio(
                    current_peak_total_profit_ratio,
                    realized_trigger_profit_ratio,
                    remaining_weight,
                    reference_buy_price,
                    favorable_price,
                    direction,
                )
                if remaining_weight <= EPSILON:
                    remaining_weight = 0.0
                    break
        peak_total_profit_ratio = _update_peak_total_profit_ratio(
            peak_total_profit_ratio,
            realized_trigger_profit_ratio,
            remaining_weight,
            reference_buy_price,
            favorable_price,
            direction,
        )

        # 3) 整笔 OR 离场（对剩余仓位统一生效）
        if remaining_weight > EPSILON and params.enable_board_ma_exit:
            board_ma_exit_rule = _build_board_ma_exit_rule(params)
            board_ma_value = _resolve_numeric_threshold_exit(
                day_row,
                column_name=board_ma_exit_rule.column_name,
                operator=board_ma_exit_rule.operator,
                threshold=board_ma_exit_rule.threshold,
            )
            if board_ma_value is not None:
                fills.append(
                    TradeFill(
                        _trade_timestamp_str(day_date, params),
                        _apply_exit_slippage(day_close, params, direction),
                        remaining_weight,
                        "board_ma_exit",
                        holding_days,
                    )
                )
                triggered_board_ma_value = board_ma_value
                remaining_weight = 0.0

        if remaining_weight > EPSILON:
            for rule in params.effective_imported_indicator_exits:
                imported_indicator_exit_rule = _build_numeric_rule(
                    rule, "imported_indicator_exit_value"
                )
                imported_indicator_exit_value = _resolve_numeric_threshold_exit(
                    day_row,
                    column_name=imported_indicator_exit_rule.column_name,
                    operator=imported_indicator_exit_rule.operator,
                    threshold=imported_indicator_exit_rule.threshold,
                )
                if imported_indicator_exit_value is None:
                    continue
                fills.append(
                    TradeFill(
                        _trade_timestamp_str(day_date, params),
                        _apply_exit_slippage(day_close, params, direction),
                        remaining_weight,
                        "imported_indicator_exit",
                        holding_days,
                    )
                )
                triggered_imported_indicator_exit_value = imported_indicator_exit_value
                remaining_weight = 0.0
                break

        # 4) 旧版整笔退出
        if (not params.partial_exit_enabled) and remaining_weight > EPSILON:
            if remaining_weight > EPSILON and params.enable_profit_drawdown_exit:
                current_total_profit_ratio = _total_trade_profit_ratio(
                    realized_trigger_profit_ratio,
                    remaining_weight,
                    reference_buy_price,
                    day_close,
                    direction,
                )
                profit_drawdown_ratio = _compute_profit_drawdown_ratio(
                    peak_total_profit_ratio, current_total_profit_ratio
                )
                if (
                    peak_total_profit_ratio
                    >= params.min_profit_to_activate_profit_drawdown_ratio
                    and not pd.isna(profit_drawdown_ratio)
                    and profit_drawdown_ratio >= params.profit_drawdown_ratio
                ):
                    fills.append(
                        TradeFill(
                            _trade_timestamp_str(day_date, params),
                            _apply_exit_slippage(day_close, params, direction),
                            remaining_weight,
                            "profit_drawdown_exit",
                            holding_days,
                        )
                    )
                    triggered_profit_drawdown_ratio = profit_drawdown_ratio
                    remaining_weight = 0.0

            if (
                remaining_weight > EPSILON
                and params.enable_ma_exit
                and exit_ma_series is not None
            ):
                day_exit_ma = exit_ma_series.iloc[day_idx]
                if pd.notna(day_exit_ma):
                    exit_hit = (
                        day_close > float(day_exit_ma)
                        if direction == "short"
                        else day_close < float(day_exit_ma)
                    )
                    if exit_hit:
                        fills.append(
                            TradeFill(
                                _trade_timestamp_str(day_date, params),
                                _apply_exit_slippage(day_close, params, direction),
                                remaining_weight,
                                "ma_exit",
                                holding_days,
                            )
                        )
                        triggered_exit_ma_value = float(day_exit_ma)
                        remaining_weight = 0.0

            if (
                remaining_weight > EPSILON
                and params.enable_atr_trailing_exit
                and atr_trailing_series is not None
            ):
                atr_value = atr_trailing_series.iloc[day_idx]
                trailing_stop_price = _atr_trailing_stop_price(
                    trailing_reference_price,
                    None if pd.isna(atr_value) else float(atr_value),
                    params.atr_trailing_multiplier,
                    direction,
                )
                atr_hit = trailing_stop_price is not None and (
                    day_high >= trailing_stop_price
                    if direction == "short"
                    else day_low <= trailing_stop_price
                )
                if (
                    atr_hit
                    and trailing_stop_price is not None
                    and peak_total_profit_ratio
                    >= params.min_profit_to_activate_atr_trailing_ratio
                ):
                    fills.append(
                        TradeFill(
                            _trade_timestamp_str(day_date, params),
                            _apply_exit_slippage(
                                trailing_stop_price, params, direction
                            ),
                            remaining_weight,
                            "atr_trailing",
                            holding_days,
                        )
                    )
                    remaining_weight = 0.0

            tp_hit = (
                day_low <= take_profit_price
                if direction == "short"
                else day_high >= take_profit_price
            )
            if remaining_weight > EPSILON and params.enable_take_profit and tp_hit:
                fills.append(
                    TradeFill(
                        _trade_timestamp_str(day_date, params),
                        _apply_exit_slippage(take_profit_price, params, direction),
                        remaining_weight,
                        "take_profit",
                        holding_days,
                    )
                )
                remaining_weight = 0.0

        # 5) 时间退出
        if remaining_weight > EPSILON and holding_days >= params.time_stop_days:
            holding_return = (
                (reference_buy_price - day_close) / reference_buy_price
                if direction == "short"
                else day_close / reference_buy_price - 1.0
            )
            if holding_return < params.time_stop_target_ratio:
                fills.append(
                    TradeFill(
                        _trade_timestamp_str(day_date, params),
                        _apply_exit_slippage(day_close, params, direction),
                        remaining_weight,
                        "time_exit",
                        holding_days,
                    )
                )
                remaining_weight = 0.0

        if remaining_weight <= EPSILON:
            remaining_weight = 0.0
            break

        trailing_reference_price = (
            min(trailing_reference_price, favorable_price)
            if direction == "short"
            else max(trailing_reference_price, favorable_price)
        )

    # 6) 数据结束处理
    if remaining_weight > EPSILON:
        if params.time_exit_mode == "strict":
            return None, "unclosed_trade"
        if params.time_exit_mode == "force_close":
            last_idx = len(stock_df) - 1
            last_row = stock_df.iloc[last_idx]
            reference_force_close = float(last_row["close"])
            fills.append(
                TradeFill(
                    _trade_timestamp_str(pd.Timestamp(last_row["date"]), params),
                    _apply_exit_slippage(reference_force_close, params, direction),
                    remaining_weight,
                    "force_close",
                    last_idx - signal_idx,
                )
            )
            remaining_weight = 0.0

    if not fills:
        return None, "no_exit"
    if remaining_weight > EPSILON:
        return None, "unclosed_trade"

    fill_dicts = [asdict(fill) for fill in fills]
    total_weight = sum(fill["weight"] for fill in fill_dicts)
    if total_weight <= EPSILON:
        return None, "no_exit"

    weighted_sell_price = (
        sum(fill["sell_price"] * fill["weight"] for fill in fill_dicts) / total_weight
    )
    exit_reason = "+".join(
        _exit_reason_label(str(fill["exit_type"])) for fill in fill_dicts
    )
    sell_ts = pd.to_datetime(fill_dicts[-1]["sell_date"])
    sell_date = _trade_date_value(sell_ts, params)
    exit_type = "+".join(fill["exit_type"] for fill in fill_dicts)
    sell_day_idx = signal_idx + int(fill_dicts[-1]["holding_days"])

    actual_buy_cost = buy_price * (1.0 + params.buy_cost_ratio)
    actual_sell_value = weighted_sell_price * (1.0 - params.sell_cost_ratio)
    gross_return = (
        (buy_price - weighted_sell_price) / buy_price
        if direction == "short"
        else weighted_sell_price / buy_price - 1.0
    )
    net_return = (
        actual_sell_value / actual_buy_cost - 1.0
        if direction == "long"
        else (buy_price - actual_sell_value) / actual_buy_cost
    )

    holding_slice = stock_df.iloc[signal_idx + 1 : sell_day_idx + 1]
    if holding_slice.empty:
        mfe = 0.0
        mae = 0.0
    elif direction == "short":
        mfe = (buy_price - holding_slice["low"].min()) / buy_price
        mae = (buy_price - holding_slice["high"].max()) / buy_price
    else:
        mfe = holding_slice["high"].max() / buy_price - 1.0
        mae = holding_slice["low"].min() / buy_price - 1.0

    return {
        "date": _trade_date_value(buy_date, params),
        "stock_code": signal_row["stock_code"],
        "prev_close": float(signal_row["prev_close"]),
        "prev_high": float(signal_row["prev_high"]),
        "prev_low": float(signal_row["prev_low"]),
        "open": float(signal_row["open"]),
        "close": float(signal_row["close"]),
        "volume": float(signal_row["volume"])
        if pd.notna(signal_row["volume"])
        else math.nan,
        "gap_pct_vs_prev_close": float(signal_row["gap_pct_vs_prev_close"]),
        "buy_date": _trade_timestamp_str(buy_date, params),
        "buy_price": buy_price,
        "sell_price": float(weighted_sell_price),
        "sell_date": sell_date,
        "exit_type": exit_type,
        "holding_days": int(fill_dicts[-1]["holding_days"]),
        "fills": fill_dicts,
        "gross_return_pct": gross_return * 100.0,
        "net_return_pct": net_return * 100.0,
        "win_flag": 1 if net_return > 0 else 0,
        "mfe_pct": float(mfe) * 100.0,
        "mae_pct": float(mae) * 100.0,
        "max_profit_pct": float(mfe) * 100.0,
        "exit_ma_value": float(triggered_exit_ma_value)
        if pd.notna(triggered_exit_ma_value)
        else math.nan,
        "profit_drawdown_ratio": float(triggered_profit_drawdown_ratio) * 100.0
        if pd.notna(triggered_profit_drawdown_ratio)
        else math.nan,
        "board_ma_value": float(triggered_board_ma_value)
        if pd.notna(triggered_board_ma_value)
        else math.nan,
        "imported_indicator_exit_value": float(triggered_imported_indicator_exit_value)
        if pd.notna(triggered_imported_indicator_exit_value)
        else math.nan,
        "partial_indicator_rule_label": triggered_partial_indicator_label,
        "partial_indicator_trigger_value": float(triggered_partial_indicator_value)
        if pd.notna(triggered_partial_indicator_value)
        else math.nan,
        "entry_factor": entry_factor,
        "entry_reason": _build_entry_reason(params, entry_factor),
        "entry_trigger_price": float(entry_trigger_price)
        if pd.notna(entry_trigger_price)
        else math.nan,
        "entry_fill_type": entry_fill_type,
        "exit_reason": exit_reason,
    }, None
