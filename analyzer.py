from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
from itertools import product
import json
from typing import Any, Callable, cast

import pandas as pd

from data_loader import load_local_parquet_data
from models import AnalysisParams, PartialExitRule, apply_scan_overrides
from rules import ESHB_ENTRY_FACTOR, apply_gap_filters, simulate_trade


BASE_DETAIL_COLUMNS = [
    "date",
    "stock_code",
    "prev_close",
    "prev_high",
    "prev_low",
    "open",
    "close",
    "volume",
    "gap_pct_vs_prev_close",
    "buy_price",
    "sell_price",
    "sell_date",
    "exit_type",
    "holding_days",
    "gross_return_pct",
    "net_return_pct",
    "win_flag",
    "mfe_pct",
    "mae_pct",
    "max_profit_pct",
    "exit_ma_value",
    "profit_drawdown_ratio",
    "board_ma_value",
    "imported_indicator_exit_value",
    "partial_indicator_rule_label",
    "partial_indicator_trigger_value",
    "fills",
    "fill_count",
    "fill_detail_json",
    "entry_factor",
    "entry_reason",
    "entry_trigger_price",
    "entry_fill_type",
    "exit_reason",
]

REJECTED_SIGNAL_COLUMNS = [
    "date",
    "stock_code",
    "entry_factor",
    "entry_trigger_price",
    "gap_pct_vs_prev_close",
    "reject_reason_chain",
]

SIGNAL_TRACE_COLUMNS = [
    "date",
    "stock_code",
    "entry_factor",
    "entry_trigger_price",
    "setup_pass",
    "setup_reason",
    "trigger_pass",
    "trigger_reason",
    "filter_pass",
    "ma_filter_pass",
    "atr_filter_pass",
    "board_ma_filter_pass",
    "imported_filter_pass",
    "trade_closed",
    "reject_reason_chain",
    "execution_skip_reason",
]

DETAIL_COLUMNS = BASE_DETAIL_COLUMNS + [
    "trade_no",
    "nav_before_trade",
    "nav_after_trade",
]

DAILY_COLUMNS = [
    "date",
    "sample_count",
    "win_count",
    "lose_count",
    "win_rate_pct",
    "avg_net_return_pct",
    "median_net_return_pct",
    "avg_holding_days",
]

EQUITY_COLUMNS = [
    "date",
    "net_value",
    "drawdown_pct",
    "trade_no",
    "stock_code",
    "event",
]

SCAN_RESULT_COLUMNS = [
    "scan_id",
    "rank",
    "signal_count",
    "closed_trade_candidates",
    "executed_trades",
    "strategy_win_rate_pct",
    "total_return_pct",
    "max_drawdown_pct",
    "final_net_value",
    "avg_holding_days",
    "profit_risk_ratio",
    "trade_return_volatility_pct",
]

TRADE_BEHAVIOR_COLUMNS = [
    "executed_trades",
    "win_rate_pct",
    "avg_net_return_pct",
    "median_net_return_pct",
    "avg_mfe_pct",
    "avg_mae_pct",
    "avg_give_back_pct",
    "avg_mfe_capture_pct",
    "trigger_fill_share_pct",
    "multi_fill_trade_share_pct",
    "avg_profit_drawdown_ratio",
]

DD_EPISODE_COLUMNS = [
    "episode_no",
    "drawdown_start_date",
    "trough_date",
    "recovery_date",
    "peak_to_trough_pct",
    "underwater_bars",
    "trade_count",
    "worst_trade_return_pct",
    "dominant_entry_reason",
    "recovered_flag",
]

DD_CONTRIBUTOR_COLUMNS = [
    "entry_reason",
    "trade_count",
    "avg_net_return_pct",
    "total_net_return_pct",
    "avg_mae_pct",
    "avg_mfe_pct",
]

ANOMALY_QUEUE_COLUMNS = [
    "anomaly_type",
    "severity_score",
    "trade_no",
    "date",
    "stock_code",
    "holding_days",
    "activation_threshold_pct",
    "threshold_excess_pct",
    "holding_anchor_mfe_pct",
    "holding_anchor_mae_pct",
    "give_back_pct",
    "net_return_pct",
    "entry_reason",
    "exit_reason",
    "anomaly_note",
]


@dataclass(frozen=True)
class BacktestResultBundle:
    detail_df: pd.DataFrame
    signal_trace_df: pd.DataFrame
    rejected_signal_df: pd.DataFrame
    daily_df: pd.DataFrame
    equity_df: pd.DataFrame
    trade_behavior_df: pd.DataFrame
    drawdown_episodes_df: pd.DataFrame
    drawdown_contributors_df: pd.DataFrame
    anomaly_queue_df: pd.DataFrame
    stats: dict[str, float]
    scan_df: pd.DataFrame
    best_scan_overrides: dict[str, int | float]
    per_stock_stats_df: pd.DataFrame
    batch_backtest_mode: str


@dataclass(frozen=True)
class ScanExecutionContext:
    grouped_stock_frames: tuple[pd.DataFrame, ...]
    execution_by_code: dict[str, pd.DataFrame]


def _empty_scan_stats() -> dict[str, int]:
    return {
        "core_signal_count": 0,
        "signal_count": 0,
        "rejected_signal_count": 0,
        "closed_trade_candidates": 0,
        "skipped_insufficient_future": 0,
        "skipped_unclosed_trade": 0,
        "skipped_no_exit": 0,
        "skipped_entry_not_filled": 0,
        "skipped_locked_bar_unfillable": 0,
    }


def _empty_strategy_stats() -> dict[str, float]:
    return {
        "executed_trades": 0,
        "skipped_overlapping_position": 0,
        "win_count": 0,
        "lose_count": 0,
        "strategy_win_rate_pct": 0.0,
        "final_net_value": 1.0,
        "total_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "avg_holding_days": 0.0,
        "median_net_return_pct": 0.0,
        "avg_mfe_pct": 0.0,
        "avg_mae_pct": 0.0,
        "profit_risk_ratio": 0.0,
        "trade_return_volatility_pct": 0.0,
    }


def _signal_window(params: AnalysisParams) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = cast(pd.Timestamp, pd.to_datetime(params.start_date))
    end_ts = cast(pd.Timestamp, pd.to_datetime(params.end_date))
    if params.timeframe == "1d":
        return start_ts, end_ts
    intraday_end = cast(
        pd.Timestamp,
        end_ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1),
    )
    return start_ts, intraday_end


def _prepare_execution_frame(
    stock_df: pd.DataFrame, params: AnalysisParams
) -> pd.DataFrame:
    execution_df = stock_df.sort_values("date").reset_index(drop=True).copy()
    execution_df["prev_close"] = execution_df["close"].shift(1)
    execution_df["prev_high"] = execution_df["high"].shift(1)
    execution_df["prev_low"] = execution_df["low"].shift(1)
    execution_df["gap_pct_vs_prev_close"] = (
        execution_df["open"] / execution_df["prev_close"] - 1.0
    ) * 100.0
    execution_df["entry_factor"] = params.entry_factor
    execution_df["entry_trigger_price"] = pd.NA
    execution_df["entry_fill_type"] = pd.NA
    execution_df["is_signal"] = False
    return execution_df


def _breakout_volume_ratio(
    execution_df: pd.DataFrame, trigger_idx: int, lookback: int
) -> float:
    if trigger_idx <= 0:
        return 1.0
    baseline_start = max(0, trigger_idx - max(lookback, 1))
    baseline = execution_df.iloc[baseline_start:trigger_idx]["volume"].mean()
    trigger_volume = float(execution_df.iloc[trigger_idx]["volume"])
    if pd.isna(baseline) or float(baseline) <= 1e-12 or pd.isna(trigger_volume):
        return 0.0
    return float(trigger_volume) / float(baseline)


def _build_grouped_stock_frames(all_data: pd.DataFrame) -> tuple[pd.DataFrame, ...]:
    if all_data.empty:
        return ()
    return tuple(
        stock_df.sort_values("date").reset_index(drop=True).copy()
        for _, stock_df in all_data.groupby("stock_code", sort=True)
    )


def _build_scan_execution_context(
    all_data: pd.DataFrame, params: AnalysisParams
) -> ScanExecutionContext:
    grouped_stock_frames = _build_grouped_stock_frames(all_data)
    execution_by_code: dict[str, pd.DataFrame] = {}
    if params.entry_factor == ESHB_ENTRY_FACTOR:
        execution_data = load_local_parquet_data(
            start_date=params.start_date,
            end_date=params.end_date,
            stock_codes=params.stock_codes,
            lookback_days=params.required_lookback_days,
            lookahead_days=params.required_lookahead_days,
            local_data_root=params.local_data_root,
            adjust=params.adjust,
            timeframe="5m",
            indicator_keys=params.execution_indicator_keys,
        )
        for stock_code, stock_df in execution_data.groupby("stock_code", sort=True):
            execution_by_code[str(stock_code)] = _prepare_execution_frame(
                stock_df, params
            )
    return ScanExecutionContext(
        grouped_stock_frames=grouped_stock_frames,
        execution_by_code=execution_by_code,
    )


def _scan_trade_candidates_with_context(
    params: AnalysisParams,
    context: ScanExecutionContext,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    if not context.grouped_stock_frames:
        return (
            pd.DataFrame(columns=pd.Index(BASE_DETAIL_COLUMNS)),
            pd.DataFrame(columns=pd.Index(SIGNAL_TRACE_COLUMNS)),
            pd.DataFrame(columns=pd.Index(REJECTED_SIGNAL_COLUMNS)),
            _empty_scan_stats(),
        )

    start_ts, end_ts = _signal_window(params)

    detail_records: list[dict[str, Any]] = []
    signal_trace_records: list[dict[str, Any]] = []
    rejected_signal_frames: list[pd.DataFrame] = []
    stats = Counter()

    for stock_df in context.grouped_stock_frames:
        enriched = apply_gap_filters(stock_df, params)
        for column_name in REJECTED_SIGNAL_COLUMNS:
            if column_name not in enriched.columns:
                enriched[column_name] = pd.NA if column_name != "reject_reason_chain" else ""
        if "core_signal_pass" not in enriched.columns:
            enriched["core_signal_pass"] = enriched.get("is_signal", False)
        if "filter_pass" not in enriched.columns:
            enriched["filter_pass"] = enriched.get("is_signal", False)
        if "reject_reason_chain" not in enriched.columns:
            enriched["reject_reason_chain"] = ""
        if "setup_pass" not in enriched.columns:
            enriched["setup_pass"] = enriched.get("core_signal_pass", enriched.get("is_signal", False))
        if "trigger_pass" not in enriched.columns:
            enriched["trigger_pass"] = enriched.get("core_signal_pass", enriched.get("is_signal", False))
        trace_base_columns = [
            "date",
            "stock_code",
            "entry_factor",
            "entry_trigger_price",
            "setup_pass",
            "setup_reason",
            "trigger_pass",
            "trigger_reason",
            "filter_pass",
            "ma_filter_pass",
            "atr_filter_pass",
            "board_ma_filter_pass",
            "imported_filter_pass",
            "reject_reason_chain",
        ]
        for column_name in trace_base_columns:
            if column_name not in enriched.columns:
                enriched[column_name] = pd.NA if column_name != "reject_reason_chain" else ""
        trace_preview = enriched.loc[
            enriched["setup_pass"].fillna(False) & enriched["date"].between(start_ts, end_ts),
            trace_base_columns,
        ].copy()
        if not trace_preview.empty:
            trace_preview["trade_closed"] = False
            trace_preview["execution_skip_reason"] = ""
        core_signal_window_mask = enriched["core_signal_pass"].fillna(False) & enriched["date"].between(start_ts, end_ts)
        rejected_signal_mask = (
            enriched["core_signal_pass"].fillna(False)
            & (~enriched["filter_pass"].fillna(False))
        ) & enriched["date"].between(start_ts, end_ts)
        rejected_signal_preview = enriched.loc[
            rejected_signal_mask, REJECTED_SIGNAL_COLUMNS
        ].copy()
        if not rejected_signal_preview.empty:
            rejected_signal_frames.append(rejected_signal_preview)
        stats["core_signal_count"] += int(core_signal_window_mask.sum())
        stats["rejected_signal_count"] += int(rejected_signal_mask.sum())
        signal_mask = enriched["is_signal"] & enriched["date"].between(start_ts, end_ts)
        signal_indices = enriched.index[signal_mask].tolist()
        stats["signal_count"] += len(signal_indices)

        for signal_idx in signal_indices:
            direction = "long" if params.gap_direction == "up" else "short"
            if params.entry_factor == ESHB_ENTRY_FACTOR:
                setup_row = enriched.iloc[signal_idx]
                stock_code = str(setup_row["stock_code"])
                execution_df = context.execution_by_code.get(stock_code)
                if execution_df is None or execution_df.empty:
                    stats["skipped_no_exit"] += 1
                    continue

                trigger_price = float(setup_row["entry_trigger_price"])
                setup_time = pd.Timestamp(setup_row["date"])
                candidate_exec = execution_df.loc[execution_df["date"] > setup_time]
                trigger_hits = candidate_exec.loc[
                    candidate_exec["high"] >= trigger_price
                ]
                if trigger_hits.empty:
                    trade = None
                    skip_reason = "entry_not_filled"
                else:
                    trigger_idx = int(trigger_hits.index[0])
                    breakout_volume_ratio = _breakout_volume_ratio(
                        execution_df, trigger_idx, int(params.eshb_open_window_bars)
                    )
                    if breakout_volume_ratio < params.eshb_min_breakout_volume_ratio:
                        trade = None
                        skip_reason = "entry_not_filled"
                    else:
                        entry_idx = trigger_idx + 1
                        if entry_idx >= len(execution_df):
                            trade = None
                            skip_reason = "insufficient_future"
                        else:
                            execution_df.loc[entry_idx, "entry_factor"] = (
                                ESHB_ENTRY_FACTOR
                            )
                            execution_df.loc[entry_idx, "entry_trigger_price"] = (
                                trigger_price
                            )
                            execution_df.loc[
                                entry_idx, "eshb_breakout_volume_ratio"
                            ] = breakout_volume_ratio
                            trade, skip_reason = simulate_trade(
                                execution_df, entry_idx, params, direction=direction
                            )
            else:
                trade, skip_reason = simulate_trade(
                    enriched, signal_idx, params, direction=direction
                )
            if trade is None:
                if not trace_preview.empty:
                    matching_mask = (
                        pd.to_datetime(trace_preview["date"]).eq(pd.to_datetime(enriched.iloc[signal_idx]["date"]))
                        & trace_preview["stock_code"].astype(str).eq(str(enriched.iloc[signal_idx]["stock_code"]))
                    )
                    trace_preview.loc[matching_mask, "execution_skip_reason"] = str(skip_reason or "")
                if skip_reason == "insufficient_future":
                    stats["skipped_insufficient_future"] += 1
                elif skip_reason == "unclosed_trade":
                    stats["skipped_unclosed_trade"] += 1
                elif skip_reason == "entry_not_filled":
                    stats["skipped_entry_not_filled"] += 1
                elif skip_reason == "locked_bar_unfillable":
                    stats["skipped_locked_bar_unfillable"] += 1
                else:
                    stats["skipped_no_exit"] += 1
                continue

            fills = trade.get("fills", [])
            total_weight = sum(float(fill.get("weight", 0.0)) for fill in fills)
            if total_weight <= 0:
                stats["skipped_no_exit"] += 1
                continue
            trade["sell_price"] = (
                sum(float(fill["sell_price"]) * float(fill["weight"]) for fill in fills)
                / total_weight
            )
            trade["sell_date"] = fills[-1]["sell_date"]
            trade["exit_type"] = "+".join(str(fill["exit_type"]) for fill in fills)
            trade["fill_count"] = len(fills)
            trade["fill_detail_json"] = json.dumps(fills, ensure_ascii=False)
            detail_records.append(trade)
            stats["closed_trade_candidates"] += 1
            if not trace_preview.empty:
                matching_mask = (
                    pd.to_datetime(trace_preview["date"]).eq(pd.to_datetime(trade["date"]))
                    & trace_preview["stock_code"].astype(str).eq(str(trade["stock_code"]))
                )
                trace_preview.loc[matching_mask, "trade_closed"] = True

        if not trace_preview.empty:
            signal_trace_records.extend(trace_preview.to_dict("records"))

    detail_df = pd.DataFrame(detail_records, columns=pd.Index(BASE_DETAIL_COLUMNS))
    signal_trace_df = pd.DataFrame(signal_trace_records, columns=pd.Index(SIGNAL_TRACE_COLUMNS))
    rejected_signal_df = (
        pd.concat(rejected_signal_frames, ignore_index=True)
        if rejected_signal_frames
        else pd.DataFrame(columns=pd.Index(REJECTED_SIGNAL_COLUMNS))
    )
    if detail_df.empty:
        return detail_df, signal_trace_df, rejected_signal_df, {**_empty_scan_stats(), **dict(stats)}

    detail_df = detail_df.sort_values(["date", "stock_code", "sell_date"]).reset_index(
        drop=True
    )
    if not rejected_signal_df.empty:
        rejected_signal_df = rejected_signal_df.sort_values(["date", "stock_code"]).reset_index(drop=True)
    if not signal_trace_df.empty:
        signal_trace_df = signal_trace_df.sort_values(["date", "stock_code"]).reset_index(drop=True)
    return detail_df, signal_trace_df, rejected_signal_df, {**_empty_scan_stats(), **dict(stats)}


def scan_trade_candidates(
    all_data: pd.DataFrame, params: AnalysisParams
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    return _scan_trade_candidates_with_context(
        params, _build_scan_execution_context(all_data, params)
    )


def build_strategy_trades(
    candidate_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    if candidate_df.empty:
        return pd.DataFrame(columns=pd.Index(DETAIL_COLUMNS)), _empty_strategy_stats()

    strategy_records: list[dict[str, Any]] = []
    stats = _empty_strategy_stats()
    last_sell_date: pd.Timestamp | None = None
    current_nav = 1.0
    trade_no = 0

    sorted_candidates = candidate_df.sort_values(
        ["date", "stock_code", "sell_date"]
    ).reset_index(drop=True)

    for record in sorted_candidates.to_dict("records"):
        buy_date = pd.to_datetime(record["date"])
        sell_date = pd.to_datetime(record["sell_date"])

        if last_sell_date is not None and buy_date <= last_sell_date:
            stats["skipped_overlapping_position"] += 1
            continue

        trade_no += 1
        nav_before_trade = current_nav
        nav_after_trade = nav_before_trade * (
            1.0 + float(record["net_return_pct"]) / 100.0
        )

        strategy_record = {
            **record,
            "trade_no": trade_no,
            "nav_before_trade": nav_before_trade,
            "nav_after_trade": nav_after_trade,
        }
        strategy_records.append(strategy_record)

        current_nav = nav_after_trade
        last_sell_date = sell_date
        stats["executed_trades"] += 1

    strategy_df = pd.DataFrame(strategy_records, columns=pd.Index(DETAIL_COLUMNS))
    if strategy_df.empty:
        return strategy_df, stats

    stats["win_count"] = int(strategy_df["win_flag"].sum())
    stats["lose_count"] = int(len(strategy_df) - stats["win_count"])
    stats["strategy_win_rate_pct"] = float(strategy_df["win_flag"].mean() * 100.0)
    stats["final_net_value"] = float(strategy_df["nav_after_trade"].iloc[-1])
    stats["total_return_pct"] = (stats["final_net_value"] - 1.0) * 100.0
    stats["avg_holding_days"] = float(strategy_df["holding_days"].mean())
    stats["median_net_return_pct"] = float(strategy_df["net_return_pct"].median())
    stats["avg_mfe_pct"] = float(strategy_df["mfe_pct"].mean())
    stats["avg_mae_pct"] = float(strategy_df["mae_pct"].mean())
    if stats["avg_mae_pct"] == 0:
        stats["profit_risk_ratio"] = 0.0
    else:
        stats["profit_risk_ratio"] = stats["avg_mfe_pct"] / abs(stats["avg_mae_pct"])
    stats["trade_return_volatility_pct"] = float(
        strategy_df["net_return_pct"].std(ddof=0)
    )

    return strategy_df, stats


def build_equity_curve(
    all_data: pd.DataFrame, strategy_df: pd.DataFrame, params: AnalysisParams
) -> pd.DataFrame:
    start_ts, end_ts = _signal_window(params)
    is_daily = params.timeframe == "1d"

    if strategy_df.empty:
        market_dates = [start_ts]
        if not all_data.empty:
            market_dates = sorted(
                timestamp
                for timestamp in pd.to_datetime(all_data["date"])
                .dropna()
                .unique()
                .tolist()
                if start_ts <= pd.Timestamp(timestamp) <= end_ts
            ) or [start_ts]
        equity_df = pd.DataFrame(
            {
                "date": market_dates,
                "net_value": 1.0,
                "drawdown_pct": 0.0,
                "trade_no": pd.NA,
                "stock_code": "",
                "event": "",
            }
        )
        return equity_df.loc[:, EQUITY_COLUMNS]

    last_exit_ts = pd.to_datetime(strategy_df["sell_date"]).max()
    curve_end = max(end_ts, last_exit_ts)

    market_dates: list[pd.Timestamp] = []
    if not all_data.empty:
        market_dates = sorted(
            timestamp
            for timestamp in pd.to_datetime(all_data["date"]).dropna().unique().tolist()
            if start_ts <= pd.Timestamp(timestamp) <= curve_end
        )
    if not market_dates:
        market_dates = list(pd.date_range(start=start_ts, end=curve_end, freq="D"))

    close_lookup: dict[tuple[str, pd.Timestamp], float] = {}
    if not all_data.empty:
        rows = (
            all_data[["stock_code", "date", "close"]]
            .dropna()
            .itertuples(index=False, name=None)
        )
        for stock_code, date_value, close_value in rows:
            raw_date = cast(pd.Timestamp, pd.Timestamp(date_value))
            lookup_date = raw_date.normalize() if is_daily else raw_date
            close_lookup[(str(stock_code), lookup_date)] = float(close_value)

    trades = strategy_df.sort_values("date").to_dict("records")
    trade_index = 0
    active_trade: dict | None = None
    active_last_close: float | None = None

    current_nav = 1.0
    curve_records: list[dict] = []
    direction = "long" if params.gap_direction == "up" else "short"

    trade_state = {
        "remaining_weight": 0.0,
        "realized_value": 0.0,
        "fills": [],
        "fill_idx": 0,
    }

    for market_date in market_dates:
        raw_market_date = cast(pd.Timestamp, pd.Timestamp(market_date))
        current_date = raw_market_date.normalize() if is_daily else raw_market_date
        event_label = ""
        event_trade_no = pd.NA
        event_stock_code = ""

        if active_trade is None and trade_index < len(trades):
            next_trade = trades[trade_index]
            buy_date_raw = cast(pd.Timestamp, pd.Timestamp(next_trade["date"]))
            buy_date = buy_date_raw.normalize() if is_daily else buy_date_raw
            if current_date >= buy_date:
                active_trade = next_trade
                active_last_close = None
                trade_state["remaining_weight"] = 1.0
                trade_state["realized_value"] = 0.0
                trade_state["fills"] = list(active_trade.get("fills", []))
                trade_state["fill_idx"] = 0
                event_label = "buy"
                event_trade_no = int(active_trade["trade_no"])
                event_stock_code = str(active_trade["stock_code"])

        if active_trade is not None:
            stock_code = str(active_trade["stock_code"])
            buy_price = float(active_trade["buy_price"])
            nav_before_trade = float(active_trade["nav_before_trade"])
            sell_date = cast(pd.Timestamp, pd.Timestamp(active_trade["sell_date"]))
            sell_date = sell_date.normalize() if is_daily else sell_date

            day_close = close_lookup.get((stock_code, current_date), active_last_close)
            if day_close is not None:
                active_last_close = day_close

            # 按 fill 顺序处理当日成交
            while trade_state["fill_idx"] < len(trade_state["fills"]):
                fill = trade_state["fills"][trade_state["fill_idx"]]
                fill_date = cast(pd.Timestamp, pd.Timestamp(fill["sell_date"]))
                fill_date = fill_date.normalize() if is_daily else fill_date
                if fill_date != current_date:
                    break
                fill_weight = float(fill["weight"])
                fill_price = float(fill["sell_price"])
                trade_state["realized_value"] += fill_weight * fill_price
                trade_state["remaining_weight"] -= fill_weight
                fill_exit = str(fill.get("exit_type", "fill"))
                event_label = (
                    fill_exit if not event_label else f"{event_label}+{fill_exit}"
                )
                event_trade_no = int(active_trade["trade_no"])
                event_stock_code = stock_code
                trade_state["fill_idx"] += 1

            if day_close is not None:
                remaining_weight = max(0.0, float(trade_state["remaining_weight"]))
                realized_value = float(trade_state["realized_value"])

                if direction == "short":
                    effective_cover = realized_value + remaining_weight * day_close
                    holding_return = (buy_price - effective_cover) / buy_price
                else:
                    marked_value = realized_value + remaining_weight * day_close
                    holding_return = marked_value / buy_price - 1.0

                current_nav = nav_before_trade * (1.0 + holding_return)

            if current_date >= sell_date and trade_state["remaining_weight"] <= 1e-12:
                current_nav = float(active_trade["nav_after_trade"])
                if not event_label:
                    event_label = str(active_trade["exit_type"])
                    event_trade_no = int(active_trade["trade_no"])
                    event_stock_code = stock_code
                active_trade = None
                active_last_close = None
                trade_index += 1

        curve_records.append(
            {
                "date": pd.Timestamp(current_date),
                "net_value": float(current_nav),
                "trade_no": event_trade_no,
                "stock_code": event_stock_code,
                "event": event_label,
            }
        )

    equity_df = pd.DataFrame(
        curve_records,
        columns=pd.Index(["date", "net_value", "trade_no", "stock_code", "event"]),
    )
    equity_df["drawdown_pct"] = (
        equity_df["net_value"] / equity_df["net_value"].cummax() - 1.0
    ) * 100.0
    return equity_df.loc[:, EQUITY_COLUMNS]


def analyze_all_stocks(
    all_data: pd.DataFrame,
    params: AnalysisParams,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float]]:
    candidate_df, signal_trace_df, rejected_signal_df, scan_stats = _scan_trade_candidates_with_context(
        params, _build_scan_execution_context(all_data, params)
    )
    strategy_df, strategy_stats = build_strategy_trades(candidate_df)
    daily_df = build_daily_summary(strategy_df)
    equity_df = build_equity_curve(all_data, strategy_df, params)

    if not equity_df.empty:
        strategy_stats["max_drawdown_pct"] = abs(float(equity_df["drawdown_pct"].min()))

    combined_stats: dict[str, float] = {**scan_stats, **strategy_stats}
    return strategy_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, combined_stats


def _analyze_all_stocks_with_context(
    all_data: pd.DataFrame,
    params: AnalysisParams,
    context: ScanExecutionContext,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float]]:
    candidate_df, signal_trace_df, rejected_signal_df, scan_stats = _scan_trade_candidates_with_context(params, context)
    strategy_df, strategy_stats = build_strategy_trades(candidate_df)
    daily_df = build_daily_summary(strategy_df)
    equity_df = build_equity_curve(all_data, strategy_df, params)

    if not equity_df.empty:
        strategy_stats["max_drawdown_pct"] = abs(float(equity_df["drawdown_pct"].min()))

    combined_stats: dict[str, float] = {**scan_stats, **strategy_stats}
    return strategy_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, combined_stats


def run_parameter_scan(
    all_data: pd.DataFrame,
    params: AnalysisParams,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, float],
    dict[str, int | float],
]:
    scan_config = params.scan_config
    if not scan_config.enabled or not scan_config.axes:
        detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = analyze_all_stocks(all_data, params)
        return pd.DataFrame(), detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, {}

    field_names = [axis.field_name for axis in scan_config.axes]
    value_product = product(*(axis.values for axis in scan_config.axes))
    result_rows: list[dict[str, int | float]] = []
    best_payload: (
        tuple[
            pd.DataFrame,
            pd.DataFrame,
            pd.DataFrame,
            pd.DataFrame,
            pd.DataFrame,
            dict[str, float],
            dict[str, int | float],
        ]
        | None
    ) = None
    best_metric_value: float | None = None
    lower_is_better = scan_config.metric in {
        "max_drawdown_pct",
        "trade_return_volatility_pct",
        "avg_holding_days",
    }
    scan_context = _build_scan_execution_context(all_data, params)

    for scan_id, combo in enumerate(value_product, start=1):
        overrides = {
            field_name: value
            for field_name, value in zip(field_names, combo, strict=True)
        }
        scan_params = apply_scan_overrides(params, overrides)
        detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = _analyze_all_stocks_with_context(
            all_data, scan_params, scan_context
        )
        metric_value = float(stats.get(scan_config.metric, 0.0))
        row: dict[str, int | float] = {"scan_id": scan_id, **overrides}
        for column in SCAN_RESULT_COLUMNS:
            if column in {"scan_id", "rank"}:
                continue
            row[column] = float(stats.get(column, 0.0))
        result_rows.append(row)

        if best_payload is None:
            best_payload = (detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, overrides)
            best_metric_value = metric_value
            continue

        assert best_metric_value is not None
        if lower_is_better:
            should_replace = metric_value < best_metric_value
        else:
            should_replace = metric_value > best_metric_value
        if should_replace:
            best_payload = (detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, overrides)
            best_metric_value = metric_value

    scan_df = pd.DataFrame(result_rows)
    if scan_df.empty or best_payload is None:
        detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = analyze_all_stocks(all_data, params)
        return pd.DataFrame(), detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, {}

    sort_ascending = lower_is_better
    scan_df = scan_df.sort_values(
        [scan_config.metric, "scan_id"], ascending=[sort_ascending, True]
    ).reset_index(drop=True)
    scan_df["rank"] = range(1, len(scan_df) + 1)

    ordered_columns = (
        ["scan_id"]
        + field_names
        + [
            column
            for column in SCAN_RESULT_COLUMNS
            if column not in {"scan_id", "rank"}
        ]
        + ["rank"]
    )
    detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, overrides = best_payload
    ordered_scan_df = scan_df.loc[:, ordered_columns].copy()
    return ordered_scan_df, detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, overrides


def build_daily_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(columns=pd.Index(DAILY_COLUMNS))

    grouped = detail_df.groupby("date", as_index=False).agg(
        sample_count=("stock_code", "size"),
        win_count=("win_flag", "sum"),
        avg_net_return_pct=("net_return_pct", "mean"),
        median_net_return_pct=("net_return_pct", "median"),
        avg_holding_days=("holding_days", "mean"),
    )
    daily_df = pd.DataFrame(grouped).sort_values(by="date").reset_index(drop=True)

    daily_df["lose_count"] = daily_df["sample_count"] - daily_df["win_count"]
    daily_df["win_rate_pct"] = daily_df["win_count"] / daily_df["sample_count"] * 100.0

    ordered = daily_df.loc[:, DAILY_COLUMNS].copy()
    ordered["sample_count"] = ordered["sample_count"].astype(int)
    ordered["win_count"] = ordered["win_count"].astype(int)
    ordered["lose_count"] = ordered["lose_count"].astype(int)
    return ordered


def build_trade_behavior_overview(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(columns=pd.Index(TRADE_BEHAVIOR_COLUMNS))

    trade_count = len(detail_df)
    give_back_series = (
        detail_df["mfe_pct"].fillna(0.0) - detail_df["net_return_pct"].fillna(0.0)
    ).clip(lower=0.0)
    positive_mfe = detail_df["mfe_pct"].fillna(0.0) > 0
    capture_series = pd.Series(0.0, index=detail_df.index, dtype=float)
    capture_series.loc[positive_mfe] = (
        detail_df.loc[positive_mfe, "net_return_pct"].fillna(0.0)
        / detail_df.loc[positive_mfe, "mfe_pct"].replace(0.0, pd.NA)
        * 100.0
    ).fillna(0.0)

    overview = pd.DataFrame(
        [
            {
                "executed_trades": trade_count,
                "win_rate_pct": float(detail_df["win_flag"].fillna(0).mean() * 100.0),
                "avg_net_return_pct": float(detail_df["net_return_pct"].mean()),
                "median_net_return_pct": float(detail_df["net_return_pct"].median()),
                "avg_mfe_pct": float(detail_df["mfe_pct"].mean()),
                "avg_mae_pct": float(detail_df["mae_pct"].mean()),
                "avg_give_back_pct": float(give_back_series.mean()),
                "avg_mfe_capture_pct": float(capture_series.mean()),
                "trigger_fill_share_pct": float(
                    (detail_df["entry_fill_type"].fillna("") == "trigger").mean()
                    * 100.0
                ),
                "multi_fill_trade_share_pct": float(
                    (detail_df["fill_count"].fillna(0) > 1).mean() * 100.0
                ),
                "avg_profit_drawdown_ratio": float(
                    detail_df["profit_drawdown_ratio"].dropna().mean()
                )
                if bool(detail_df["profit_drawdown_ratio"].notna().any())
                else 0.0,
            }
        ],
        columns=pd.Index(TRADE_BEHAVIOR_COLUMNS),
    )
    return overview


def _episode_trade_mask(
    strategy_df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.Series:
    buy_dates = pd.to_datetime(strategy_df["date"])
    sell_dates = pd.to_datetime(strategy_df["sell_date"])
    return (buy_dates <= end_date) & (sell_dates >= start_date)


def build_drawdown_diagnostics(
    equity_df: pd.DataFrame, strategy_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_episodes = pd.DataFrame(columns=pd.Index(DD_EPISODE_COLUMNS))
    empty_contributors = pd.DataFrame(columns=pd.Index(DD_CONTRIBUTOR_COLUMNS))
    if equity_df.empty:
        return empty_episodes, empty_contributors

    drawdown_df = equity_df[["date", "drawdown_pct"]].copy()
    drawdown_df["date"] = pd.to_datetime(drawdown_df["date"])
    drawdown_df = (
        pd.DataFrame(drawdown_df).sort_values(by="date").reset_index(drop=True)
    )

    episodes: list[dict[str, Any]] = []
    in_episode = False
    start_idx = 0

    for idx, drawdown in enumerate(drawdown_df["drawdown_pct"].fillna(0.0)):
        if not in_episode and drawdown < 0:
            in_episode = True
            start_idx = idx
        is_recovered = in_episode and drawdown >= 0
        is_last = idx == len(drawdown_df) - 1
        if not in_episode:
            continue
        if not is_recovered and not is_last:
            continue

        end_idx = idx if is_recovered else idx
        episode_slice = drawdown_df.iloc[start_idx : end_idx + 1].copy()
        trough_local_idx = int(episode_slice["drawdown_pct"].idxmin())
        trough_row = drawdown_df.loc[trough_local_idx]
        start_date = cast(
            pd.Timestamp, pd.to_datetime(drawdown_df.loc[start_idx, "date"])
        )
        end_date = cast(pd.Timestamp, pd.to_datetime(drawdown_df.loc[end_idx, "date"]))
        trade_count = 0
        worst_trade_return_pct = 0.0
        dominant_entry_reason = ""

        if not strategy_df.empty:
            trade_mask = _episode_trade_mask(strategy_df, start_date, end_date)
            episode_trades = strategy_df.loc[trade_mask].copy()
            trade_count = int(len(episode_trades))
            if not episode_trades.empty:
                worst_trade_return_pct = float(episode_trades["net_return_pct"].min())
                dominant = (
                    episode_trades.groupby("entry_reason", dropna=False)[
                        "net_return_pct"
                    ]
                    .sum()
                    .sort_values()
                )
                dominant_entry_reason = (
                    str(dominant.index[0]) if not dominant.empty else ""
                )

        episodes.append(
            {
                "episode_no": len(episodes) + 1,
                "drawdown_start_date": start_date,
                "trough_date": cast(pd.Timestamp, pd.to_datetime(trough_row["date"])),
                "recovery_date": end_date if is_recovered else pd.NaT,
                "peak_to_trough_pct": abs(float(episode_slice["drawdown_pct"].min())),
                "underwater_bars": len(episode_slice),
                "trade_count": trade_count,
                "worst_trade_return_pct": worst_trade_return_pct,
                "dominant_entry_reason": dominant_entry_reason,
                "recovered_flag": bool(is_recovered),
            }
        )
        in_episode = False

    episodes_df = pd.DataFrame(episodes, columns=pd.Index(DD_EPISODE_COLUMNS))
    if episodes_df.empty or strategy_df.empty:
        return episodes_df, empty_contributors

    focus_episode = episodes_df.sort_values(
        ["peak_to_trough_pct", "episode_no"], ascending=[False, True]
    ).iloc[0]
    focus_start = cast(
        pd.Timestamp, pd.to_datetime(focus_episode["drawdown_start_date"])
    )
    focus_end_raw = focus_episode["recovery_date"]
    latest_trough = cast(pd.Timestamp, pd.to_datetime(episodes_df["trough_date"]).max())
    focus_end = (
        cast(pd.Timestamp, pd.to_datetime(focus_end_raw))
        if pd.notna(focus_end_raw)
        else latest_trough
    )
    contributor_mask = _episode_trade_mask(strategy_df, focus_start, focus_end)
    focus_trades = strategy_df.loc[contributor_mask].copy()
    if focus_trades.empty:
        return episodes_df, empty_contributors

    contributors_df = (
        focus_trades.groupby("entry_reason", dropna=False)
        .agg(
            trade_count=("stock_code", "size"),
            avg_net_return_pct=("net_return_pct", "mean"),
            total_net_return_pct=("net_return_pct", "sum"),
            avg_mae_pct=("mae_pct", "mean"),
            avg_mfe_pct=("mfe_pct", "mean"),
        )
        .reset_index()
        .sort_values(["total_net_return_pct", "trade_count"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return episodes_df, contributors_df.loc[:, DD_CONTRIBUTOR_COLUMNS]


def _partial_exit_rules(params: AnalysisParams, mode: str) -> list[PartialExitRule]:
    return [
        rule
        for rule in params.partial_exit_rules
        if rule.enabled and str(rule.mode) == mode
    ]


def _review_activation_thresholds(params: AnalysisParams) -> dict[str, float | None]:
    fixed_tp_threshold = (
        float(params.take_profit_pct) if params.enable_take_profit else None
    )

    profit_drawdown_rules = _partial_exit_rules(params, "profit_drawdown")
    if profit_drawdown_rules:
        profit_drawdown_threshold = min(
            float(rule.min_profit_to_activate_drawdown or 5.0)
            for rule in profit_drawdown_rules
        )
    elif params.enable_profit_drawdown_exit:
        profit_drawdown_threshold = float(
            params.min_profit_to_activate_profit_drawdown_pct
        )
    else:
        profit_drawdown_threshold = None

    atr_rules = _partial_exit_rules(params, "atr_trailing")
    if atr_rules or params.enable_atr_trailing_exit:
        atr_threshold = float(params.min_profit_to_activate_atr_trailing_pct)
    else:
        atr_threshold = None

    return {
        "fixed_tp": fixed_tp_threshold,
        "profit_drawdown": profit_drawdown_threshold,
        "atr_trailing": atr_threshold,
    }


def build_trade_anomaly_queue(
    detail_df: pd.DataFrame, params: AnalysisParams, limit: int = 15
) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame(columns=pd.Index(ANOMALY_QUEUE_COLUMNS))

    working = detail_df.copy()
    holding_days = working["holding_days"].fillna(0.0).clip(lower=1.0)
    working["give_back_pct"] = (
        working["mfe_pct"].fillna(0.0) - working["net_return_pct"].fillna(0.0)
    ).clip(lower=0.0)
    working["abs_mae_pct"] = working["mae_pct"].fillna(0.0).abs()
    working["holding_anchor_mfe_pct"] = working["mfe_pct"].fillna(0.0) / holding_days
    working["holding_anchor_mae_pct"] = working["abs_mae_pct"] / holding_days
    working["holding_anchor_give_back_pct"] = working["give_back_pct"] / holding_days

    thresholds = _review_activation_thresholds(params)

    anomaly_frames: list[pd.DataFrame] = []

    def _append_anomalies(
        source_df: pd.DataFrame,
        anomaly_type: str,
        severity_col: str,
        note_builder: Callable[[pd.Series], str],
        top_n: int = 5,
    ) -> None:
        if source_df.empty:
            return
        subset = source_df.nlargest(top_n, severity_col).copy()
        subset["anomaly_type"] = anomaly_type
        subset["severity_score"] = subset[severity_col].fillna(0.0)
        subset["anomaly_note"] = subset.apply(note_builder, axis=1)
        anomaly_frames.append(subset.loc[:, ANOMALY_QUEUE_COLUMNS])

    fixed_tp_threshold = thresholds["fixed_tp"]
    if fixed_tp_threshold is not None:
        fixed_tp_candidates = working.loc[
            (working["mfe_pct"].fillna(0.0) >= fixed_tp_threshold)
            & (~working["exit_reason"].fillna("").str.contains("take_profit"))
        ].copy()
        if not fixed_tp_candidates.empty:
            fixed_tp_candidates["activation_threshold_pct"] = fixed_tp_threshold
            fixed_tp_candidates["threshold_excess_pct"] = (
                fixed_tp_candidates["mfe_pct"].fillna(0.0) - fixed_tp_threshold
            ).clip(lower=0.0)
            fixed_tp_candidates = fixed_tp_candidates.loc[
                fixed_tp_candidates["threshold_excess_pct"]
                >= max(0.5, fixed_tp_threshold * 0.1)
            ].copy()
            fixed_tp_candidates["fixed_tp_severity"] = (
                fixed_tp_candidates["threshold_excess_pct"]
                + fixed_tp_candidates["holding_anchor_give_back_pct"]
            )
            _append_anomalies(
                fixed_tp_candidates,
                "fixed_tp_review",
                "fixed_tp_severity",
                lambda row: (
                    f"持有 {int(row['holding_days'])} 天内最大浮盈 {float(row['mfe_pct']):.2f}% ，已超过固定止盈激发阈值 "
                    f"{float(row['activation_threshold_pct']):.2f}% ，但最终由 {row['exit_reason']} 离场，仅实现 {float(row['net_return_pct']):.2f}%"
                ),
            )

    profit_drawdown_threshold = thresholds["profit_drawdown"]
    if profit_drawdown_threshold is not None:
        profit_drawdown_candidates = working.loc[
            (working["mfe_pct"].fillna(0.0) >= profit_drawdown_threshold)
            & (working["give_back_pct"] >= max(1.0, profit_drawdown_threshold * 0.5))
        ].copy()
        if not profit_drawdown_candidates.empty:
            profit_drawdown_candidates["activation_threshold_pct"] = (
                profit_drawdown_threshold
            )
            profit_drawdown_candidates["threshold_excess_pct"] = (
                profit_drawdown_candidates["mfe_pct"].fillna(0.0)
                - profit_drawdown_threshold
            ).clip(lower=0.0)
            profit_drawdown_candidates["profit_drawdown_severity"] = (
                profit_drawdown_candidates["holding_anchor_give_back_pct"]
            )
            _append_anomalies(
                profit_drawdown_candidates,
                "profit_drawdown_review",
                "profit_drawdown_severity",
                lambda row: (
                    f"持有 {int(row['holding_days'])} 天内最大浮盈 {float(row['mfe_pct']):.2f}% ，达到利润回撤激发阈值 "
                    f"{float(row['activation_threshold_pct']):.2f}% 后仍回吐 {float(row['give_back_pct']):.2f}% ，当前离场为 {row['exit_reason']}"
                ),
            )

    atr_threshold = thresholds["atr_trailing"]
    if atr_threshold is not None:
        atr_candidates = working.loc[
            (working["mfe_pct"].fillna(0.0) >= atr_threshold)
            & (working["give_back_pct"] >= max(1.0, atr_threshold * 0.5))
            & (~working["exit_reason"].fillna("").str.contains("atr_trailing"))
        ].copy()
        if not atr_candidates.empty:
            atr_candidates["activation_threshold_pct"] = atr_threshold
            atr_candidates["threshold_excess_pct"] = (
                atr_candidates["mfe_pct"].fillna(0.0) - atr_threshold
            ).clip(lower=0.0)
            atr_candidates["atr_trailing_severity"] = atr_candidates[
                "holding_anchor_give_back_pct"
            ]
            _append_anomalies(
                atr_candidates,
                "atr_trailing_review",
                "atr_trailing_severity",
                lambda row: (
                    f"持有 {int(row['holding_days'])} 天内最大浮盈 {float(row['mfe_pct']):.2f}% ，已超过 ATR 回撤诊断激发阈值 "
                    f"{float(row['activation_threshold_pct']):.2f}% ，但最终未以 ATR 跟踪止盈离场"
                ),
            )

    adverse_candidates = working.loc[
        working["holding_anchor_mae_pct"]
        >= max(
            0.8, float(params.stop_loss_pct) / max(float(params.time_stop_days), 1.0)
        )
    ].copy()
    if not adverse_candidates.empty:
        adverse_candidates["activation_threshold_pct"] = max(
            0.8, float(params.stop_loss_pct) / max(float(params.time_stop_days), 1.0)
        )
        adverse_candidates["threshold_excess_pct"] = (
            adverse_candidates["holding_anchor_mae_pct"]
            - adverse_candidates["activation_threshold_pct"]
        ).clip(lower=0.0)
        adverse_candidates["adverse_severity"] = adverse_candidates[
            "holding_anchor_mae_pct"
        ]
        _append_anomalies(
            adverse_candidates,
            "holding_period_adverse",
            "adverse_severity",
            lambda row: (
                f"持有 {int(row['holding_days'])} 天内最大不利波动 {float(row['mae_pct']):.2f}% ，折算日均不利波动 "
                f"{float(row['holding_anchor_mae_pct']):.2f}%"
            ),
        )

    stall_candidates = working.loc[
        (
            working["holding_days"].fillna(0.0)
            > working["holding_days"].fillna(0.0).median()
        )
        & (working["net_return_pct"].fillna(0.0).abs() <= 1.0)
    ].copy()
    if not stall_candidates.empty:
        stall_candidates["activation_threshold_pct"] = 1.0
        stall_candidates["threshold_excess_pct"] = 0.0
        stall_candidates["stall_severity"] = stall_candidates["holding_days"].fillna(
            0.0
        )
        _append_anomalies(
            stall_candidates,
            "long_hold_stall",
            "stall_severity",
            lambda row: (
                f"持有 {int(row['holding_days'])} 天但净收益仅 {float(row['net_return_pct']):.2f}% ，日均最大浮盈/不利波动分别为 "
                f"{float(row['holding_anchor_mfe_pct']):.2f}% / {float(row['holding_anchor_mae_pct']):.2f}%"
            ),
        )

    if not anomaly_frames:
        return pd.DataFrame(columns=pd.Index(ANOMALY_QUEUE_COLUMNS))

    anomaly_df = (
        pd.concat(anomaly_frames, ignore_index=True)
        .sort_values(["severity_score", "date"], ascending=[False, False])
        .head(limit)
        .reset_index(drop=True)
    )
    return anomaly_df.loc[:, ANOMALY_QUEUE_COLUMNS]


def _finalize_result_bundle(
    *,
    detail_df: pd.DataFrame,
    signal_trace_df: pd.DataFrame,
    rejected_signal_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    equity_df: pd.DataFrame,
    stats: dict[str, float],
    params: AnalysisParams,
    scan_df: pd.DataFrame | None = None,
    best_scan_overrides: dict[str, int | float] | None = None,
    per_stock_stats_df: pd.DataFrame | None = None,
    batch_backtest_mode: str = "combined",
) -> BacktestResultBundle:
    trade_behavior_df = build_trade_behavior_overview(detail_df)
    drawdown_episodes_df, drawdown_contributors_df = build_drawdown_diagnostics(
        equity_df, detail_df
    )
    anomaly_queue_df = build_trade_anomaly_queue(detail_df, params)
    return BacktestResultBundle(
        detail_df=detail_df,
        signal_trace_df=signal_trace_df,
        rejected_signal_df=rejected_signal_df,
        daily_df=daily_df,
        equity_df=equity_df,
        trade_behavior_df=trade_behavior_df,
        drawdown_episodes_df=drawdown_episodes_df,
        drawdown_contributors_df=drawdown_contributors_df,
        anomaly_queue_df=anomaly_queue_df,
        stats=stats,
        scan_df=scan_df if scan_df is not None else pd.DataFrame(),
        best_scan_overrides=best_scan_overrides or {},
        per_stock_stats_df=per_stock_stats_df
        if per_stock_stats_df is not None
        else pd.DataFrame(),
        batch_backtest_mode=batch_backtest_mode,
    )


def _run_combined_backtest(
    all_data: pd.DataFrame, params: AnalysisParams
) -> BacktestResultBundle:
    detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = analyze_all_stocks(all_data, params)
    return _finalize_result_bundle(
        detail_df=detail_df,
        signal_trace_df=signal_trace_df,
        rejected_signal_df=rejected_signal_df,
        daily_df=daily_df,
        equity_df=equity_df,
        stats=stats,
        params=params,
    )


def _run_scan_backtest(
    all_data: pd.DataFrame, params: AnalysisParams
) -> BacktestResultBundle:
    scan_df, detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, best_scan_overrides = (
        run_parameter_scan(all_data, params)
    )
    return _finalize_result_bundle(
        detail_df=detail_df,
        signal_trace_df=signal_trace_df,
        rejected_signal_df=rejected_signal_df,
        daily_df=daily_df,
        equity_df=equity_df,
        stats=stats,
        params=params,
        scan_df=scan_df,
        best_scan_overrides=best_scan_overrides,
    )


def _run_per_stock_backtest(
    all_data: pd.DataFrame, params: AnalysisParams
) -> BacktestResultBundle:
    batch_detail_frames: list[pd.DataFrame] = []
    batch_signal_trace_frames: list[pd.DataFrame] = []
    batch_rejected_frames: list[pd.DataFrame] = []
    batch_daily_frames: list[pd.DataFrame] = []
    batch_equity_frames: list[pd.DataFrame] = []
    batch_rows: list[dict[str, Any]] = []

    for stock_code in params.stock_codes:
        single_params = replace(
            params,
            stock_codes=(stock_code,),
            scan_config=replace(params.scan_config, enabled=False, axes=()),
        )
        stock_data = all_data.loc[
            all_data["stock_code"].astype(str) == str(stock_code)
        ].copy()
        single_detail_df, single_signal_trace_df, single_rejected_df, single_daily_df, single_equity_df, single_stats = (
            analyze_all_stocks(stock_data, single_params)
        )
        if not single_detail_df.empty:
            single_detail_df = single_detail_df.copy()
            single_detail_df["batch_stock_code"] = stock_code
            batch_detail_frames.append(single_detail_df)
        if not single_daily_df.empty:
            single_daily_df = single_daily_df.copy()
            single_daily_df["batch_stock_code"] = stock_code
            batch_daily_frames.append(single_daily_df)
        if not single_equity_df.empty:
            single_equity_df = single_equity_df.copy()
            single_equity_df["batch_stock_code"] = stock_code
            batch_equity_frames.append(single_equity_df)
        if not single_rejected_df.empty:
            single_rejected_df = single_rejected_df.copy()
            single_rejected_df["batch_stock_code"] = stock_code
            batch_rejected_frames.append(single_rejected_df)
        if not single_signal_trace_df.empty:
            single_signal_trace_df = single_signal_trace_df.copy()
            single_signal_trace_df["batch_stock_code"] = stock_code
            batch_signal_trace_frames.append(single_signal_trace_df)
        batch_rows.append(
            {
                "stock_code": stock_code,
                "signal_count": int(single_stats.get("signal_count", 0)),
                "executed_trades": int(single_stats.get("executed_trades", 0)),
                "total_return_pct": float(single_stats.get("total_return_pct", 0.0)),
                "strategy_win_rate_pct": float(
                    single_stats.get("strategy_win_rate_pct", 0.0)
                ),
                "max_drawdown_pct": float(single_stats.get("max_drawdown_pct", 0.0)),
                "final_net_value": float(single_stats.get("final_net_value", 1.0)),
                "avg_holding_days": float(single_stats.get("avg_holding_days", 0.0)),
                "profit_risk_ratio": float(single_stats.get("profit_risk_ratio", 0.0)),
                "avg_mfe_pct": float(single_stats.get("avg_mfe_pct", 0.0)),
                "avg_mae_pct": float(single_stats.get("avg_mae_pct", 0.0)),
                "trade_return_volatility_pct": float(
                    single_stats.get("trade_return_volatility_pct", 0.0)
                ),
            }
        )

    detail_df = (
        pd.concat(batch_detail_frames, ignore_index=True)
        if batch_detail_frames
        else pd.DataFrame()
    )
    daily_df = (
        pd.concat(batch_daily_frames, ignore_index=True)
        if batch_daily_frames
        else pd.DataFrame()
    )
    equity_df = (
        pd.concat(batch_equity_frames, ignore_index=True)
        if batch_equity_frames
        else pd.DataFrame()
    )
    rejected_signal_df = (
        pd.concat(batch_rejected_frames, ignore_index=True)
        if batch_rejected_frames
        else pd.DataFrame(columns=pd.Index(REJECTED_SIGNAL_COLUMNS))
    )
    signal_trace_df = (
        pd.concat(batch_signal_trace_frames, ignore_index=True)
        if batch_signal_trace_frames
        else pd.DataFrame(columns=pd.Index(SIGNAL_TRACE_COLUMNS))
    )
    per_stock_stats_df = pd.DataFrame(batch_rows)
    if per_stock_stats_df.empty:
        stats: dict[str, float] = {}
    else:
        stats = {
            "total_return_pct": float(per_stock_stats_df["total_return_pct"].mean()),
            "strategy_win_rate_pct": float(
                per_stock_stats_df["strategy_win_rate_pct"].mean()
            ),
            "max_drawdown_pct": float(per_stock_stats_df["max_drawdown_pct"].mean()),
            "executed_trades": float(per_stock_stats_df["executed_trades"].sum()),
            "signal_count": float(per_stock_stats_df["signal_count"].sum()),
            "core_signal_count": float(
                signal_trace_df["trigger_pass"].fillna(False).sum()
            )
            if not signal_trace_df.empty
            else 0.0,
            "rejected_signal_count": float(
                (
                    signal_trace_df["trigger_pass"].fillna(False)
                    & (~signal_trace_df["filter_pass"].fillna(False))
                ).sum()
            )
            if not signal_trace_df.empty
            else 0.0,
            "closed_trade_candidates": float(len(detail_df)),
        }

    return _finalize_result_bundle(
        detail_df=detail_df,
        signal_trace_df=signal_trace_df,
        rejected_signal_df=rejected_signal_df,
        daily_df=daily_df,
        equity_df=equity_df,
        stats=stats,
        params=params,
        per_stock_stats_df=per_stock_stats_df,
        batch_backtest_mode="per_stock",
    )


def run_backtest(
    all_data: pd.DataFrame,
    params: AnalysisParams,
    batch_mode: str = "combined",
) -> BacktestResultBundle:
    if batch_mode == "per_stock":
        return _run_per_stock_backtest(all_data, params)
    if params.scan_config.enabled:
        return _run_scan_backtest(all_data, params)
    return _run_combined_backtest(all_data, params)
