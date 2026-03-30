from __future__ import annotations

from collections import Counter
from itertools import product
import json
from typing import Any, cast

import pandas as pd

from data_loader import load_local_parquet_data
from models import AnalysisParams, apply_scan_overrides
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
    "fills",
    "fill_count",
    "fill_detail_json",
    "entry_factor",
    "entry_reason",
    "entry_trigger_price",
    "entry_fill_type",
    "exit_reason",
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


def _empty_scan_stats() -> dict[str, int]:
    return {
        "signal_count": 0,
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


def _prepare_execution_frame(stock_df: pd.DataFrame, params: AnalysisParams) -> pd.DataFrame:
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


def scan_trade_candidates(
    all_data: pd.DataFrame, params: AnalysisParams
) -> tuple[pd.DataFrame, dict[str, int]]:
    if all_data.empty:
        return pd.DataFrame(columns=pd.Index(BASE_DETAIL_COLUMNS)), _empty_scan_stats()

    start_ts, end_ts = _signal_window(params)

    detail_records: list[dict[str, Any]] = []
    stats = Counter()

    execution_data: pd.DataFrame | None = None
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
        )
        for stock_code, stock_df in execution_data.groupby("stock_code", sort=True):
            execution_by_code[str(stock_code)] = _prepare_execution_frame(stock_df, params)

    for _, stock_df in all_data.groupby("stock_code", sort=True):
        enriched = apply_gap_filters(stock_df, params)
        signal_mask = enriched["is_signal"] & enriched["date"].between(start_ts, end_ts)
        signal_indices = enriched.index[signal_mask].tolist()
        stats["signal_count"] += len(signal_indices)

        for signal_idx in signal_indices:
            direction = "long" if params.gap_direction == "up" else "short"
            if params.entry_factor == ESHB_ENTRY_FACTOR:
                setup_row = enriched.iloc[signal_idx]
                stock_code = str(setup_row["stock_code"])
                execution_df = execution_by_code.get(stock_code)
                if execution_df is None or execution_df.empty:
                    stats["skipped_no_exit"] += 1
                    continue

                trigger_price = float(setup_row["entry_trigger_price"])
                setup_time = pd.Timestamp(setup_row["date"])
                candidate_exec = execution_df.loc[execution_df["date"] > setup_time]
                trigger_hits = candidate_exec.loc[candidate_exec["high"] >= trigger_price]
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
                            execution_df.loc[entry_idx, "entry_factor"] = ESHB_ENTRY_FACTOR
                            execution_df.loc[entry_idx, "entry_trigger_price"] = trigger_price
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
            trade["gross_return_pct"] = (
                trade["sell_price"] / float(trade["buy_price"]) - 1.0
            ) * 100.0
            trade["fill_count"] = len(fills)
            trade["fill_detail_json"] = json.dumps(fills, ensure_ascii=False)
            detail_records.append(trade)
            stats["closed_trade_candidates"] += 1

    detail_df = pd.DataFrame(detail_records, columns=pd.Index(BASE_DETAIL_COLUMNS))
    if detail_df.empty:
        return detail_df, {**_empty_scan_stats(), **dict(stats)}

    detail_df = detail_df.sort_values(["date", "stock_code", "sell_date"]).reset_index(
        drop=True
    )
    return detail_df, {**_empty_scan_stats(), **dict(stats)}


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
            sell_date = cast(
                pd.Timestamp, pd.Timestamp(active_trade["sell_date"])
            )
            sell_date = sell_date.normalize() if is_daily else sell_date

            day_close = close_lookup.get((stock_code, current_date), active_last_close)
            if day_close is not None:
                active_last_close = day_close

            # 按 fill 顺序处理当日成交
            while trade_state["fill_idx"] < len(trade_state["fills"]):
                fill = trade_state["fills"][trade_state["fill_idx"]]
                fill_date = cast(
                    pd.Timestamp, pd.Timestamp(fill["sell_date"])
                )
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
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float]]:
    candidate_df, scan_stats = scan_trade_candidates(all_data, params)
    strategy_df, strategy_stats = build_strategy_trades(candidate_df)
    daily_df = build_daily_summary(strategy_df)
    equity_df = build_equity_curve(all_data, strategy_df, params)

    if not equity_df.empty:
        strategy_stats["max_drawdown_pct"] = abs(float(equity_df["drawdown_pct"].min()))

    combined_stats: dict[str, float] = {**scan_stats, **strategy_stats}
    return strategy_df, daily_df, equity_df, combined_stats


def run_parameter_scan(
    all_data: pd.DataFrame,
    params: AnalysisParams,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, float],
    dict[str, int | float],
]:
    scan_config = params.scan_config
    if not scan_config.enabled or not scan_config.axes:
        detail_df, daily_df, equity_df, stats = analyze_all_stocks(all_data, params)
        return pd.DataFrame(), detail_df, daily_df, equity_df, stats, {}

    field_names = [axis.field_name for axis in scan_config.axes]
    value_product = product(*(axis.values for axis in scan_config.axes))
    result_rows: list[dict[str, int | float]] = []
    best_payload: (
        tuple[
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

    for scan_id, combo in enumerate(value_product, start=1):
        overrides = {
            field_name: value
            for field_name, value in zip(field_names, combo, strict=True)
        }
        scan_params = apply_scan_overrides(params, overrides)
        detail_df, daily_df, equity_df, stats = analyze_all_stocks(
            all_data, scan_params
        )
        metric_value = float(stats.get(scan_config.metric, 0.0))
        row: dict[str, int | float] = {"scan_id": scan_id, **overrides}
        for column in SCAN_RESULT_COLUMNS:
            if column in {"scan_id", "rank"}:
                continue
            row[column] = float(stats.get(column, 0.0))
        result_rows.append(row)

        if best_payload is None:
            best_payload = (detail_df, daily_df, equity_df, stats, overrides)
            best_metric_value = metric_value
            continue

        assert best_metric_value is not None
        if lower_is_better:
            should_replace = metric_value < best_metric_value
        else:
            should_replace = metric_value > best_metric_value
        if should_replace:
            best_payload = (detail_df, daily_df, equity_df, stats, overrides)
            best_metric_value = metric_value

    scan_df = pd.DataFrame(result_rows)
    if scan_df.empty or best_payload is None:
        detail_df, daily_df, equity_df, stats = analyze_all_stocks(all_data, params)
        return pd.DataFrame(), detail_df, daily_df, equity_df, stats, {}

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
    detail_df, daily_df, equity_df, stats, overrides = best_payload
    ordered_scan_df = scan_df.loc[:, ordered_columns].copy()
    return ordered_scan_df, detail_df, daily_df, equity_df, stats, overrides


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
