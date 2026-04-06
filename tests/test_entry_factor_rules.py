from __future__ import annotations

from typing import Any

import pandas as pd

from models import AnalysisParams
from rules import apply_gap_filters, simulate_trade


def make_params(**overrides: Any) -> AnalysisParams:
    base: dict[str, Any] = {
        "data_source_type": "local_parquet",
        "db_path": "",
        "table_name": None,
        "column_overrides": {},
        "excel_sheet_name": None,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "stock_codes": (),
        "gap_direction": "up",
        "entry_factor": "gap",
        "gap_entry_mode": "strict_break",
        "gap_pct": 2.0,
        "max_gap_filter_pct": 9.9,
        "trend_breakout_lookback": 2,
        "vcb_range_lookback": 2,
        "vcb_breakout_lookback": 2,
        "use_ma_filter": False,
        "fast_ma_period": 5,
        "slow_ma_period": 20,
        "time_stop_days": 1,
        "time_stop_target_pct": -100.0,
        "stop_loss_pct": 50.0,
        "take_profit_pct": 5.0,
        "enable_take_profit": False,
        "enable_profit_drawdown_exit": False,
        "profit_drawdown_pct": 40.0,
        "enable_ma_exit": False,
        "exit_ma_period": 10,
        "ma_exit_batches": 2,
        "partial_exit_enabled": False,
        "partial_exit_count": 2,
        "partial_exit_rules": (),
        "buy_cost_pct": 0.0,
        "sell_cost_pct": 0.0,
        "buy_slippage_pct": 0.0,
        "sell_slippage_pct": 0.0,
        "time_exit_mode": "force_close",
    }
    base.update(overrides)
    return AnalysisParams(**base)


def make_df(rows: list[tuple[float, float, float, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=len(rows), freq="D"),
            "stock_code": ["000001.SZ"] * len(rows),
            "open": [row[0] for row in rows],
            "high": [row[1] for row in rows],
            "low": [row[2] for row in rows],
            "close": [row[3] for row in rows],
            "volume": [row[4] for row in rows],
        }
    )


def make_df_with_board_ma(
    rows: list[tuple[float, float, float, float, float]],
    board20: list[float],
    board50: list[float],
) -> pd.DataFrame:
    df = make_df(rows)
    df["board_ma_ratio_20"] = board20
    df["board_ma_ratio_50"] = board50
    return df


def test_entry_factor_default_is_gap() -> None:
    assert getattr(make_params(), "entry_factor", None) == "gap"


def test_explicit_gap_entry_factor_matches_default_gap_behavior() -> None:
    df = make_df([(100, 106, 99, 100, 1000), (103, 104, 102, 103, 1000)])
    default_result = apply_gap_filters(df, make_params())
    explicit_gap_result = apply_gap_filters(df, make_params(entry_factor="gap"))

    assert bool(default_result.loc[1, "is_signal"]) == bool(
        explicit_gap_result.loc[1, "is_signal"]
    )


def test_trend_breakout_long_trigger_uses_only_prior_bars() -> None:
    df = make_df(
        [
            (9.5, 10.0, 9.0, 9.8, 1000),
            (10.5, 11.0, 10.0, 10.8, 1000),
            (11.5, 50.0, 11.0, 12.0, 1000),
            (12.0, 12.5, 11.5, 12.2, 1000),
        ]
    )
    result = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=2),
    )

    assert float(result.loc[2, "entry_trigger_price"]) == 11.0
    assert bool(result.loc[2, "is_signal"])


def test_trend_breakout_short_trigger_uses_only_prior_bars() -> None:
    df = make_df(
        [
            (10.0, 11.0, 9.0, 10.0, 1000),
            (9.5, 10.0, 8.0, 9.0, 1000),
            (7.9, 8.5, 1.0, 7.8, 1000),
            (7.8, 8.0, 7.0, 7.2, 1000),
        ]
    )
    result = apply_gap_filters(
        df,
        make_params(
            gap_direction="down",
            entry_factor="trend_breakout",
            trend_breakout_lookback=2,
        ),
    )

    assert float(result.loc[2, "entry_trigger_price"]) == 8.0
    assert bool(result.loc[2, "is_signal"])


def test_vcb_breakout_requires_contraction_gate() -> None:
    df = make_df(
        [
            (100.0, 105.0, 100.0, 104.0, 1000),
            (104.0, 111.0, 104.0, 110.0, 1000),
            (112.0, 113.0, 109.0, 112.0, 1000),
            (114.0, 116.0, 113.0, 115.0, 1000),
            (115.0, 116.0, 114.0, 115.0, 1000),
        ]
    )
    result = apply_gap_filters(
        df,
        make_params(
            entry_factor="volatility_contraction_breakout",
            vcb_range_lookback=2,
            vcb_breakout_lookback=2,
        ),
    )

    assert not bool(result.loc[2, "is_signal"])
    assert bool(result.loc[3, "is_signal"])
    assert not bool(result.loc[2, "is_contraction"])
    assert bool(result.loc[3, "is_contraction"])


def test_atr_filter_blocks_signal_when_prior_atr_pct_is_below_minimum() -> None:
    df = make_df([(100.0, 101.0, 99.0, 100.0, 1000), (104.0, 105.0, 103.0, 104.0, 1000)])
    result = apply_gap_filters(
        df,
        make_params(
            entry_factor="gap",
            enable_atr_filter=True,
            atr_filter_period=1,
            min_atr_filter_pct=2.5,
            max_atr_filter_pct=5.0,
        ),
    )

    assert float(result.loc[1, "atr_filter_pct"]) == 2.0
    assert not bool(result.loc[1, "is_signal"])


def test_board_ma_filter_blocks_long_signal_when_ratio_below_threshold() -> None:
    df = make_df_with_board_ma(
        [(100.0, 101.0, 99.0, 100.0, 1000), (104.0, 105.0, 103.0, 104.0, 1000)],
        [40.0, 45.0],
        [35.0, 38.0],
    )
    result = apply_gap_filters(
        df,
        make_params(
            entry_factor="gap",
            enable_board_ma_filter=True,
            board_ma_filter_line="20",
            board_ma_filter_threshold=50.0,
        ),
    )

    assert float(result.loc[1, "board_ma_signal_value"]) == 45.0
    assert not bool(result.loc[1, "is_signal"])


def test_board_ma_filter_allows_long_signal_when_ratio_meets_threshold() -> None:
    df = make_df_with_board_ma(
        [(100.0, 101.0, 99.0, 100.0, 1000), (104.0, 105.0, 103.0, 104.0, 1000)],
        [40.0, 55.0],
        [35.0, 38.0],
    )
    result = apply_gap_filters(
        df,
        make_params(
            entry_factor="gap",
            enable_board_ma_filter=True,
            board_ma_filter_line="20",
            board_ma_filter_threshold=50.0,
        ),
    )

    assert bool(result.loc[1, "is_signal"])


def test_board_ma_filter_blocks_short_signal_when_ratio_above_threshold() -> None:
    df = make_df_with_board_ma(
        [(100.0, 101.0, 99.0, 100.0, 1000), (95.0, 96.0, 94.0, 95.0, 1000)],
        [60.0, 60.0],
        [52.0, 52.0],
    )
    result = apply_gap_filters(
        df,
        make_params(
            gap_direction="down",
            entry_factor="gap",
            gap_pct=2.0,
            enable_board_ma_filter=True,
            board_ma_filter_line="50",
            board_ma_filter_operator="<=",
            board_ma_filter_threshold=40.0,
        ),
    )

    assert not bool(result.loc[1, "is_signal"])


def test_board_ma_filter_can_use_less_equal_for_long_signal() -> None:
    df = make_df_with_board_ma(
        [(100.0, 101.0, 99.0, 100.0, 1000), (104.0, 105.0, 103.0, 104.0, 1000)],
        [40.0, 35.0],
        [35.0, 30.0],
    )
    result = apply_gap_filters(
        df,
        make_params(
            entry_factor="gap",
            enable_board_ma_filter=True,
            board_ma_filter_line="20",
            board_ma_filter_operator="<=",
            board_ma_filter_threshold=40.0,
        ),
    )

    assert bool(result.loc[1, "is_signal"])


def test_trend_breakout_long_open_fill_uses_open_price() -> None:
    df = make_df(
        [
            (98.0, 100.0, 95.0, 99.0, 1000),
            (105.0, 106.0, 104.0, 105.0, 1000),
            (104.0, 104.0, 103.0, 103.0, 1000),
        ]
    )
    enriched = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
    )
    trade, reason = simulate_trade(
        enriched, 1, make_params(entry_factor="trend_breakout")
    )

    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "trend_breakout"
    assert float(trade["entry_trigger_price"]) == 100.0
    assert trade["entry_fill_type"] == "open"
    assert float(trade["buy_price"]) == 105.0
    assert int(trade["holding_days"]) >= 1


def test_trend_breakout_long_trigger_fill_uses_trigger_price() -> None:
    df = make_df(
        [
            (98.0, 100.0, 95.0, 99.0, 1000),
            (99.0, 101.0, 98.0, 100.0, 1000),
            (100.0, 101.0, 99.0, 100.0, 1000),
        ]
    )
    enriched = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
    )
    trade, reason = simulate_trade(
        enriched, 1, make_params(entry_factor="trend_breakout")
    )

    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "trend_breakout"
    assert float(trade["entry_trigger_price"]) == 100.0
    assert trade["entry_fill_type"] == "trigger"
    assert float(trade["buy_price"]) == 100.0


def test_trend_breakout_long_no_fill_is_skipped() -> None:
    df = make_df(
        [
            (98.0, 100.0, 95.0, 99.0, 1000),
            (99.0, 99.5, 98.0, 99.0, 1000),
            (99.0, 99.5, 98.0, 99.0, 1000),
        ]
    )
    enriched = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
    )
    trade, reason = simulate_trade(
        enriched, 1, make_params(entry_factor="trend_breakout")
    )

    assert trade is None
    assert reason == "entry_not_filled"


def test_trend_breakout_short_open_fill_uses_open_price() -> None:
    df = make_df(
        [
            (100.0, 101.0, 95.0, 99.0, 1000),
            (94.0, 95.0, 93.0, 94.0, 1000),
            (95.0, 96.0, 94.0, 95.0, 1000),
        ]
    )
    params = make_params(
        gap_direction="down", entry_factor="trend_breakout", trend_breakout_lookback=1
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 1, params, direction="short")

    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "trend_breakout"
    assert float(trade["entry_trigger_price"]) == 95.0
    assert trade["entry_fill_type"] == "open"
    assert float(trade["buy_price"]) == 94.0


def test_trend_breakout_short_trigger_fill_uses_trigger_price() -> None:
    df = make_df(
        [
            (100.0, 101.0, 95.0, 99.0, 1000),
            (97.0, 98.0, 94.0, 96.0, 1000),
            (96.0, 97.0, 95.0, 96.0, 1000),
        ]
    )
    params = make_params(
        gap_direction="down", entry_factor="trend_breakout", trend_breakout_lookback=1
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 1, params, direction="short")

    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "trend_breakout"
    assert float(trade["entry_trigger_price"]) == 95.0
    assert trade["entry_fill_type"] == "trigger"
    assert float(trade["buy_price"]) == 95.0


def test_trend_breakout_locked_bar_is_unfillable() -> None:
    df = make_df(
        [
            (98.0, 100.0, 95.0, 99.0, 1000),
            (100.0, 100.0, 100.0, 100.0, 1000),
            (100.0, 101.0, 99.0, 100.0, 1000),
        ]
    )
    enriched = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
    )
    trade, reason = simulate_trade(
        enriched, 1, make_params(entry_factor="trend_breakout")
    )

    assert trade is None
    assert reason == "locked_bar_unfillable"


def test_trend_breakout_zero_volume_is_unfillable() -> None:
    df = make_df(
        [
            (98.0, 100.0, 95.0, 99.0, 1000),
            (105.0, 106.0, 104.0, 105.0, 0.0),
            (104.0, 104.0, 103.0, 103.0, 1000),
        ]
    )
    enriched = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
    )
    trade, reason = simulate_trade(
        enriched, 1, make_params(entry_factor="trend_breakout")
    )

    assert trade is None
    assert reason == "locked_bar_unfillable"


def test_board_ma_exit_triggers_whole_position_exit_for_long_trade() -> None:
    df = make_df_with_board_ma(
        [
            (100.0, 101.0, 99.0, 100.0, 1000),
            (104.0, 106.0, 103.0, 105.0, 1000),
            (105.0, 106.0, 104.0, 105.0, 1000),
        ],
        [60.0, 55.0, 35.0],
        [58.0, 56.0, 34.0],
    )
    params = make_params(
        entry_factor="gap",
        enable_board_ma_exit=True,
        board_ma_exit_line="20",
        board_ma_exit_threshold=40.0,
        enable_take_profit=False,
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 1, params)

    assert reason is None
    assert trade is not None
    assert trade["exit_type"] == "board_ma_exit"
    assert trade["board_ma_value"] == 35.0


def test_board_ma_exit_triggers_whole_position_exit_for_short_trade() -> None:
    df = make_df_with_board_ma(
        [
            (100.0, 101.0, 95.0, 99.0, 1000),
            (94.0, 95.0, 93.0, 94.0, 1000),
            (93.0, 96.0, 92.0, 95.0, 1000),
        ],
        [30.0, 35.0, 65.0],
        [32.0, 34.0, 66.0],
    )
    params = make_params(
        gap_direction="down",
        entry_factor="gap",
        gap_pct=2.0,
        enable_board_ma_exit=True,
        board_ma_exit_line="20",
        board_ma_exit_operator=">=",
        board_ma_exit_threshold=60.0,
        enable_take_profit=False,
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 1, params, direction="short")

    assert reason is None
    assert trade is not None
    assert trade["exit_type"] == "board_ma_exit"
    assert trade["board_ma_value"] == 65.0


def test_board_ma_exit_can_use_greater_equal_for_long_trade() -> None:
    df = make_df_with_board_ma(
        [
            (100.0, 101.0, 99.0, 100.0, 1000),
            (104.0, 106.0, 103.0, 105.0, 1000),
            (105.0, 106.0, 104.0, 105.0, 1000),
        ],
        [60.0, 55.0, 65.0],
        [58.0, 56.0, 64.0],
    )
    params = make_params(
        entry_factor="gap",
        enable_board_ma_exit=True,
        board_ma_exit_line="20",
        board_ma_exit_operator=">=",
        board_ma_exit_threshold=60.0,
        enable_take_profit=False,
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 1, params)

    assert reason is None
    assert trade is not None
    assert trade["exit_type"] == "board_ma_exit"
    assert trade["board_ma_value"] == 65.0


def test_trend_breakout_missing_volume_remains_fillable() -> None:
    df = make_df(
        [
            (98.0, 100.0, 95.0, 99.0, 1000),
            (105.0, 106.0, 104.0, 105.0, float("nan")),
            (104.0, 104.0, 103.0, 103.0, 1000),
        ]
    )
    enriched = apply_gap_filters(
        df,
        make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
    )
    trade, reason = simulate_trade(
        enriched, 1, make_params(entry_factor="trend_breakout")
    )

    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "trend_breakout"
    assert float(trade["entry_trigger_price"]) == 100.0
    assert trade["entry_fill_type"] == "open"


def test_candle_run_long_signal_uses_prior_bars_and_open_entry() -> None:
    df = make_df(
        [
            (100.0, 101.5, 99.8, 101.2, 1000),
            (101.2, 103.0, 101.0, 102.8, 1000),
            (103.0, 104.0, 102.7, 103.5, 1000),
            (103.5, 104.0, 102.8, 103.0, 1000),
        ]
    )
    params = make_params(
        entry_factor="candle_run",
        candle_run_length=2,
        candle_run_min_body_pct=1.0,
        candle_run_total_move_pct=2.0,
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 2, params)

    assert not bool(enriched.loc[1, "is_signal"])
    assert bool(enriched.loc[2, "is_signal"])
    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "candle_run"
    assert trade["entry_fill_type"] == "open"
    assert pd.isna(trade["entry_trigger_price"])
    assert float(trade["buy_price"]) == 103.0


def test_candle_run_short_signal_uses_prior_bars_and_open_entry() -> None:
    df = make_df(
        [
            (100.0, 100.2, 98.5, 98.8, 1000),
            (98.8, 99.0, 96.8, 97.0, 1000),
            (96.8, 97.0, 95.8, 96.2, 1000),
            (96.2, 96.5, 95.7, 96.0, 1000),
        ]
    )
    params = make_params(
        gap_direction="down",
        entry_factor="candle_run",
        candle_run_length=2,
        candle_run_min_body_pct=1.0,
        candle_run_total_move_pct=2.0,
    )
    enriched = apply_gap_filters(df, params)
    trade, reason = simulate_trade(enriched, 2, params, direction="short")

    assert bool(enriched.loc[2, "is_signal"])
    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "candle_run"
    assert trade["entry_fill_type"] == "open"
    assert float(trade["buy_price"]) == 96.8


def test_candle_run_acceleration_requires_non_decreasing_body_strength() -> None:
    accelerating_df = make_df(
        [
            (100.0, 101.2, 99.8, 101.0, 1000),
            (101.0, 103.4, 100.9, 103.2, 1000),
            (103.2, 104.0, 103.0, 103.6, 1000),
        ]
    )
    decelerating_df = make_df(
        [
            (100.0, 102.3, 99.8, 102.0, 1000),
            (102.0, 103.2, 101.8, 103.0, 1000),
            (103.0, 103.6, 102.8, 103.3, 1000),
        ]
    )
    params = make_params(
        entry_factor="candle_run_acceleration",
        candle_run_length=2,
        candle_run_min_body_pct=0.5,
        candle_run_total_move_pct=2.0,
    )

    accelerating = apply_gap_filters(accelerating_df, params)
    decelerating = apply_gap_filters(decelerating_df, params)

    assert bool(accelerating.loc[2, "is_signal"])
    assert not bool(decelerating.loc[2, "is_signal"])


def test_eshb_setup_detects_valid_30m_pattern_and_populates_diagnostics() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02 09:30:00",
                    "2024-01-02 10:00:00",
                    "2024-01-02 10:30:00",
                    "2024-01-02 11:00:00",
                    "2024-01-02 11:30:00",
                ]
            ),
            "stock_code": ["000001.SZ"] * 5,
            "open": [100.0, 100.0, 104.5, 104.6, 104.8],
            "high": [101.0, 105.0, 104.8, 104.9, 105.0],
            "low": [99.0, 99.8, 104.2, 104.3, 104.7],
            "close": [100.0, 104.5, 104.6, 104.7, 104.9],
            "volume": [100.0, 300.0, 120.0, 110.0, 130.0],
        }
    )
    params = make_params(
        entry_factor="early_surge_high_base",
        timeframe="30m",
        eshb_open_window_bars=3,
        eshb_base_min_bars=2,
        eshb_base_max_bars=4,
        eshb_surge_min_pct=3.0,
        eshb_max_base_pullback_pct=2.5,
        eshb_max_base_range_pct=1.0,
        eshb_max_anchor_breaks=0,
        eshb_max_anchor_break_depth_pct=0.5,
        eshb_min_open_volume_ratio=1.5,
        eshb_trigger_buffer_pct=0.0,
    )

    enriched = apply_gap_filters(df, params)

    assert bool(enriched.loc[4, "is_signal"])
    assert float(enriched.loc[4, "eshb_base_bars"]) == 2
    assert float(enriched.loc[4, "eshb_surge_pct"]) >= 3.0
    assert pd.notna(enriched.loc[4, "entry_trigger_price"])


def test_eshb_setup_rejects_invalid_base_pullback_shape() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02 09:30:00",
                    "2024-01-02 10:00:00",
                    "2024-01-02 10:30:00",
                    "2024-01-02 11:00:00",
                ]
            ),
            "stock_code": ["000001.SZ"] * 4,
            "open": [100.0, 100.0, 104.5, 101.0],
            "high": [101.0, 105.0, 104.8, 101.5],
            "low": [99.0, 99.8, 100.0, 100.5],
            "close": [100.0, 104.5, 101.2, 101.3],
            "volume": [100.0, 300.0, 120.0, 110.0],
        }
    )
    params = make_params(
        entry_factor="early_surge_high_base",
        timeframe="30m",
        eshb_open_window_bars=3,
        eshb_base_min_bars=1,
        eshb_base_max_bars=3,
        eshb_surge_min_pct=3.0,
        eshb_max_base_pullback_pct=1.0,
        eshb_min_open_volume_ratio=1.0,
    )

    enriched = apply_gap_filters(df, params)

    assert not bool(enriched.loc[3, "is_signal"])


def test_eshb_trade_preserves_intraday_timestamp_and_trigger_metadata() -> None:
    execution_df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02 11:40:00",
                    "2024-01-02 11:45:00",
                    "2024-01-02 11:50:00",
                ]
            ),
            "stock_code": ["000001.SZ"] * 3,
            "open": [105.1, 105.2, 105.5],
            "high": [105.4, 105.6, 105.8],
            "low": [104.9, 105.0, 105.3],
            "close": [105.2, 105.5, 105.7],
            "volume": [200.0, 210.0, 220.0],
        }
    )
    execution_df["prev_close"] = execution_df["close"].shift(1)
    execution_df["prev_high"] = execution_df["high"].shift(1)
    execution_df["prev_low"] = execution_df["low"].shift(1)
    execution_df["gap_pct_vs_prev_close"] = (
        execution_df["open"] / execution_df["prev_close"] - 1.0
    ) * 100.0
    execution_df["entry_factor"] = "early_surge_high_base"
    execution_df["entry_trigger_price"] = [float("nan"), 105.0, float("nan")]

    params = make_params(
        entry_factor="early_surge_high_base",
        timeframe="30m",
        time_stop_days=1,
        time_stop_target_pct=-100.0,
        time_exit_mode="force_close",
    )
    trade, reason = simulate_trade(execution_df, 1, params)

    assert reason is None
    assert trade is not None
    assert trade["entry_factor"] == "early_surge_high_base"
    assert trade["entry_fill_type"] == "open"
    assert float(trade["entry_trigger_price"]) == 105.0
    assert str(trade["buy_date"]).startswith("2024-01-02 11:45:00")
