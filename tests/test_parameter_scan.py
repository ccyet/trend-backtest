from __future__ import annotations

from typing import Any

import pandas as pd

from analyzer import run_parameter_scan
from models import (
    ENTRY_FACTORS,
    FACTOR_SCAN_ELIGIBLE_FIELDS,
    AnalysisParams,
    ParamScanAxis,
    ParamScanConfig,
    PartialExitRule,
    apply_scan_overrides,
    validate_params,
)


def make_params(scan_config: ParamScanConfig, **overrides: Any) -> AnalysisParams:
    base: dict[str, Any] = dict(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-31",
        stock_codes=(),
        gap_direction="up",
        entry_factor="gap",
        gap_entry_mode="strict_break",
        gap_pct=2.0,
        max_gap_filter_pct=9.9,
        trend_breakout_lookback=20,
        vcb_range_lookback=7,
        vcb_breakout_lookback=20,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=1,
        time_stop_target_pct=-10.0,
        stop_loss_pct=50.0,
        take_profit_pct=5.0,
        enable_take_profit=True,
        enable_profit_drawdown_exit=False,
        profit_drawdown_pct=40.0,
        enable_ma_exit=False,
        exit_ma_period=10,
        ma_exit_batches=2,
        partial_exit_enabled=False,
        partial_exit_count=2,
        partial_exit_rules=(),
        buy_cost_pct=0.0,
        sell_cost_pct=0.0,
        buy_slippage_pct=0.0,
        sell_slippage_pct=0.0,
        time_exit_mode="strict",
        scan_config=scan_config,
    )
    base.update(overrides)
    return AnalysisParams(**base)


def make_market_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "stock_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "open": [100.0, 104.0, 109.0],
            "high": [101.0, 106.0, 110.0],
            "low": [99.0, 103.0, 108.0],
            "close": [100.0, 105.0, 109.0],
            "volume": [1000.0, 1200.0, 900.0],
        }
    )


def test_parameter_scan_ranks_best_combo_and_keeps_best_outputs() -> None:
    scan_config = ParamScanConfig(
        enabled=True,
        axes=(ParamScanAxis(field_name="gap_pct", values=(2.0, 5.0)),),
        metric="total_return_pct",
        max_combinations=25,
    )
    scan_df, detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats, best_overrides = run_parameter_scan(
        make_market_data(), make_params(scan_config)
    )
    assert len(scan_df) == 2
    assert int(scan_df.iloc[0]["rank"]) == 1
    assert float(scan_df.iloc[0]["gap_pct"]) == 2.0
    assert not detail_df.empty
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty
    assert not daily_df.empty
    assert not equity_df.empty
    assert float(best_overrides["gap_pct"]) == 2.0
    assert float(stats["total_return_pct"]) > 0.0


def test_parameter_scan_validation_rejects_oversized_grid() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(field_name="gap_pct", values=(1.0, 2.0, 3.0, 4.0, 5.0)),
                ParamScanAxis(
                    field_name="stop_loss_pct", values=(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
                ),
            ),
            metric="total_return_pct",
            max_combinations=20,
        )
    )
    errors, _ = validate_params(params)
    assert any("组合数超出上限" in error for error in errors)


def test_parameter_scan_validation_accepts_trend_breakout_lookback_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(field_name="trend_breakout_lookback", values=(10, 20)),
            ),
            metric="total_return_pct",
            max_combinations=25,
        ),
        entry_factor="trend_breakout",
    )
    errors, _ = validate_params(params)
    assert not errors


def test_parameter_scan_validation_rejects_irrelevant_axis_for_factor() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(ParamScanAxis(field_name="gap_pct", values=(1.0, 2.0)),),
            metric="total_return_pct",
            max_combinations=25,
        ),
        entry_factor="trend_breakout",
    )
    errors, _ = validate_params(params)
    assert any("不支持扫描字段" in error and "gap_pct" in error for error in errors)


def test_validation_rejects_gap_entry_mode_when_factor_is_not_gap() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        entry_factor="trend_breakout",
        gap_entry_mode="open_vs_prev_close_threshold",
    )
    errors, _ = validate_params(params)
    assert any("gap_entry_mode" in error for error in errors)


def test_validation_accepts_atr_filter_and_whole_position_atr_exit() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        enable_atr_filter=True,
        atr_filter_period=14,
        min_atr_filter_pct=1.0,
        max_atr_filter_pct=5.0,
        enable_atr_trailing_exit=True,
        atr_trailing_period=14,
        atr_trailing_multiplier=2.5,
    )
    errors, _ = validate_params(params)
    assert not errors


def test_parameter_scan_validation_accepts_atr_filter_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(ParamScanAxis(field_name="atr_filter_period", values=(7, 14)),),
            metric="total_return_pct",
            max_combinations=25,
        ),
        enable_atr_filter=True,
    )
    errors, _ = validate_params(params)
    assert not errors


def test_parameter_scan_validation_accepts_atr_trailing_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(
                    field_name="atr_trailing_multiplier", values=(2.0, 3.0)
                ),
            ),
            metric="total_return_pct",
            max_combinations=25,
        ),
        enable_atr_trailing_exit=True,
    )
    errors, _ = validate_params(params)
    assert not errors


def test_validation_accepts_partial_atr_trailing_rule() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            PartialExitRule(True, 50.0, "atr_trailing", 1, atr_period=14, atr_multiplier=2.0),
            PartialExitRule(True, 50.0, "fixed_tp", 2, target_profit_pct=4.0),
        ),
    )
    errors, _ = validate_params(params)
    assert not errors


def test_parameter_scan_validation_accepts_partial_atr_trailing_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(ParamScanAxis(field_name="partial_rule_1_atr_period", values=(7, 14)),),
            metric="total_return_pct",
            max_combinations=25,
        ),
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            PartialExitRule(True, 50.0, "atr_trailing", 1, atr_period=10, atr_multiplier=2.0),
            PartialExitRule(True, 50.0, "fixed_tp", 2, target_profit_pct=4.0),
        ),
    )
    errors, _ = validate_params(params)
    assert not errors


def test_apply_scan_overrides_updates_atr_fields() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        enable_atr_filter=True,
        atr_filter_period=14,
        enable_atr_trailing_exit=True,
        atr_trailing_period=14,
        atr_trailing_multiplier=3.0,
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            PartialExitRule(True, 50.0, "atr_trailing", 1, atr_period=10, atr_multiplier=2.0),
            PartialExitRule(True, 50.0, "fixed_tp", 2, target_profit_pct=4.0),
        ),
    )
    updated = apply_scan_overrides(
        params,
        {
            "atr_filter_period": 21,
            "atr_trailing_multiplier": 2.5,
            "partial_rule_1_atr_period": 7,
            "partial_rule_1_atr_multiplier": 1.5,
        },
    )

    assert updated.atr_filter_period == 21
    assert updated.atr_trailing_multiplier == 2.5
    assert updated.partial_exit_rules[0].atr_period == 7
    assert updated.partial_exit_rules[0].atr_multiplier == 1.5


def test_required_lookback_days_considers_factor_lookback() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        entry_factor="trend_breakout",
        trend_breakout_lookback=30,
    )
    assert params.required_lookback_days == 35


def test_parameter_scan_validation_accepts_candle_run_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(ParamScanAxis(field_name="candle_run_length", values=(2, 3)),),
            metric="total_return_pct",
            max_combinations=25,
        ),
        entry_factor="candle_run",
    )
    errors, _ = validate_params(params)
    assert not errors


def test_parameter_scan_validation_accepts_candle_run_acceleration_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(field_name="candle_run_min_body_pct", values=(0.5, 1.0)),
            ),
            metric="total_return_pct",
            max_combinations=25,
        ),
        entry_factor="candle_run_acceleration",
    )
    errors, _ = validate_params(params)
    assert not errors


def test_selected_strategy_variants_are_present_in_model_contract() -> None:
    assert "candle_run" in ENTRY_FACTORS
    assert "candle_run_acceleration" in ENTRY_FACTORS
    assert "early_surge_high_base" in ENTRY_FACTORS
    assert "eshb_trigger_buffer_pct" in FACTOR_SCAN_ELIGIBLE_FIELDS["early_surge_high_base"]
    assert (
        FACTOR_SCAN_ELIGIBLE_FIELDS["candle_run"]
        == FACTOR_SCAN_ELIGIBLE_FIELDS["candle_run_acceleration"]
    )


def test_validation_accepts_intraday_timeframes() -> None:
    params = make_params(ParamScanConfig(enabled=False), timeframe="30m")
    errors, _ = validate_params(params)
    assert not errors

    params_15m = make_params(ParamScanConfig(enabled=False), timeframe="15m")
    errors_15m, _ = validate_params(params_15m)
    assert not errors_15m

    params_5m = make_params(ParamScanConfig(enabled=False), timeframe="5m")
    errors_5m, _ = validate_params(params_5m)
    assert not errors_5m


def test_validation_rejects_invalid_timeframe_value() -> None:
    params = make_params(ParamScanConfig(enabled=False), timeframe="2m")
    errors, _ = validate_params(params)
    assert any("timeframe" in error for error in errors)


def test_validation_enforces_eshb_intraday_contract() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        entry_factor="early_surge_high_base",
        timeframe="30m",
        data_source_type="local_parquet",
    )
    errors, _ = validate_params(params)
    assert not errors

    invalid_timeframe = make_params(
        ParamScanConfig(enabled=False),
        entry_factor="early_surge_high_base",
        timeframe="1d",
    )
    invalid_errors, _ = validate_params(invalid_timeframe)
    assert any("timeframe=30m" in error for error in invalid_errors)


def test_parameter_scan_validation_accepts_partial_rule_fixed_tp_axis() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(
                    field_name="partial_rule_1_target_profit_pct", values=(2.0, 3.0)
                ),
            ),
            metric="total_return_pct",
            max_combinations=25,
        ),
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            PartialExitRule(True, 50.0, "fixed_tp", 1, target_profit_pct=2.0),
            PartialExitRule(True, 50.0, "ma_exit", 2, ma_period=5),
        ),
    )
    errors, _ = validate_params(params)
    assert not errors


def test_parameter_scan_validation_rejects_partial_rule_axis_when_partial_exit_disabled() -> (
    None
):
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(
                    field_name="partial_rule_1_target_profit_pct", values=(2.0, 3.0)
                ),
            ),
            metric="total_return_pct",
            max_combinations=25,
        ),
        partial_exit_enabled=False,
    )
    errors, _ = validate_params(params)
    assert any("依赖分批止盈" in error for error in errors)


def test_parameter_scan_validation_rejects_partial_rule_axis_on_mode_mismatch() -> None:
    params = make_params(
        ParamScanConfig(
            enabled=True,
            axes=(
                ParamScanAxis(
                    field_name="partial_rule_1_target_profit_pct", values=(2.0, 3.0)
                ),
            ),
            metric="total_return_pct",
            max_combinations=25,
        ),
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            PartialExitRule(True, 50.0, "ma_exit", 1, ma_period=5),
            PartialExitRule(True, 50.0, "fixed_tp", 2, target_profit_pct=4.0),
        ),
    )
    errors, _ = validate_params(params)
    assert any("仅支持 mode=fixed_tp" in error for error in errors)


def test_apply_scan_overrides_updates_nested_partial_rules() -> None:
    params = make_params(
        ParamScanConfig(enabled=False),
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            PartialExitRule(True, 50.0, "fixed_tp", 1, target_profit_pct=2.0),
            PartialExitRule(True, 50.0, "ma_exit", 2, ma_period=5),
        ),
    )
    updated = apply_scan_overrides(
        params,
        {
            "partial_rule_1_target_profit_pct": 3.5,
            "partial_rule_2_ma_period": 8,
            "stop_loss_pct": 6.0,
        },
    )

    assert updated.partial_exit_rules[0].target_profit_pct == 3.5
    assert updated.partial_exit_rules[1].ma_period == 8
    assert updated.stop_loss_pct == 6.0
    assert params.partial_exit_rules[0].target_profit_pct == 2.0
