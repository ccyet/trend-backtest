from __future__ import annotations

from models import (
    AnalysisParams,
    ImportedIndicatorRule,
    ParamScanConfig,
    validate_params,
)


def make_params(**overrides):
    base = dict(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-10",
        stock_codes=("000001.SZ",),
        gap_direction="up",
        entry_factor="gap",
        gap_pct=2.0,
        max_gap_filter_pct=8.0,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=5,
        time_stop_target_pct=1.0,
        stop_loss_pct=3.0,
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
        buy_cost_pct=0.03,
        sell_cost_pct=0.13,
        time_exit_mode="strict",
        scan_config=ParamScanConfig(
            enabled=False, axes=(), metric="total_return_pct", max_combinations=25
        ),
    )
    base.update(overrides)
    return AnalysisParams(**base)


def test_imported_indicator_filter_master_switch_disables_rules() -> None:
    params = make_params(
        enable_imported_indicator_filter=False,
        imported_indicator_filters=(
            ImportedIndicatorRule(
                enabled=True,
                indicator_key="board_ma",
                column="board_ma_ratio_20",
                operator=">=",
                threshold=50.0,
                priority=1,
            ),
        ),
    )

    assert params.effective_imported_indicator_filters == ()


def test_imported_indicator_exit_master_switch_disables_rules() -> None:
    params = make_params(
        enable_imported_indicator_exit=False,
        imported_indicator_exits=(
            ImportedIndicatorRule(
                enabled=True,
                indicator_key="board_ma",
                column="board_ma_ratio_20",
                operator=">=",
                threshold=50.0,
                priority=1,
            ),
        ),
    )

    assert params.effective_imported_indicator_exits == ()


def test_validate_params_rejects_timeframe_outside_strategy_capability() -> None:
    params = make_params(entry_factor="gap", timeframe="30m")
    errors, warnings = validate_params(params)
    assert "gap 仅支持 timeframe=1d。" in errors
    assert warnings == []
