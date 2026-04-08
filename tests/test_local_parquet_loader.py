from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_loader import load_local_parquet_data, resolve_local_data_root
from models import AnalysisParams, validate_params


def test_validate_params_local_parquet_and_adjust():
    params = AnalysisParams(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-31",
        stock_codes=(),
        gap_direction="up",
        gap_pct=2.0,
        max_gap_filter_pct=9.9,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=2,
        time_stop_target_pct=1.0,
        stop_loss_pct=5.0,
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
        time_exit_mode="strict",
        local_data_root="data/market/daily",
        adjust="qfq",
    )
    errors, _ = validate_params(params)
    assert not errors


def test_validate_params_adjust_invalid():
    params = AnalysisParams(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-31",
        stock_codes=(),
        gap_direction="up",
        gap_pct=2.0,
        max_gap_filter_pct=9.9,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=2,
        time_stop_target_pct=1.0,
        stop_loss_pct=5.0,
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
        time_exit_mode="strict",
        local_data_root="data/market/daily",
        adjust="bad",
    )
    errors, _ = validate_params(params)
    assert any("复权方式" in msg for msg in errors)


def test_local_parquet_loader_missing_dir(tmp_path: Path):
    try:
        load_local_parquet_data("2024-01-01", "2024-01-31", local_data_root=str(tmp_path / "missing"), adjust="qfq")
        raised = False
    except FileNotFoundError:
        raised = True
    assert raised


def test_local_parquet_loader_reads_and_filters(tmp_path: Path):
    import pytest

    pytest.importorskip("pyarrow")
    root = tmp_path / "daily"
    qfq = root / "qfq"
    qfq.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "symbol": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "open": [1.0, 2.0, 3.0],
            "high": [1.1, 2.1, 3.1],
            "low": [0.9, 1.9, 2.9],
            "close": [1.0, 2.0, 3.0],
            "volume": [10, 20, 30],
            "board_ma_ratio_20": [40.0, 50.0, 60.0],
            "board_ma_ratio_50": [30.0, 35.0, 40.0],
        }
    ).to_parquet(qfq / "000001.SZ.parquet", index=False)

    out = load_local_parquet_data(
        "2024-01-02",
        "2024-01-02",
        stock_codes=("000001.SZ",),
        local_data_root=str(root),
        adjust="qfq",
    )
    assert not out.empty
    assert out["stock_code"].nunique() == 1
    assert out["stock_code"].iloc[0] == "000001.SZ"
    assert out["date"].min() <= pd.Timestamp("2024-01-02") <= out["date"].max()


def test_resolve_local_data_root_switches_default_daily_root_for_intraday_socket(tmp_path: Path):
    resolved = resolve_local_data_root(str(tmp_path / "data" / "market" / "daily"), "30m")
    assert resolved == tmp_path / "data" / "market" / "30m"


def test_resolve_local_data_root_switches_default_daily_root_for_1m_socket(tmp_path: Path):
    resolved = resolve_local_data_root(str(tmp_path / "data" / "market" / "daily"), "1m")
    assert resolved == tmp_path / "data" / "market" / "1m"


def test_local_parquet_loader_uses_timeframe_resolved_root(tmp_path: Path):
    import pytest

    pytest.importorskip("pyarrow")
    intraday_root = tmp_path / "market" / "30m"
    qfq = intraday_root / "qfq"
    qfq.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": ["2024-01-02 10:00:00", "2024-01-02 10:30:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [10.0, 10.2],
            "high": [10.3, 10.5],
            "low": [9.9, 10.1],
            "close": [10.2, 10.4],
            "volume": [100, 120],
        }
    ).to_parquet(qfq / "000001.SZ.parquet", index=False)

    out = load_local_parquet_data(
        "2024-01-02",
        "2024-01-02",
        stock_codes=("000001.SZ",),
        local_data_root=str(tmp_path / "market" / "daily"),
        adjust="qfq",
        timeframe="30m",
    )

    assert len(out) == 2
    assert out["stock_code"].iloc[0] == "000001.SZ"
    assert list(out["date"]) == [
        pd.Timestamp("2024-01-02 10:00:00"),
        pd.Timestamp("2024-01-02 10:30:00"),
    ]


def test_local_parquet_loader_prefers_inventory_symbols_when_stock_pool_empty(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    intraday_root = tmp_path / "market" / "5m"
    qfq = intraday_root / "qfq"
    qfq.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": ["2024-01-02 10:00:00"],
            "symbol": ["000777.SZ"],
            "open": [10.0],
            "high": [10.1],
            "low": [9.9],
            "close": [10.0],
            "volume": [10],
        }
    ).to_parquet(qfq / "000777.SZ.parquet", index=False)

    monkeypatch.setattr(
        "data_loader.list_local_symbols_by_timeframe",
        lambda timeframe, adjust=None: ["000777.SZ"] if timeframe == "5m" and adjust == "qfq" else [],
    )

    out = load_local_parquet_data(
        "2024-01-02",
        "2024-01-02",
        stock_codes=(),
        local_data_root=str(tmp_path / "market" / "daily"),
        adjust="qfq",
        timeframe="5m",
    )

    assert out["stock_code"].tolist() == ["000777.SZ"]


def test_local_parquet_loader_reads_1m_from_resolved_root(tmp_path: Path):
    import pytest

    pytest.importorskip("pyarrow")
    intraday_root = tmp_path / "market" / "1m"
    qfq = intraday_root / "qfq"
    qfq.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": ["2024-01-02 09:30:00", "2024-01-02 09:31:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [10.0, 10.1],
            "high": [10.2, 10.3],
            "low": [9.9, 10.0],
            "close": [10.1, 10.2],
            "volume": [100, 110],
        }
    ).to_parquet(qfq / "000001.SZ.parquet", index=False)

    out = load_local_parquet_data(
        "2024-01-02",
        "2024-01-02",
        stock_codes=("000001.SZ",),
        local_data_root=str(tmp_path / "market" / "daily"),
        adjust="qfq",
        timeframe="1m",
    )

    assert len(out) == 2
    assert list(out["date"]) == [
        pd.Timestamp("2024-01-02 09:30:00"),
        pd.Timestamp("2024-01-02 09:31:00"),
    ]


def test_local_parquet_loader_merges_imported_board_ma_indicator(tmp_path: Path):
    import pytest

    pytest.importorskip("pyarrow")
    root = tmp_path / "daily"
    qfq = root / "qfq"
    qfq.mkdir(parents=True, exist_ok=True)
    indicator_dir = tmp_path / "indicators" / "board_ma"
    indicator_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume": [10, 20],
        }
    ).to_parquet(qfq / "000001.SZ.parquet", index=False)

    pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "symbol": ["000001.SZ", "000001.SZ"],
            "board_ma_ratio_20": [55.0, 60.0],
            "board_ma_ratio_50": [45.0, 50.0],
        }
    ).to_parquet(indicator_dir / "000001.SZ.parquet", index=False)

    out = load_local_parquet_data(
        "2024-01-02",
        "2024-01-03",
        stock_codes=("000001.SZ",),
        local_data_root=str(root),
        adjust="qfq",
        indicator_keys=("board_ma",),
        indicator_root=str(tmp_path / "indicators"),
    )

    assert out["board_ma_ratio_20"].tolist() == [55.0, 60.0]
    assert out["board_ma_ratio_50"].tolist() == [45.0, 50.0]


def test_validate_params_accepts_board_ma_controls() -> None:
    params = AnalysisParams(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-31",
        stock_codes=(),
        gap_direction="up",
        gap_pct=2.0,
        max_gap_filter_pct=9.9,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=2,
        time_stop_target_pct=1.0,
        stop_loss_pct=5.0,
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
        time_exit_mode="strict",
        local_data_root="data/market/daily",
        adjust="qfq",
        enable_board_ma_filter=True,
        board_ma_filter_line="20",
        board_ma_filter_operator=">=",
        board_ma_filter_threshold=55.0,
        enable_board_ma_exit=True,
        board_ma_exit_line="50",
        board_ma_exit_operator="<=",
        board_ma_exit_threshold=35.0,
    )
    errors, _ = validate_params(params)
    assert not errors


def test_validate_params_accepts_generic_imported_indicator_filter() -> None:
    params = AnalysisParams(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-31",
        stock_codes=(),
        gap_direction="up",
        gap_pct=2.0,
        max_gap_filter_pct=9.9,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=2,
        time_stop_target_pct=1.0,
        stop_loss_pct=5.0,
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
        time_exit_mode="strict",
        local_data_root="data/market/daily",
        adjust="qfq",
        enable_imported_indicator_filter=True,
        imported_indicator_filter_key="custom_indicator",
        imported_indicator_filter_column="custom_strength",
        imported_indicator_filter_operator=">=",
        imported_indicator_filter_threshold=50.0,
    )
    errors, _ = validate_params(params)
    assert not errors


def test_validate_params_accepts_generic_imported_indicator_exit() -> None:
    params = AnalysisParams(
        data_source_type="local_parquet",
        db_path="",
        table_name=None,
        column_overrides={},
        excel_sheet_name=None,
        start_date="2024-01-01",
        end_date="2024-01-31",
        stock_codes=(),
        gap_direction="up",
        gap_pct=2.0,
        max_gap_filter_pct=9.9,
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        time_stop_days=2,
        time_stop_target_pct=1.0,
        stop_loss_pct=5.0,
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
        time_exit_mode="strict",
        local_data_root="data/market/daily",
        adjust="qfq",
        enable_imported_indicator_exit=True,
        imported_indicator_exit_key="custom_indicator",
        imported_indicator_exit_column="custom_exit",
        imported_indicator_exit_operator="<=",
        imported_indicator_exit_threshold=10.0,
    )
    errors, _ = validate_params(params)
    assert not errors
