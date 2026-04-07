from __future__ import annotations

from pathlib import Path

import pandas as pd

from data.services.indicator_catalog_service import (
    build_registry_manifest,
    list_indicator_symbols,
    summarize_indicator_availability,
    upsert_indicator_inventory_row,
)


def test_build_registry_manifest_includes_board_ma_contract() -> None:
    manifest = build_registry_manifest()

    assert "indicator_key" in manifest.columns
    board_ma = manifest.loc[manifest["indicator_key"] == "board_ma"].iloc[0]
    assert board_ma["source_type"] == "tdx_formula_local"
    assert "board_ma_ratio_20" in str(board_ma["output_columns"])
    assert bool(board_ma["allow_filter"]) is True
    assert bool(board_ma["allow_exit"]) is True


def test_indicator_inventory_supports_symbol_listing_and_availability_summary(
    tmp_path: Path,
) -> None:
    import pytest

    pytest.importorskip("pyarrow")
    inventory_path = tmp_path / "indicator_inventory.parquet"
    upsert_indicator_inventory_row(
        {
            "indicator_key": "board_ma",
            "display_name": "板块均线",
            "source_type": "tdx_formula_local",
            "symbol": "000001.SZ",
            "timeframe": "1d",
            "adjust": "qfq",
            "file_path": str(tmp_path / "board_ma" / "000001.SZ.parquet"),
            "row_count": 2,
            "non_null_columns": "board_ma_ratio_20, board_ma_ratio_50",
            "min_date": pd.Timestamp("2024-01-02"),
            "max_date": pd.Timestamp("2024-01-03"),
            "last_success_at": pd.Timestamp("2024-01-03"),
            "last_update_status": "success",
            "last_error_message": "",
            "updated_at": pd.Timestamp("2024-01-03"),
        },
        inventory_path=inventory_path,
    )

    assert list_indicator_symbols("board_ma", inventory_path=inventory_path) == ["000001.SZ"]

    summary = summarize_indicator_availability(limit=10, inventory_path=inventory_path)
    assert isinstance(summary, pd.DataFrame)
    assert summary.iloc[0]["indicator_key"] == "board_ma"
