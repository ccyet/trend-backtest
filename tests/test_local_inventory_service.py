from __future__ import annotations

from pathlib import Path

import pandas as pd

from data.services import local_inventory_service as inventory


def test_upsert_inventory_row_replaces_same_symbol_timeframe_adjust(monkeypatch, tmp_path: Path):
    import pytest

    pytest.importorskip("pyarrow")
    inventory_path = tmp_path / "local_inventory.parquet"
    monkeypatch.setattr(inventory, "METADATA_DIR", tmp_path)

    inventory.upsert_inventory_row(
        {
            "symbol": "000001.SZ",
            "timeframe": "1d",
            "adjust": "qfq",
            "file_path": "a.parquet",
            "row_count": 10,
            "min_date": pd.Timestamp("2024-01-01"),
            "max_date": pd.Timestamp("2024-01-10"),
            "file_size_bytes": 100,
            "last_success_at": pd.Timestamp("2024-01-10 12:00:00"),
            "last_update_status": "success",
            "last_error_message": "",
            "updated_at": pd.Timestamp("2024-01-10 12:00:00"),
        },
        inventory_path=inventory_path,
    )
    inventory.upsert_inventory_row(
        {
            "symbol": "000001.SZ",
            "timeframe": "1d",
            "adjust": "qfq",
            "file_path": "b.parquet",
            "row_count": 12,
            "min_date": pd.Timestamp("2024-01-01"),
            "max_date": pd.Timestamp("2024-01-12"),
            "file_size_bytes": 120,
            "last_success_at": pd.Timestamp("2024-01-12 12:00:00"),
            "last_update_status": "success",
            "last_error_message": "",
            "updated_at": pd.Timestamp("2024-01-12 12:00:00"),
        },
        inventory_path=inventory_path,
    )

    loaded = inventory.load_inventory(inventory_path)
    assert len(loaded) == 1
    assert loaded.iloc[0]["file_path"] == "b.parquet"
    assert int(loaded.iloc[0]["row_count"]) == 12


def test_list_local_symbols_by_timeframe_filters_adjust_and_success(monkeypatch, tmp_path: Path):
    import pytest

    pytest.importorskip("pyarrow")
    inventory_path = tmp_path / "local_inventory.parquet"
    monkeypatch.setattr(inventory, "METADATA_DIR", tmp_path)

    pd.DataFrame(
        [
            {"symbol": "000001.SZ", "timeframe": "30m", "adjust": "qfq", "file_path": "a", "row_count": 1, "min_date": pd.Timestamp("2024-01-01"), "max_date": pd.Timestamp("2024-01-01"), "file_size_bytes": 1, "last_success_at": pd.Timestamp("2024-01-01"), "last_update_status": "success", "last_error_message": "", "updated_at": pd.Timestamp("2024-01-01")},
            {"symbol": "000002.SZ", "timeframe": "30m", "adjust": "hfq", "file_path": "b", "row_count": 1, "min_date": pd.Timestamp("2024-01-01"), "max_date": pd.Timestamp("2024-01-01"), "file_size_bytes": 1, "last_success_at": pd.Timestamp("2024-01-01"), "last_update_status": "success", "last_error_message": "", "updated_at": pd.Timestamp("2024-01-01")},
            {"symbol": "000003.SZ", "timeframe": "30m", "adjust": "qfq", "file_path": "c", "row_count": 1, "min_date": pd.Timestamp("2024-01-01"), "max_date": pd.Timestamp("2024-01-01"), "file_size_bytes": 1, "last_success_at": pd.NaT, "last_update_status": "failed", "last_error_message": "x", "updated_at": pd.Timestamp("2024-01-01")},
        ]
    ).to_parquet(inventory_path, index=False)

    result = inventory.list_local_symbols_by_timeframe("30m", adjust="qfq", inventory_path=inventory_path)
    assert result == ["000001.SZ"]
