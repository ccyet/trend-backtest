from __future__ import annotations

from pathlib import Path

import pandas as pd

import scripts.update_data as upd


def test_update_one_symbol_export_excel(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(upd, "UPDATE_LOG_PATH", tmp_path / "data" / "market" / "metadata" / "update_log.parquet")
    monkeypatch.setattr(upd, "upsert_inventory_row", lambda row: row)

    fake_df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume": [10.0, 20.0],
            "amount": [100.0, 200.0],
        }
    )

    monkeypatch.setattr(upd.AkshareProvider, "fetch_bars", staticmethod(lambda **kwargs: fake_df))
    monkeypatch.setattr(upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ"))

    local_root = tmp_path / "data" / "market" / "daily"
    upd.update_one_symbol("000001.SZ", "2024-01-01", "2024-01-02", "qfq", local_root, export_excel=True)

    excel_path = tmp_path / "data" / "market" / "exports" / "qfq" / "000001.SZ.xlsx"
    assert excel_path.exists()


def test_update_one_symbol_skips_rewrite_when_incremental_fetch_empty(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(upd, "UPDATE_LOG_PATH", tmp_path / "data" / "market" / "metadata" / "update_log.parquet")
    recorded_rows: list[dict[str, object]] = []
    monkeypatch.setattr(upd, "upsert_inventory_row", lambda row: recorded_rows.append(row))
    monkeypatch.setattr(upd.AkshareProvider, "fetch_bars", staticmethod(lambda **kwargs: pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "amount"])))
    monkeypatch.setattr(upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ"))

    local_root = tmp_path / "data" / "market" / "30m"
    parquet_path = local_root / "qfq" / "000001.SZ.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    existing_df = pd.DataFrame(
        {
            "date": ["2024-01-02 10:00:00", "2024-01-02 10:30:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume": [10.0, 20.0],
            "amount": [100.0, 200.0],
        }
    )
    existing_df.to_parquet(parquet_path, index=False)
    original_mtime = parquet_path.stat().st_mtime_ns

    upd.update_one_symbol(
        "000001.SZ",
        "2024-01-01",
        "2024-01-03",
        "qfq",
        local_root,
        export_excel=False,
        timeframe="30m",
    )

    assert parquet_path.stat().st_mtime_ns == original_mtime
    assert recorded_rows
    assert recorded_rows[0]["timeframe"] == "30m"


def test_update_one_symbol_writes_timeframe_into_update_log(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(upd, "UPDATE_LOG_PATH", tmp_path / "data" / "market" / "metadata" / "update_log.parquet")
    monkeypatch.setattr(upd, "upsert_inventory_row", lambda row: row)
    fake_df = pd.DataFrame(
        {
            "date": ["2024-01-02 10:00:00"],
            "symbol": ["000001.SZ"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "volume": [10.0],
            "amount": [100.0],
        }
    )
    monkeypatch.setattr(upd.AkshareProvider, "fetch_bars", staticmethod(lambda **kwargs: fake_df))
    monkeypatch.setattr(upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ"))

    local_root = tmp_path / "data" / "market" / "30m"
    upd.update_one_symbol("000001.SZ", "2024-01-01", "2024-01-02", "qfq", local_root, export_excel=False, timeframe="30m")

    log_df = pd.read_parquet(upd.UPDATE_LOG_PATH)
    assert "timeframe" in log_df.columns
    assert log_df.iloc[0]["timeframe"] == "30m"
