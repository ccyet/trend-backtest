from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

import scripts.update_data as upd


def test_normalize_timeframes_defaults_and_dedupes_order() -> None:
    assert upd._normalize_timeframes(None) == ["1d"]
    assert upd._normalize_timeframes([]) == ["1d"]
    assert upd._normalize_timeframes(["1d", "30m", "1d", "15m"]) == [
        "1d",
        "30m",
        "15m",
    ]


def test_parse_args_accepts_repeated_timeframe_flags(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "update_data.py",
            "--timeframe",
            "1d",
            "--timeframe",
            "30m",
            "--timeframe",
            "1d",
        ],
    )

    args = upd.parse_args()

    assert args.timeframe == ["1d", "30m", "1d"]


def test_parse_args_accepts_repeated_provider_flags(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "update_data.py",
            "--provider",
            "1d=tdx",
            "--provider",
            "5m=akshare",
        ],
    )

    args = upd.parse_args()

    assert args.provider == ["1d=tdx", "5m=akshare"]


def test_fetch_bars_for_timeframe_routes_by_provider(monkeypatch):
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        upd.TdxQuantProvider,
        "fetch_bars",
        staticmethod(
            lambda **kwargs: (
                calls.append(("tdx_quant", kwargs["timeframe"])) or pd.DataFrame()
            )
        ),
    )
    monkeypatch.setattr(
        upd.AkshareProvider,
        "fetch_bars",
        staticmethod(
            lambda **kwargs: (
                calls.append(("akshare", kwargs["timeframe"])) or pd.DataFrame()
            )
        ),
    )

    upd._fetch_bars_for_timeframe(
        symbol="000001.SZ",
        timeframe="1m",
        start_date="2024-01-01",
        end_date="2024-01-02",
        adjust="qfq",
        provider="tdx",
    )
    upd._fetch_bars_for_timeframe(
        symbol="000001.SZ",
        timeframe="5m",
        start_date="2024-01-01",
        end_date="2024-01-02",
        adjust="qfq",
        provider="tdx",
    )
    upd._fetch_bars_for_timeframe(
        symbol="000001.SZ",
        timeframe="1d",
        start_date="2024-01-01",
        end_date="2024-01-02",
        adjust="qfq",
        provider="tdx",
    )
    upd._fetch_bars_for_timeframe(
        symbol="000001.SZ",
        timeframe="15m",
        start_date="2024-01-01",
        end_date="2024-01-02",
        adjust="qfq",
        provider="akshare",
    )

    assert calls == [
        ("tdx_quant", "1m"),
        ("tdx_quant", "5m"),
        ("tdx_quant", "1d"),
        ("akshare", "15m"),
    ]


def test_normalize_provider_overrides_validates_supported_sources() -> None:
    assert upd._normalize_provider_overrides(["1d=tdx", "5m=akshare", "30m=tdx"]) == {
        "1d": "tdx",
        "5m": "akshare",
        "30m": "tdx",
    }


def test_resolve_timeframe_provider_prefers_cli_override() -> None:
    assert (
        upd.resolve_timeframe_provider(
            "1d",
            {"1d": "tdx"},
            {"1d": "akshare", "5m": "tdx"},
        )
        == "tdx"
    )


def test_update_one_symbol_uses_tdx_quant_provider_for_30m(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(
        upd,
        "UPDATE_LOG_PATH",
        tmp_path / "data" / "market" / "metadata" / "update_log.parquet",
    )
    monkeypatch.setattr(upd, "upsert_inventory_row", lambda row: row)
    monkeypatch.setattr(
        upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ")
    )

    calls: list[str] = []
    fake_df = pd.DataFrame(
        {
            "date": ["2024-01-02 10:00:00", "2024-01-02 10:30:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [1.0, 1.1],
            "high": [1.1, 1.2],
            "low": [0.9, 1.0],
            "close": [1.05, 1.15],
            "volume": [10.0, 12.0],
            "amount": [100.0, 120.0],
        }
    )
    monkeypatch.setattr(
        upd.TdxQuantProvider,
        "fetch_bars",
        staticmethod(lambda **kwargs: calls.append(kwargs["timeframe"]) or fake_df),
    )
    monkeypatch.setattr(
        upd.AkshareProvider,
        "fetch_bars",
        staticmethod(
            lambda **kwargs: (_ for _ in ()).throw(
                AssertionError("akshare should not be used for 30m tdx provider")
            )
        ),
    )

    local_root = tmp_path / "data" / "market" / "30m"
    upd.update_one_symbol(
        "000001.SZ",
        "2024-01-01",
        "2024-01-02",
        "qfq",
        local_root,
        export_excel=False,
        timeframe="30m",
        provider="tdx",
    )

    saved = pd.read_parquet(local_root / "qfq" / "000001.SZ.parquet")
    assert calls == ["30m"]
    assert len(saved) == 2


def test_update_one_symbol_export_excel(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(
        upd,
        "UPDATE_LOG_PATH",
        tmp_path / "data" / "market" / "metadata" / "update_log.parquet",
    )
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

    monkeypatch.setattr(
        upd.AkshareProvider, "fetch_bars", staticmethod(lambda **kwargs: fake_df)
    )
    monkeypatch.setattr(
        upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ")
    )

    local_root = tmp_path / "data" / "market" / "daily"
    upd.update_one_symbol(
        "000001.SZ", "2024-01-01", "2024-01-02", "qfq", local_root, export_excel=True
    )

    excel_path = tmp_path / "data" / "market" / "exports" / "qfq" / "000001.SZ.xlsx"
    assert excel_path.exists()


def test_main_fans_out_symbols_across_selected_timeframes(monkeypatch, capsys) -> None:
    calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        upd,
        "parse_args",
        lambda: upd.argparse.Namespace(
            symbols="000001.SZ,000002.SZ",
            start_date="2024-01-01",
            end_date="2024-01-05",
            adjust="qfq",
            timeframe=["1d", "30m", "1d"],
            provider=["1d=tdx"],
            refresh_symbols=False,
            export_excel=False,
        ),
    )
    monkeypatch.setattr(
        upd,
        "read_config",
        lambda: {
            "data_source": "local_parquet",
            "local_data_root": "data/market/daily",
            "default_adjust": "qfq",
            "offline_update_source_1d": "akshare",
            "offline_update_source_30m": "akshare",
            "offline_update_source_15m": "akshare",
            "offline_update_source_5m": "tdx",
            "offline_update_source_1m": "tdx",
        },
    )
    monkeypatch.setattr(
        upd,
        "resolve_offline_update_sources",
        lambda config: {
            "1d": "akshare",
            "30m": "akshare",
            "15m": "akshare",
            "5m": "tdx",
            "1m": "tdx",
        },
    )
    monkeypatch.setattr(
        upd,
        "resolve_local_data_root",
        lambda root, timeframe: Path(root) / timeframe,
    )

    def _fake_update_one_symbol(
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str,
        local_root: Path,
        export_excel: bool,
        timeframe: str = "1d",
        provider: str = "akshare",
    ) -> bool:
        del export_excel
        calls.append((timeframe, symbol, str(local_root), provider))
        return symbol != "000002.SZ"

    monkeypatch.setattr(upd, "update_one_symbol", _fake_update_one_symbol)

    upd.main()

    assert calls == [
        ("1d", "000001.SZ", str(upd.ROOT / "data" / "market" / "daily" / "1d"), "tdx"),
        ("1d", "000002.SZ", str(upd.ROOT / "data" / "market" / "daily" / "1d"), "tdx"),
        (
            "30m",
            "000001.SZ",
            str(upd.ROOT / "data" / "market" / "daily" / "30m"),
            "akshare",
        ),
        (
            "30m",
            "000002.SZ",
            str(upd.ROOT / "data" / "market" / "daily" / "30m"),
            "akshare",
        ),
    ]
    stdout = capsys.readouterr().out
    assert "[timeframe=1d source=tdx] 开始更新，共 2 只标的" in stdout
    assert "[timeframe=1d source=tdx] 更新完成：success=1, failed=1, total=2" in stdout
    assert "[timeframe=30m source=akshare] 开始更新，共 2 只标的" in stdout
    assert (
        "[timeframe=30m source=akshare] 更新完成：success=1, failed=1, total=2"
        in stdout
    )


def test_update_one_symbol_skips_rewrite_when_incremental_fetch_empty(
    tmp_path: Path, monkeypatch
):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(
        upd,
        "UPDATE_LOG_PATH",
        tmp_path / "data" / "market" / "metadata" / "update_log.parquet",
    )
    recorded_rows: list[dict[str, object]] = []
    monkeypatch.setattr(
        upd, "upsert_inventory_row", lambda row: recorded_rows.append(row)
    )
    monkeypatch.setattr(
        upd.AkshareProvider,
        "fetch_bars",
        staticmethod(
            lambda **kwargs: pd.DataFrame(
                columns=pd.Index(
                    [
                        "date",
                        "symbol",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                    ]
                )
            )
        ),
    )
    monkeypatch.setattr(
        upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ")
    )

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


def test_update_one_symbol_writes_timeframe_into_update_log(
    tmp_path: Path, monkeypatch
):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(
        upd,
        "UPDATE_LOG_PATH",
        tmp_path / "data" / "market" / "metadata" / "update_log.parquet",
    )
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
    monkeypatch.setattr(
        upd.AkshareProvider, "fetch_bars", staticmethod(lambda **kwargs: fake_df)
    )
    monkeypatch.setattr(
        upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ")
    )

    local_root = tmp_path / "data" / "market" / "30m"
    upd.update_one_symbol(
        "000001.SZ",
        "2024-01-01",
        "2024-01-02",
        "qfq",
        local_root,
        export_excel=False,
        timeframe="30m",
    )

    log_df = pd.read_parquet(upd.UPDATE_LOG_PATH)
    assert "timeframe" in log_df.columns
    assert log_df.iloc[0]["timeframe"] == "30m"


def test_update_one_symbol_uses_tdx_quant_provider_for_1m(tmp_path: Path, monkeypatch):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(
        upd,
        "UPDATE_LOG_PATH",
        tmp_path / "data" / "market" / "metadata" / "update_log.parquet",
    )
    monkeypatch.setattr(upd, "upsert_inventory_row", lambda row: row)
    monkeypatch.setattr(
        upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ")
    )

    calls: list[str] = []
    fake_df = pd.DataFrame(
        {
            "date": ["2024-01-02 09:30:00", "2024-01-02 09:31:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [1.0, 1.1],
            "high": [1.1, 1.2],
            "low": [0.9, 1.0],
            "close": [1.05, 1.15],
            "volume": [10.0, 12.0],
            "amount": [100.0, 120.0],
        }
    )
    monkeypatch.setattr(
        upd.TdxQuantProvider,
        "fetch_bars",
        staticmethod(lambda **kwargs: calls.append(kwargs["timeframe"]) or fake_df),
    )
    monkeypatch.setattr(
        upd.AkshareProvider,
        "fetch_bars",
        staticmethod(
            lambda **kwargs: (_ for _ in ()).throw(
                AssertionError("akshare should not be used for 1m")
            )
        ),
    )

    local_root = tmp_path / "data" / "market" / "1m"
    upd.update_one_symbol(
        "000001.SZ",
        "2024-01-01",
        "2024-01-02",
        "qfq",
        local_root,
        export_excel=False,
        timeframe="1m",
        provider="tdx",
    )

    saved = pd.read_parquet(local_root / "qfq" / "000001.SZ.parquet")
    assert calls == ["1m"]
    assert list(saved["date"]) == [
        pd.Timestamp("2024-01-02 09:30:00"),
        pd.Timestamp("2024-01-02 09:31:00"),
    ]


def test_resolve_incremental_start_keeps_intraday_timestamp_for_1m():
    existing = pd.DataFrame(
        {
            "date": ["2024-01-05 14:58:00", "2024-01-05 14:59:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
        }
    )

    result = upd._resolve_incremental_start("2024-01-01", existing, "1m")

    assert result == "2024-01-02 14:59:00"


def test_resolve_incremental_start_respects_requested_start_for_1m():
    existing = pd.DataFrame(
        {
            "date": ["2024-01-02 09:31:00"],
            "symbol": ["000001.SZ"],
        }
    )

    result = upd._resolve_incremental_start("2024-01-04", existing, "1m")

    assert result == "2024-01-04 00:00:00"


def test_update_one_symbol_preserves_root_cause_in_error_message(
    tmp_path: Path, monkeypatch
):
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(upd, "ROOT", tmp_path)
    monkeypatch.setattr(upd, "METADATA_DIR", tmp_path / "data" / "market" / "metadata")
    monkeypatch.setattr(
        upd,
        "UPDATE_LOG_PATH",
        tmp_path / "data" / "market" / "metadata" / "update_log.parquet",
    )
    recorded_rows: list[dict[str, object]] = []
    monkeypatch.setattr(
        upd, "upsert_inventory_row", lambda row: recorded_rows.append(row)
    )
    monkeypatch.setattr(
        upd.AkshareProvider, "to_standard_symbol", staticmethod(lambda x: "000001.SZ")
    )

    def _raise_fetch_error(**kwargs):
        del kwargs
        try:
            raise ValueError("bad time format")
        except ValueError as inner:
            raise RuntimeError("TDX Quant 行情下载失败。根因见下文") from inner

    monkeypatch.setattr(upd, "_fetch_bars_for_timeframe", _raise_fetch_error)

    local_root = tmp_path / "data" / "market" / "1m"
    upd.update_one_symbol(
        "000001.SZ",
        "2024-01-01",
        "2024-01-02",
        "qfq",
        local_root,
        export_excel=False,
        timeframe="1m",
    )

    log_df = pd.read_parquet(upd.UPDATE_LOG_PATH)
    message = str(log_df.iloc[0]["error_message"])
    assert "RuntimeError: TDX Quant 行情下载失败" in message
    assert "ValueError: bad time format" in message
    assert recorded_rows
    assert "ValueError: bad time format" in str(recorded_rows[0]["last_error_message"])
