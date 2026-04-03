from __future__ import annotations

import pandas as pd

import scripts.fetch_tdx_official_standalone as cli
from data.providers.tdx_official_standalone import (
    TdxOfficialFetchDiagnostics,
    TdxOfficialFetchResult,
    TdxOfficialStandaloneError,
)


def _diag(**overrides):
    payload = {
        "symbol": "000001.SZ",
        "timeframe": "1m",
        "adjust": "qfq",
        "start_date": "2024-01-02",
        "end_date": "2024-01-02",
        "formatted_start_time": "20240102",
        "formatted_end_time": "20240102",
        "request_kwargs": {
            "field_list": ["Open", "High", "Low", "Close", "Volume", "Amount"],
            "stock_list": ["000001.SZ"],
            "period": "1m",
            "start_time": "20240102",
            "end_time": "20240102",
            "count": -1,
            "dividend_type": "front",
            "fill_data": False,
        },
        "raw_payload_type": "dict",
        "raw_payload_summary": "mapping_keys=['Open'],size=1",
        "returned_keys": ["Open"],
        "field_shapes": {"Open": "shape=(1, 1)"},
        "symbol_column_presence": {"Open": True, "High": True, "Low": True, "Close": True, "Volume": True, "Amount": True},
        "raw_row_count": 1,
        "assembled_row_count": 1,
        "normalized_row_count": 1,
        "dropped_row_count": 0,
        "failure_stage": "",
        "failure_message": "",
    }
    payload.update(overrides)
    return TdxOfficialFetchDiagnostics(**payload)


def test_cli_success_writes_output_and_prints_diagnostics(monkeypatch, tmp_path, capsys):
    output = tmp_path / "bars.csv"
    bars = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-02 09:30:00")],
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
        cli.TdxOfficialStandaloneProvider,
        "fetch_bars_with_diagnostics",
        staticmethod(lambda **kwargs: TdxOfficialFetchResult(bars=bars, diagnostics=_diag())),
    )

    code = cli.main(
        [
            "--symbol",
            "000001.SZ",
            "--timeframe",
            "1m",
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-02",
            "--print-diagnostics",
            "--output",
            str(output),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert output.exists()
    assert "Diagnostics:" in captured.out
    assert "Fetched rows: 1" in captured.out
    assert "Saved normalized bars to:" in captured.out


def test_cli_returns_failure_for_provider_error(monkeypatch, capsys):
    error = TdxOfficialStandaloneError(
        "TDX 官方行情下载失败（fetch阶段）。",
        _diag(failure_stage="fetch", failure_message="RuntimeError: endpoint down"),
    )
    monkeypatch.setattr(
        cli.TdxOfficialStandaloneProvider,
        "fetch_bars_with_diagnostics",
        staticmethod(lambda **kwargs: (_ for _ in ()).throw(error)),
    )

    code = cli.main(
        [
            "--symbol",
            "000001.SZ",
            "--timeframe",
            "1m",
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-02",
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "TDX 官方行情下载失败（fetch阶段）。" in captured.err
    assert "Diagnostics:" in captured.err
    assert '"failure_stage": "fetch"' in captured.err


def test_cli_returns_failure_for_invalid_output_extension(monkeypatch, tmp_path, capsys):
    bars = pd.DataFrame(columns=pd.Index(["date", "symbol", "open", "high", "low", "close", "volume", "amount"]))
    monkeypatch.setattr(
        cli.TdxOfficialStandaloneProvider,
        "fetch_bars_with_diagnostics",
        staticmethod(lambda **kwargs: TdxOfficialFetchResult(bars=bars, diagnostics=_diag())),
    )

    code = cli.main(
        [
            "--symbol",
            "000001.SZ",
            "--timeframe",
            "1m",
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-02",
            "--output",
            str(tmp_path / "bars.txt"),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "Output error:" in captured.err
