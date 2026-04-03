from __future__ import annotations

import pandas as pd
import pytest

from data.providers.tdx_official_standalone import (
    TdxOfficialStandaloneError,
    TdxOfficialStandaloneProvider,
)


@pytest.fixture(autouse=True)
def reset_provider_state(monkeypatch):
    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_tq", None)
    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_initialized", False)


def test_fetch_with_diagnostics_uses_official_stock_list_contract(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            assert kwargs["field_list"] == ["Open", "High", "Low", "Close", "Volume", "Amount"]
            assert kwargs["stock_list"] == ["000001.SZ"]
            assert kwargs["period"] == "1m"
            assert kwargs["start_time"] == "20240102093000"
            assert kwargs["end_time"] == "20240102150000"
            assert kwargs["count"] == -1
            assert kwargs["dividend_type"] == "front"
            assert kwargs["fill_data"] is False

            index = pd.to_datetime(["2024-01-02 09:30:00", "2024-01-02 09:31:00"])
            return {
                "Open": pd.DataFrame({"000001.SZ": [1.0, 1.01]}, index=index),
                "High": pd.DataFrame({"000001.SZ": [1.1, 1.11]}, index=index),
                "Low": pd.DataFrame({"000001.SZ": [0.9, 0.91]}, index=index),
                "Close": pd.DataFrame({"000001.SZ": [1.05, None]}, index=index),
                "Volume": pd.DataFrame({"000001.SZ": [100.0, 120.0]}, index=index),
                "Amount": pd.DataFrame({"000001.SZ": [1000.0, 1220.0]}, index=index),
            }

    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_import_tq", staticmethod(lambda _: FakeTq()))

    result = TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
        symbol="000001.SZ",
        timeframe="1m",
        start_date="2024-01-02 09:30:00",
        end_date="2024-01-02 15:00:00",
        adjust="qfq",
    )

    assert list(result.bars.columns) == ["date", "symbol", "open", "high", "low", "close", "volume", "amount"]
    assert len(result.bars) == 1
    assert result.diagnostics.formatted_start_time == "20240102093000"
    assert result.diagnostics.formatted_end_time == "20240102150000"
    assert result.diagnostics.request_kwargs["stock_list"] == ["000001.SZ"]
    assert result.diagnostics.raw_payload_type == "dict"
    assert "Open" in result.diagnostics.returned_keys
    assert result.diagnostics.raw_row_count == 2
    assert result.diagnostics.assembled_row_count == 2
    assert result.diagnostics.normalized_row_count == 1
    assert result.diagnostics.dropped_row_count == 1


def test_fetch_accepts_lowercase_field_aliases(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            index = pd.to_datetime(["2024-01-02 09:30:00"])
            return {
                "open": pd.DataFrame({"000001.SZ": [1.0]}, index=index),
                "high": pd.DataFrame({"000001.SZ": [1.1]}, index=index),
                "low": pd.DataFrame({"000001.SZ": [0.9]}, index=index),
                "close": pd.DataFrame({"000001.SZ": [1.05]}, index=index),
                "volume": pd.DataFrame({"000001.SZ": [10.0]}, index=index),
                "amount": pd.DataFrame({"000001.SZ": [100.0]}, index=index),
            }

    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_import_tq", staticmethod(lambda _: FakeTq()))

    result = TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
        symbol="000001.SZ",
        timeframe="5m",
        start_date="2024-01-02",
        end_date="2024-01-02",
        adjust="hfq",
    )

    assert len(result.bars) == 1
    assert result.bars.iloc[0]["date"] == pd.Timestamp("2024-01-02 09:30:00")


def test_fetch_empty_official_payload_returns_empty_bars(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            return {}

    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_import_tq", staticmethod(lambda _: FakeTq()))

    result = TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
        symbol="000001.SZ",
        timeframe="1m",
        start_date="2024-01-02",
        end_date="2024-01-02",
        adjust="qfq",
    )

    assert result.bars.empty
    assert result.diagnostics.raw_row_count == 0
    assert result.diagnostics.assembled_row_count == 0


def test_fetch_reports_missing_required_fields(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            index = pd.to_datetime(["2024-01-02 09:30:00"])
            return {
                "Open": pd.DataFrame({"000001.SZ": [1.0]}, index=index),
                "Close": pd.DataFrame({"000001.SZ": [1.05]}, index=index),
            }

    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_import_tq", staticmethod(lambda _: FakeTq()))

    with pytest.raises(TdxOfficialStandaloneError) as exc_info:
        TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol="000001.SZ",
            timeframe="1m",
            start_date="2024-01-02",
            end_date="2024-01-02",
        )

    assert exc_info.value.diagnostics.failure_stage == "normalize"
    assert "missing required fields" in exc_info.value.diagnostics.failure_message
    assert "Open" in exc_info.value.diagnostics.returned_keys
    assert "Close" in exc_info.value.diagnostics.returned_keys


def test_fetch_reports_missing_symbol_column(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            index = pd.to_datetime(["2024-01-02 09:30:00"])
            return {
                "Open": pd.DataFrame({"600000.SH": [1.0]}, index=index),
                "High": pd.DataFrame({"600000.SH": [1.1]}, index=index),
                "Low": pd.DataFrame({"600000.SH": [0.9]}, index=index),
                "Close": pd.DataFrame({"600000.SH": [1.05]}, index=index),
                "Volume": pd.DataFrame({"600000.SH": [10.0]}, index=index),
                "Amount": pd.DataFrame({"600000.SH": [100.0]}, index=index),
            }

    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_import_tq", staticmethod(lambda _: FakeTq()))

    with pytest.raises(TdxOfficialStandaloneError) as exc_info:
        TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol="000001.SZ",
            timeframe="1m",
            start_date="2024-01-02",
            end_date="2024-01-02",
        )

    assert exc_info.value.diagnostics.failure_stage == "normalize"
    assert "missing symbol column" in exc_info.value.diagnostics.failure_message


def test_fetch_reports_init_stage_error(monkeypatch):
    monkeypatch.setattr(
        TdxOfficialStandaloneProvider,
        "_ensure_initialized",
        staticmethod(lambda _: (_ for _ in ()).throw(RuntimeError("terminal unavailable"))),
    )

    with pytest.raises(TdxOfficialStandaloneError) as exc_info:
        TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol="000001.SZ",
            timeframe="1m",
            start_date="2024-01-02",
            end_date="2024-01-02",
            adjust="qfq",
        )

    assert exc_info.value.diagnostics.failure_stage == "init"
    assert "terminal unavailable" in exc_info.value.diagnostics.failure_message


def test_fetch_reports_fetch_stage_error(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            raise RuntimeError("endpoint down")

    monkeypatch.setattr(TdxOfficialStandaloneProvider, "_import_tq", staticmethod(lambda _: FakeTq()))

    with pytest.raises(TdxOfficialStandaloneError) as exc_info:
        TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol="000001.SZ",
            timeframe="1m",
            start_date="2024-01-02",
            end_date="2024-01-02",
        )

    assert exc_info.value.diagnostics.failure_stage == "fetch"
    assert "endpoint down" in exc_info.value.diagnostics.failure_message


def test_fetch_reports_validate_stage_error_for_bad_timeframe():
    with pytest.raises(TdxOfficialStandaloneError) as exc_info:
        TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol="000001.SZ",
            timeframe="15m",
            start_date="2024-01-02",
            end_date="2024-01-02",
        )

    assert exc_info.value.diagnostics.failure_stage == "validate"


def test_fetch_reports_validate_stage_error_for_bad_adjust():
    with pytest.raises(TdxOfficialStandaloneError) as exc_info:
        TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol="000001.SZ",
            timeframe="1m",
            start_date="2024-01-02",
            end_date="2024-01-02",
            adjust="bad",
        )

    assert exc_info.value.diagnostics.failure_stage == "validate"
