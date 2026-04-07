from __future__ import annotations

from types import ModuleType
import sys

import pandas as pd
import pytest

import data.providers.tdx_quant_provider as tdx_quant_provider_module
from data.providers.tdx_quant_provider import TdxQuantProvider


@pytest.fixture(autouse=True)
def reset_tdx_quant_provider_state(monkeypatch):
    monkeypatch.setattr(TdxQuantProvider, "_tq", None)
    monkeypatch.setattr(TdxQuantProvider, "_initialized", False)


def test_fetch_bars_initializes_once_maps_adjust_and_normalizes(monkeypatch):
    class FakeTq:
        def __init__(self):
            self.initialize_calls: list[str] = []
            self.market_calls: list[dict[str, object]] = []

        def initialize(self, caller_path: str) -> None:
            self.initialize_calls.append(caller_path)

        def get_market_data(self, **kwargs):
            self.market_calls.append(kwargs)
            index = pd.to_datetime(["2024-01-02 09:30:00", "2024-01-02 09:31:00"])
            return {
                "Open": pd.DataFrame({"000001.SZ": [1.0, 1.1]}, index=index),
                "High": pd.DataFrame({"000001.SZ": [1.2, 1.3]}, index=index),
                "Low": pd.DataFrame({"000001.SZ": [0.9, 1.0]}, index=index),
                "Close": pd.DataFrame({"000001.SZ": [1.1, 1.2]}, index=index),
                "Volume": pd.DataFrame({"000001.SZ": [10.0, 11.0]}, index=index),
                "Amount": pd.DataFrame({"000001.SZ": [100.0, 110.0]}, index=index),
            }

    fake_tq = FakeTq()
    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: fake_tq))

    result = TdxQuantProvider.fetch_bars(
        "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
    )
    second = TdxQuantProvider.fetch_bars(
        "000001.SZ", "5m", "2024-01-02", "2024-01-02", adjust="hfq"
    )
    third = TdxQuantProvider.fetch_bars(
        "000001.SZ", "30m", "2024-01-02", "2024-01-02", adjust="qfq"
    )

    assert fake_tq.initialize_calls == [tdx_quant_provider_module.__file__]
    assert fake_tq.market_calls[0]["field_list"] == [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Amount",
    ]
    assert fake_tq.market_calls[0]["stock_list"] == ["000001.SZ"]
    assert fake_tq.market_calls[0]["start_time"] == "20240102"
    assert fake_tq.market_calls[0]["end_time"] == "20240102"
    assert fake_tq.market_calls[0]["period"] == "1m"
    assert fake_tq.market_calls[0]["count"] == -1
    assert fake_tq.market_calls[0]["dividend_type"] == "front"
    assert fake_tq.market_calls[0]["fill_data"] is False
    assert fake_tq.market_calls[1]["start_time"] == "20240102"
    assert fake_tq.market_calls[1]["end_time"] == "20240102"
    assert fake_tq.market_calls[1]["period"] == "5m"
    assert fake_tq.market_calls[1]["dividend_type"] == "back"
    assert fake_tq.market_calls[2]["period"] == "30m"
    assert list(result.columns) == [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]
    assert list(result["date"]) == [
        pd.Timestamp("2024-01-02 09:30:00"),
        pd.Timestamp("2024-01-02 09:31:00"),
    ]
    assert second["symbol"].tolist() == ["000001.SZ", "000001.SZ"]
    assert third["symbol"].tolist() == ["000001.SZ", "000001.SZ"]


def test_fetch_bars_fails_fast_when_tqcenter_is_missing(monkeypatch):
    monkeypatch.setattr(
        TdxQuantProvider,
        "_import_tq",
        staticmethod(lambda: (_ for _ in ()).throw(ImportError("missing tqcenter"))),
    )

    with pytest.raises(ImportError, match="无法导入 tqcenter"):
        TdxQuantProvider.fetch_bars("000001.SZ", "1m", "2024-01-01", "2024-01-02")


def test_fetch_bars_fails_fast_when_terminal_is_unavailable(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path
            raise RuntimeError("terminal unavailable")

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    with pytest.raises(RuntimeError, match="通达信终端已启动并登录"):
        TdxQuantProvider.fetch_bars("000001.SZ", "1m", "2024-01-01", "2024-01-02")


def test_import_tq_prefers_explicit_env_path(monkeypatch, tmp_path):
    plugin_dir = tmp_path / "PYPlugins" / "user"
    plugin_dir.mkdir(parents=True)
    monkeypatch.setenv(TdxQuantProvider.TQCENTER_ENV_VAR, str(plugin_dir))

    fake_module = ModuleType("tqcenter")
    setattr(fake_module, "tq", object())
    import_calls: list[str] = []

    original_import_module = __import__("importlib").import_module

    def fake_import_module(name: str, package: str | None = None):
        del package
        if name == "tqcenter":
            import_calls.append(name)
            assert str(plugin_dir.resolve()) == sys.path[0]
            return fake_module
        return original_import_module(name)

    monkeypatch.setattr("importlib.import_module", fake_import_module)

    tq = TdxQuantProvider._import_tq()

    assert tq is fake_module.tq
    assert import_calls == ["tqcenter"]


def test_import_tq_accepts_install_root_by_expanding_pyplugins_user(
    monkeypatch, tmp_path
):
    install_root = tmp_path / "TdxInstall"
    plugin_dir = install_root / "PYPlugins" / "user"
    plugin_dir.mkdir(parents=True)
    monkeypatch.setenv(TdxQuantProvider.TQCENTER_ENV_VAR, str(install_root))

    fake_module = ModuleType("tqcenter")
    setattr(fake_module, "tq", object())

    original_import_module = __import__("importlib").import_module

    def fake_import_module(name: str, package: str | None = None):
        del package
        if name == "tqcenter":
            assert str(plugin_dir.resolve()) == sys.path[0]
            return fake_module
        return original_import_module(name)

    monkeypatch.setattr("importlib.import_module", fake_import_module)

    tq = TdxQuantProvider._import_tq()

    assert tq is fake_module.tq


def test_fetch_bars_rejects_unsupported_timeframe():
    with pytest.raises(ValueError, match="1d、30m、15m、1m 或 5m"):
        TdxQuantProvider.fetch_bars("000001.SZ", "60m", "2024-01-01", "2024-01-02")


def test_fetch_bars_accepts_official_lowercase_field_alias_payload(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            assert kwargs["stock_list"] == ["000001.SZ"]
            index = pd.to_datetime(["2024-01-02 09:35:00"])
            return {
                "open": pd.DataFrame({"000001.SZ": [1.0]}, index=index),
                "high": pd.DataFrame({"000001.SZ": [1.1]}, index=index),
                "low": pd.DataFrame({"000001.SZ": [0.9]}, index=index),
                "close": pd.DataFrame({"000001.SZ": [1.05]}, index=index),
                "vol": pd.DataFrame({"000001.SZ": [12.0]}, index=index),
                "amount": pd.DataFrame({"000001.SZ": [120.0]}, index=index),
            }

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    result = TdxQuantProvider.fetch_bars(
        "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
    )

    assert result.iloc[0]["date"] == pd.Timestamp("2024-01-02 09:35:00")
    assert result.iloc[0]["volume"] == 12


def test_fetch_bars_empty_official_payload_returns_empty_bars(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            return {}

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    result = TdxQuantProvider.fetch_bars(
        "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
    )

    assert result.empty


def test_fetch_bars_reports_missing_required_fields(monkeypatch):
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

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    with pytest.raises(RuntimeError, match="normalize阶段") as exc_info:
        TdxQuantProvider.fetch_bars(
            "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
        )

    assert "missing required fields" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_fetch_bars_reports_missing_symbol_column(monkeypatch):
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

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    with pytest.raises(RuntimeError, match="normalize阶段") as exc_info:
        TdxQuantProvider.fetch_bars(
            "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
        )

    assert "missing symbol column" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_fetch_bars_formats_explicit_time_for_tdx_request(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            assert kwargs["start_time"] == "20240102093000"
            assert kwargs["end_time"] == "20240102150000"
            index = pd.to_datetime(["2024-01-02 09:30:00"])
            return {
                "Open": pd.DataFrame({"000001.SZ": [1.0]}, index=index),
                "High": pd.DataFrame({"000001.SZ": [1.1]}, index=index),
                "Low": pd.DataFrame({"000001.SZ": [0.9]}, index=index),
                "Close": pd.DataFrame({"000001.SZ": [1.05]}, index=index),
                "Volume": pd.DataFrame({"000001.SZ": [12.0]}, index=index),
                "Amount": pd.DataFrame({"000001.SZ": [120.0]}, index=index),
            }

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    result = TdxQuantProvider.fetch_bars(
        "000001.SZ", "1m", "2024-01-02 09:30:00", "2024-01-02 15:00:00", adjust="qfq"
    )

    assert not result.empty


def test_fetch_bars_error_message_keeps_root_cause_details(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            raise ValueError("bad request format")

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    with pytest.raises(
        RuntimeError, match="ValueError: bad request format"
    ) as exc_info:
        TdxQuantProvider.fetch_bars(
            "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
        )

    assert isinstance(exc_info.value.__cause__, ValueError)


def test_fetch_bars_normalize_stage_error_message_keeps_root_cause_details(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def get_market_data(self, **kwargs):
            del kwargs
            index = pd.to_datetime(["2024-01-02 09:30:00"])
            return {
                "Open": pd.DataFrame({"000001.SZ": [1.0]}, index=index),
                "High": pd.DataFrame({"000001.SZ": [1.1]}, index=index),
                "Low": pd.DataFrame({"000001.SZ": [0.9]}, index=index),
                "Close": pd.DataFrame({"000001.SZ": [1.05]}, index=index),
                "Volume": pd.DataFrame({"000001.SZ": [12.0]}, index=index),
                "Amount": pd.DataFrame({"000001.SZ": [120.0]}, index=index),
            }

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))
    monkeypatch.setattr(
        TdxQuantProvider,
        "_normalize_bars",
        staticmethod(
            lambda raw_data, symbol, start_date, end_date: (_ for _ in ()).throw(
                ValueError("bad normalize shape")
            )
        ),
    )

    with pytest.raises(RuntimeError, match="normalize阶段") as exc_info:
        TdxQuantProvider.fetch_bars(
            "000001.SZ", "1m", "2024-01-02", "2024-01-02", adjust="qfq"
        )

    assert "ValueError: bad normalize shape" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)
