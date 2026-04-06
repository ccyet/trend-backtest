from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from data.providers.tdx_local_indicator_provider import TdxLocalIndicatorProvider
from data.providers.tdx_quant_provider import TdxQuantProvider


@pytest.fixture(autouse=True)
def reset_tdx_indicator_state(monkeypatch):
    monkeypatch.setattr(TdxQuantProvider, "_tq", None)
    monkeypatch.setattr(TdxQuantProvider, "_initialized", False)


def test_import_indicator_aligns_formula_values_to_next_trade_date(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def formula_process_mul_zb(self, **kwargs):
            assert kwargs["formula_name"] == "板块均线"
            assert kwargs["stock_list"] == ["000001.SZ"]
            assert kwargs["stock_period"] == "1d"
            assert kwargs["dividend_type"] == 1
            assert kwargs["return_date"] is True
            return {
                "ErrorId": "0",
                "000001.SZ": {
                    "NOTEXT1": [
                        {"Date": "20260401", "Value": "11.0"},
                        {"Date": "20260402", "Value": "22.0"},
                        {"Date": "20260403", "Value": "33.0"},
                    ],
                    "NOTEXT2": [
                        {"Date": "20260401", "Value": "44.0"},
                        {"Date": "20260402", "Value": "55.0"},
                        {"Date": "20260403", "Value": "66.0"},
                    ],
                },
            }

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    result = TdxLocalIndicatorProvider.import_indicator(
        symbol="000001.SZ",
        indicator_key="board_ma",
        start_date="2026-04-02",
        end_date="2026-04-03",
        adjust="qfq",
    )

    assert list(result.columns) == ["date", "symbol", "board_ma_ratio_20", "board_ma_ratio_50"]
    assert list(result["date"]) == [
        pd.Timestamp("2026-04-02 00:00:00"),
        pd.Timestamp("2026-04-03 00:00:00"),
    ]
    assert result["board_ma_ratio_20"].tolist() == [11.0, 22.0]
    assert result["board_ma_ratio_50"].tolist() == [44.0, 55.0]


def test_import_indicator_keeps_root_cause_details(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def formula_process_mul_zb(self, **kwargs):
            del kwargs
            return {"ErrorId": "1", "Error": "formula missing"}

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    with pytest.raises(RuntimeError, match="TDX 本地指标导入失败（normalize阶段）") as exc_info:
        TdxLocalIndicatorProvider.import_indicator(
            symbol="000001.SZ",
            indicator_key="board_ma",
            start_date="2026-04-01",
            end_date="2026-04-03",
        )

    assert "formula missing" in str(exc_info.value)


def test_discover_indicator_candidates_returns_registry_with_fallback_message(tmp_path: Path) -> None:
    candidates, message = TdxLocalIndicatorProvider.discover_indicator_candidates(str(tmp_path))

    assert candidates
    assert candidates[0].formula_name == "板块均线"
    assert "未发现通达信提供稳定的本地公式枚举接口" in message


def test_import_indicator_supports_manual_formula_name_and_output_map(monkeypatch):
    class FakeTq:
        def initialize(self, caller_path: str) -> None:
            del caller_path

        def formula_process_mul_zb(self, **kwargs):
            assert kwargs["formula_name"] == "我的板块均线"
            return {
                "ErrorId": "0",
                "000001.SZ": {
                    "OUT_A": [{"Date": "20260401", "Value": "10"}, {"Date": "20260402", "Value": "20"}],
                    "OUT_B": [{"Date": "20260401", "Value": "30"}, {"Date": "20260402", "Value": "40"}],
                },
            }

    monkeypatch.setattr(TdxQuantProvider, "_import_tq", staticmethod(lambda: FakeTq()))

    result = TdxLocalIndicatorProvider.import_indicator(
        symbol="000001.SZ",
        indicator_key="board_ma",
        start_date="2026-04-02",
        end_date="2026-04-02",
        formula_name="我的板块均线",
        output_map={"board_ma_ratio_20": "OUT_A", "board_ma_ratio_50": "OUT_B"},
    )

    assert result["board_ma_ratio_20"].tolist() == [10.0]
    assert result["board_ma_ratio_50"].tolist() == [30.0]
