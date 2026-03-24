from __future__ import annotations

import sys
from types import SimpleNamespace

import pandas as pd

from data.providers.akshare_provider import AkshareProvider


def test_fetch_daily_bars_fallback_sina_to_tencent(monkeypatch):
    calls: list[str] = []

    def sina_fail(**kwargs):
        calls.append("sina")
        raise RuntimeError("sina down")

    def tx_ok(**kwargs):
        calls.append("tencent")
        return pd.DataFrame(
            {
                "日期": ["2024-01-02", "2024-01-03"],
                "开盘": [1.0, 2.0],
                "最高": [1.2, 2.2],
                "最低": [0.9, 1.9],
                "收盘": [1.1, 2.1],
                "成交量": [100.0, 200.0],
                "成交额": [1000.0, 2000.0],
            }
        )

    def em_should_not_run(**kwargs):
        calls.append("eastmoney")
        raise AssertionError("eastmoney should not be called when tencent succeeds")

    fake_ak = SimpleNamespace(
        stock_zh_a_daily=sina_fail,
        stock_zh_a_hist_tx=tx_ok,
        stock_zh_a_hist=em_should_not_run,
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    result = AkshareProvider.fetch_daily_bars("000001.SZ", "2024-01-01", "2024-01-31", adjust="qfq")
    assert not result.empty
    assert calls == ["sina", "tencent"]
    assert result["symbol"].nunique() == 1
    assert result["symbol"].iloc[0] == "000001.SZ"


def test_fetch_daily_bars_raises_after_all_fail(monkeypatch):
    def always_fail(**kwargs):
        raise RuntimeError("network error")

    fake_ak = SimpleNamespace(
        stock_zh_a_daily=always_fail,
        stock_zh_a_hist_tx=always_fail,
        stock_zh_a_hist=always_fail,
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    try:
        AkshareProvider.fetch_daily_bars("000001.SZ", "2024-01-01", "2024-01-31", adjust="qfq")
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "sina->tencent->eastmoney" in str(exc)

    assert raised


def test_fetch_daily_bars_routes_etf_to_etf_hist_with_volume(monkeypatch):
    calls: list[str] = []

    def etf_ok(**kwargs):
        calls.append("etf")
        return pd.DataFrame(
            {
                "日期": ["2024-01-02", "2024-01-03"],
                "开盘": [1.0, 1.1],
                "最高": [1.2, 1.3],
                "最低": [0.9, 1.0],
                "收盘": [1.1, 1.2],
                "成交量": [10000.0, 20000.0],
                "成交额": [1_000_000.0, 2_000_000.0],
            }
        )

    fake_ak = SimpleNamespace(fund_etf_hist_em=etf_ok)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    result = AkshareProvider.fetch_daily_bars(
        "510300.SH", "2024-01-01", "2024-01-31", adjust="qfq"
    )

    assert calls == ["etf"]
    assert result["symbol"].iloc[0] == "510300.SH"
    assert result["volume"].tolist() == [10000.0, 20000.0]


def test_fetch_daily_bars_routes_index_to_index_hist_with_volume(monkeypatch):
    calls: list[str] = []

    def index_ok(**kwargs):
        calls.append("index")
        return pd.DataFrame(
            {
                "日期": ["2024-01-02", "2024-01-03"],
                "开盘": [5000.0, 5050.0],
                "最高": [5060.0, 5100.0],
                "最低": [4980.0, 5030.0],
                "收盘": [5050.0, 5080.0],
                "成交量": [123456789.0, 223456789.0],
                "成交额": [987654321.0, 1087654321.0],
            }
        )

    fake_ak = SimpleNamespace(index_zh_a_hist=index_ok)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    result = AkshareProvider.fetch_daily_bars(
        "000852.SH", "2024-01-01", "2024-01-31", adjust="qfq"
    )

    assert calls == ["index"]
    assert result["symbol"].iloc[0] == "000852.SH"
    assert result["volume"].tolist() == [123456789.0, 223456789.0]
