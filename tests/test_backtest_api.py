from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

import analyzer as api


@dataclass(frozen=True)
class _FakeScanConfig:
    enabled: bool
    axes: tuple[object, ...] = ()


@dataclass(frozen=True)
class _FakeParams:
    stock_codes: tuple[str, ...]
    scan_config: _FakeScanConfig


def _empty_diag_frame() -> pd.DataFrame:
    return pd.DataFrame()


def test_run_backtest_combined_wraps_analyzer_outputs(monkeypatch) -> None:
    monkeypatch.setattr(
        api,
        "analyze_all_stocks",
        lambda all_data, params: (
            pd.DataFrame({"stock_code": ["000001.SZ"]}),
            pd.DataFrame({"stock_code": ["000001.SZ"], "setup_pass": [True]}),
            pd.DataFrame({"stock_code": []}),
            pd.DataFrame({"date": ["2024-01-01"]}),
            pd.DataFrame({"date": ["2024-01-01"], "net_value": [1.0]}),
            {"total_return_pct": 1.2},
        ),
    )
    monkeypatch.setattr(
        api, "build_trade_behavior_overview", lambda detail_df: _empty_diag_frame()
    )
    monkeypatch.setattr(
        api,
        "build_drawdown_diagnostics",
        lambda equity_df, detail_df: (_empty_diag_frame(), _empty_diag_frame()),
    )
    monkeypatch.setattr(
        api, "build_trade_anomaly_queue", lambda detail_df, params: _empty_diag_frame()
    )

    result = api.run_backtest(
        pd.DataFrame({"stock_code": ["000001.SZ"]}),
        _FakeParams(stock_codes=("000001.SZ",), scan_config=_FakeScanConfig(False)),
    )

    assert list(result.detail_df["stock_code"]) == ["000001.SZ"]
    assert result.stats["total_return_pct"] == 1.2
    assert result.batch_backtest_mode == "combined"
    assert result.scan_df.empty


def test_run_backtest_scan_mode_returns_best_scan_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        api,
        "run_parameter_scan",
        lambda all_data, params: (
            pd.DataFrame({"scan_id": [1], "rank": [1]}),
            pd.DataFrame({"stock_code": ["000001.SZ"]}),
            pd.DataFrame({"stock_code": ["000001.SZ"], "setup_pass": [True]}),
            pd.DataFrame({"stock_code": []}),
            pd.DataFrame({"date": ["2024-01-01"]}),
            pd.DataFrame({"date": ["2024-01-01"], "net_value": [1.0]}),
            {"total_return_pct": 2.5},
            {"gap_pct": 3.0},
        ),
    )
    monkeypatch.setattr(
        api, "build_trade_behavior_overview", lambda detail_df: _empty_diag_frame()
    )
    monkeypatch.setattr(
        api,
        "build_drawdown_diagnostics",
        lambda equity_df, detail_df: (_empty_diag_frame(), _empty_diag_frame()),
    )
    monkeypatch.setattr(
        api, "build_trade_anomaly_queue", lambda detail_df, params: _empty_diag_frame()
    )

    result = api.run_backtest(
        pd.DataFrame({"stock_code": ["000001.SZ"]}),
        _FakeParams(
            stock_codes=("000001.SZ",),
            scan_config=_FakeScanConfig(True, axes=(object(),)),
        ),
    )

    assert not result.scan_df.empty
    assert result.best_scan_overrides == {"gap_pct": 3.0}
    assert result.stats["total_return_pct"] == 2.5


def test_run_backtest_per_stock_aggregates_batch_outputs(monkeypatch) -> None:
    def _fake_analyze(stock_data: pd.DataFrame, params: _FakeParams):
        stock_code = params.stock_codes[0]
        return (
            pd.DataFrame({"stock_code": [stock_code]}),
            pd.DataFrame({"stock_code": [stock_code], "setup_pass": [True], "trigger_pass": [True], "filter_pass": [True]}),
            pd.DataFrame({"stock_code": []}),
            pd.DataFrame({"date": ["2024-01-01"]}),
            pd.DataFrame({"date": ["2024-01-01"], "net_value": [1.0]}),
            {
                "signal_count": 1,
                "executed_trades": 1,
                "total_return_pct": 2.0 if stock_code == "000001.SZ" else -1.0,
                "strategy_win_rate_pct": 100.0 if stock_code == "000001.SZ" else 0.0,
                "max_drawdown_pct": 3.0,
                "final_net_value": 1.0,
                "avg_holding_days": 2.0,
                "profit_risk_ratio": 1.0,
                "avg_mfe_pct": 2.0,
                "avg_mae_pct": 1.0,
                "trade_return_volatility_pct": 0.5,
            },
        )

    monkeypatch.setattr(api, "analyze_all_stocks", _fake_analyze)
    monkeypatch.setattr(
        api, "build_trade_behavior_overview", lambda detail_df: _empty_diag_frame()
    )
    monkeypatch.setattr(
        api,
        "build_drawdown_diagnostics",
        lambda equity_df, detail_df: (_empty_diag_frame(), _empty_diag_frame()),
    )
    monkeypatch.setattr(
        api, "build_trade_anomaly_queue", lambda detail_df, params: _empty_diag_frame()
    )

    result = api.run_backtest(
        pd.DataFrame({"stock_code": ["000001.SZ", "000002.SZ"]}),
        _FakeParams(
            stock_codes=("000001.SZ", "000002.SZ"),
            scan_config=_FakeScanConfig(False),
        ),
        batch_mode="per_stock",
    )

    assert result.batch_backtest_mode == "per_stock"
    assert len(result.per_stock_stats_df) == 2
    assert "batch_stock_code" in result.detail_df.columns
    assert result.stats["signal_count"] == 2.0
