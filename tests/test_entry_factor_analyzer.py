from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

import analyzer
from models import AnalysisParams


def make_params(**overrides: Any) -> AnalysisParams:
    base: dict[str, Any] = {
        "data_source_type": "local_parquet",
        "db_path": "",
        "table_name": None,
        "column_overrides": {},
        "excel_sheet_name": None,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "stock_codes": (),
        "gap_direction": "up",
        "entry_factor": "trend_breakout",
        "gap_entry_mode": "strict_break",
        "gap_pct": 2.0,
        "max_gap_filter_pct": 9.9,
        "trend_breakout_lookback": 2,
        "vcb_range_lookback": 2,
        "vcb_breakout_lookback": 2,
        "use_ma_filter": False,
        "fast_ma_period": 5,
        "slow_ma_period": 20,
        "time_stop_days": 1,
        "time_stop_target_pct": -100.0,
        "stop_loss_pct": 50.0,
        "take_profit_pct": 5.0,
        "enable_take_profit": False,
        "enable_profit_drawdown_exit": False,
        "profit_drawdown_pct": 40.0,
        "enable_ma_exit": False,
        "exit_ma_period": 10,
        "ma_exit_batches": 2,
        "partial_exit_enabled": False,
        "partial_exit_count": 2,
        "partial_exit_rules": (),
        "buy_cost_pct": 0.0,
        "sell_cost_pct": 0.0,
        "buy_slippage_pct": 0.0,
        "sell_slippage_pct": 0.0,
        "time_exit_mode": "force_close",
    }
    base.update(overrides)
    return AnalysisParams(**base)


def make_market_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-01",
                    "2024-01-02",
                ]
            ),
            "stock_code": ["000001.SZ", "000001.SZ", "000002.SZ", "000002.SZ"],
            "open": [10.0, 10.5, 20.0, 21.0],
            "high": [10.2, 10.7, 20.2, 21.3],
            "low": [9.8, 10.2, 19.8, 20.6],
            "close": [10.1, 10.6, 20.1, 21.1],
            "volume": [1000.0, 1200.0, 900.0, 1100.0],
        }
    )


def _build_trade(
    *,
    trade_date: date,
    stock_code: str,
    buy_price: float,
    sell_date: str,
    fill_type: str,
) -> dict[str, object]:
    fills = [
        {
            "sell_date": sell_date,
            "sell_price": buy_price,
            "weight": 1.0,
            "exit_type": "force_close",
            "holding_days": 1,
        }
    ]
    return {
        "date": trade_date,
        "stock_code": stock_code,
        "prev_close": buy_price,
        "prev_high": buy_price,
        "prev_low": buy_price,
        "open": buy_price,
        "close": buy_price,
        "volume": 1000.0,
        "gap_pct_vs_prev_close": 0.0,
        "buy_date": str(trade_date),
        "buy_price": buy_price,
        "sell_price": buy_price,
        "sell_date": pd.to_datetime(sell_date).date(),
        "exit_type": "force_close",
        "holding_days": 1,
        "fills": fills,
        "gross_return_pct": 0.0,
        "net_return_pct": 0.0,
        "win_flag": 0,
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "max_profit_pct": 0.0,
        "exit_ma_value": float("nan"),
        "board_ma_value": float("nan"),
        "imported_indicator_exit_value": float("nan"),
        "partial_indicator_rule_label": "",
        "partial_indicator_trigger_value": float("nan"),
        "profit_drawdown_ratio": float("nan"),
        "entry_factor": "trend_breakout",
        "entry_reason": "trend_breakout.up",
        "entry_trigger_price": buy_price,
        "entry_fill_type": fill_type,
        "exit_reason": "force_close: 数据结束强制平仓",
    }


def test_scan_trade_candidates_propagates_skip_counters_and_entry_fields(
    monkeypatch,
) -> None:
    def fake_apply_gap_filters(
        stock_df: pd.DataFrame, _params: AnalysisParams
    ) -> pd.DataFrame:
        enriched = stock_df.sort_values("date").reset_index(drop=True).copy()
        enriched["is_signal"] = True
        return enriched

    def fake_simulate_trade(
        stock_df: pd.DataFrame,
        signal_idx: int,
        _params: AnalysisParams,
        direction: str = "long",
    ) -> tuple[dict[str, object] | None, str | None]:
        del direction
        stock_code = str(stock_df.iloc[signal_idx]["stock_code"])
        if stock_code == "000001.SZ" and signal_idx == 0:
            return None, "entry_not_filled"
        if stock_code == "000002.SZ" and signal_idx == 0:
            return None, "locked_bar_unfillable"
        if stock_code == "000001.SZ":
            return (
                _build_trade(
                    trade_date=date(2024, 1, 3),
                    stock_code=stock_code,
                    buy_price=10.5,
                    sell_date="2024-01-06",
                    fill_type="open",
                ),
                None,
            )
        return (
            _build_trade(
                trade_date=date(2024, 1, 2),
                stock_code=stock_code,
                buy_price=21.0,
                sell_date="2024-01-05",
                fill_type="trigger",
            ),
            None,
        )

    monkeypatch.setattr(analyzer, "apply_gap_filters", fake_apply_gap_filters)
    monkeypatch.setattr(analyzer, "simulate_trade", fake_simulate_trade)

    detail_df, signal_trace_df, rejected_signal_df, stats = (
        analyzer.scan_trade_candidates(make_market_data(), make_params())
    )

    assert stats["signal_count"] == 4
    assert stats["closed_trade_candidates"] == 2
    assert stats["skipped_entry_not_filled"] == 1
    assert stats["skipped_locked_bar_unfillable"] == 1

    assert "entry_factor" in detail_df.columns
    assert "entry_reason" in detail_df.columns
    assert "entry_trigger_price" in detail_df.columns
    assert "entry_fill_type" in detail_df.columns
    assert "exit_reason" in detail_df.columns
    assert set(detail_df["entry_factor"]) == {"trend_breakout"}
    assert set(detail_df["entry_fill_type"]) == {"open", "trigger"}
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty


def test_scan_trade_candidates_keeps_date_stock_sell_date_sort_order(
    monkeypatch,
) -> None:
    def fake_apply_gap_filters(
        stock_df: pd.DataFrame, _params: AnalysisParams
    ) -> pd.DataFrame:
        enriched = stock_df.sort_values("date").reset_index(drop=True).copy()
        enriched["is_signal"] = [False, True]
        return enriched

    def fake_simulate_trade(
        stock_df: pd.DataFrame,
        signal_idx: int,
        _params: AnalysisParams,
        direction: str = "long",
    ) -> tuple[dict[str, object] | None, str | None]:
        del direction
        stock_code = str(stock_df.iloc[signal_idx]["stock_code"])
        if stock_code == "000001.SZ":
            return (
                _build_trade(
                    trade_date=date(2024, 1, 3),
                    stock_code=stock_code,
                    buy_price=10.5,
                    sell_date="2024-01-06",
                    fill_type="open",
                ),
                None,
            )
        return (
            _build_trade(
                trade_date=date(2024, 1, 2),
                stock_code=stock_code,
                buy_price=21.0,
                sell_date="2024-01-05",
                fill_type="trigger",
            ),
            None,
        )

    monkeypatch.setattr(analyzer, "apply_gap_filters", fake_apply_gap_filters)
    monkeypatch.setattr(analyzer, "simulate_trade", fake_simulate_trade)

    detail_df, _, _, _ = analyzer.scan_trade_candidates(
        make_market_data(), make_params()
    )

    observed = detail_df[["date", "stock_code", "sell_date"]].copy()
    observed["date"] = pd.Series(pd.to_datetime(observed["date"])).map(
        lambda ts: ts.date()
    )
    observed["sell_date"] = pd.Series(pd.to_datetime(observed["sell_date"])).map(
        lambda ts: ts.date()
    )
    expected = pd.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "stock_code": ["000002.SZ", "000001.SZ"],
            "sell_date": [
                pd.Timestamp("2024-01-05").date(),
                pd.Timestamp("2024-01-06").date(),
            ],
        }
    )
    pd.testing.assert_frame_equal(observed.reset_index(drop=True), expected)


def test_empty_scan_stats_include_new_skip_counters() -> None:
    detail_df, signal_trace_df, rejected_signal_df, stats = (
        analyzer.scan_trade_candidates(pd.DataFrame(), make_params())
    )

    assert detail_df.empty
    assert signal_trace_df.empty
    assert rejected_signal_df.empty
    assert stats["skipped_entry_not_filled"] == 0
    assert stats["skipped_locked_bar_unfillable"] == 0
    assert stats["skipped_missing_execution_data"] == 0


def test_eshb_missing_execution_data_is_counted_separately(monkeypatch) -> None:
    setup_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02 10:30:00"]),
            "stock_code": ["000001.SZ"],
            "open": [104.5],
            "high": [104.8],
            "low": [104.2],
            "close": [104.6],
            "volume": [120.0],
        }
    )

    def fake_apply_gap_filters(
        stock_df: pd.DataFrame, _params: AnalysisParams
    ) -> pd.DataFrame:
        enriched = stock_df.copy()
        enriched["is_signal"] = [True]
        enriched["entry_trigger_price"] = [105.0]
        enriched["entry_factor"] = ["early_surge_high_base"]
        enriched["setup_pass"] = [True]
        enriched["setup_reason"] = ["eshb.setup"]
        enriched["trigger_pass"] = [True]
        enriched["trigger_reason"] = ["eshb.trigger_pending"]
        enriched["filter_pass"] = [True]
        enriched["ma_filter_pass"] = [pd.NA]
        enriched["atr_filter_pass"] = [pd.NA]
        enriched["board_ma_filter_pass"] = [pd.NA]
        enriched["imported_filter_pass"] = [pd.NA]
        enriched["trade_closed"] = [False]
        enriched["reject_reason_chain"] = [""]
        enriched["execution_skip_reason"] = [""]
        return enriched

    monkeypatch.setattr(analyzer, "apply_gap_filters", fake_apply_gap_filters)
    monkeypatch.setattr(
        analyzer,
        "load_local_parquet_data",
        lambda **_: pd.DataFrame(
            columns=["date", "stock_code", "open", "high", "low", "close", "volume"]
        ),
    )

    detail_df, signal_trace_df, rejected_signal_df, stats = (
        analyzer.scan_trade_candidates(
            setup_data,
            make_params(
                entry_factor="early_surge_high_base",
                timeframe="30m",
                start_date="2024-01-02",
                end_date="2024-01-02",
                time_stop_days=1,
            ),
        )
    )

    assert detail_df.empty
    assert rejected_signal_df.empty
    assert stats["signal_count"] == 1
    assert stats["skipped_missing_execution_data"] == 1
    assert signal_trace_df["execution_skip_reason"].eq("missing_execution_data").any()


def test_analyze_all_stocks_supports_candle_run_strategy_level_stats() -> None:
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
            ),
            "stock_code": ["000001.SZ"] * 4,
            "open": [100.0, 101.2, 104.0, 105.0],
            "high": [101.5, 103.8, 105.5, 107.0],
            "low": [99.8, 101.0, 103.8, 104.8],
            "close": [101.2, 103.5, 105.0, 106.5],
            "volume": [1000.0, 1200.0, 1100.0, 1300.0],
        }
    )
    detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = (
        analyzer.analyze_all_stocks(
            market_data,
            make_params(
                entry_factor="candle_run",
                candle_run_length=2,
                candle_run_min_body_pct=1.0,
                candle_run_total_move_pct=2.0,
            ),
        )
    )

    assert len(detail_df) == 1
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty
    assert not daily_df.empty
    assert not equity_df.empty
    assert detail_df.iloc[0]["entry_factor"] == "candle_run"
    assert int(stats["executed_trades"]) == 1
    assert float(stats["strategy_win_rate_pct"]) == 100.0
    assert float(stats["total_return_pct"]) > 0.0


def test_analyze_all_stocks_supports_short_candle_run_strategy_level_stats() -> None:
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-02-01", "2024-02-02", "2024-02-03", "2024-02-04"]
            ),
            "stock_code": ["000002.SZ"] * 4,
            "open": [100.0, 98.8, 96.8, 96.2],
            "high": [100.2, 99.0, 97.0, 96.5],
            "low": [98.5, 96.8, 95.8, 94.0],
            "close": [98.8, 97.0, 96.2, 94.5],
            "volume": [1000.0, 1100.0, 1200.0, 1250.0],
        }
    )
    detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = (
        analyzer.analyze_all_stocks(
            market_data,
            make_params(
                gap_direction="down",
                entry_factor="candle_run",
                candle_run_length=2,
                candle_run_min_body_pct=1.0,
                candle_run_total_move_pct=2.0,
            ),
        )
    )

    assert len(detail_df) == 1
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty
    assert not daily_df.empty
    assert not equity_df.empty
    assert detail_df.iloc[0]["entry_factor"] == "candle_run"
    assert int(stats["executed_trades"]) == 1
    assert float(stats["strategy_win_rate_pct"]) == 100.0
    assert float(stats["total_return_pct"]) > 0.0


def test_analyze_all_stocks_supports_candle_run_acceleration_strategy_level_stats() -> (
    None
):
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04"]
            ),
            "stock_code": ["000003.SZ"] * 4,
            "open": [100.0, 101.0, 103.2, 104.5],
            "high": [101.2, 103.4, 104.8, 106.0],
            "low": [99.8, 100.9, 103.0, 104.3],
            "close": [101.0, 103.2, 104.5, 105.8],
            "volume": [1000.0, 1100.0, 1200.0, 1300.0],
        }
    )
    detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = (
        analyzer.analyze_all_stocks(
            market_data,
            make_params(
                entry_factor="candle_run_acceleration",
                candle_run_length=2,
                candle_run_min_body_pct=0.5,
                candle_run_total_move_pct=2.0,
            ),
        )
    )

    assert len(detail_df) == 1
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty
    assert not daily_df.empty
    assert not equity_df.empty
    assert detail_df.iloc[0]["entry_factor"] == "candle_run_acceleration"
    assert int(stats["executed_trades"]) == 1
    assert float(stats["strategy_win_rate_pct"]) == 100.0
    assert float(stats["total_return_pct"]) > 0.0


def test_analyze_all_stocks_supports_short_candle_run_acceleration_strategy_level_stats() -> (
    None
):
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-04-01", "2024-04-02", "2024-04-03", "2024-04-04"]
            ),
            "stock_code": ["000004.SZ"] * 4,
            "open": [100.0, 98.8, 96.0, 94.8],
            "high": [100.2, 99.0, 96.2, 95.0],
            "low": [98.5, 95.8, 94.5, 92.8],
            "close": [98.8, 96.0, 94.8, 93.2],
            "volume": [1000.0, 1100.0, 1200.0, 1300.0],
        }
    )
    detail_df, signal_trace_df, rejected_signal_df, daily_df, equity_df, stats = (
        analyzer.analyze_all_stocks(
            market_data,
            make_params(
                gap_direction="down",
                entry_factor="candle_run_acceleration",
                candle_run_length=2,
                candle_run_min_body_pct=0.5,
                candle_run_total_move_pct=2.0,
            ),
        )
    )

    assert len(detail_df) == 1
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty
    assert not daily_df.empty
    assert not equity_df.empty
    assert detail_df.iloc[0]["entry_factor"] == "candle_run_acceleration"
    assert int(stats["executed_trades"]) == 1
    assert float(stats["strategy_win_rate_pct"]) == 100.0
    assert float(stats["total_return_pct"]) > 0.0


def test_scan_trade_candidates_supports_eshb_30m_setup_and_5m_execution(
    monkeypatch,
) -> None:
    setup_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02 09:30:00",
                    "2024-01-02 10:00:00",
                    "2024-01-02 10:30:00",
                    "2024-01-02 11:00:00",
                    "2024-01-02 11:30:00",
                ]
            ),
            "stock_code": ["000001.SZ"] * 5,
            "open": [100.0, 100.0, 104.5, 104.6, 104.8],
            "high": [101.0, 105.0, 104.8, 104.9, 105.0],
            "low": [99.0, 99.8, 104.2, 104.3, 104.7],
            "close": [100.0, 104.5, 104.6, 104.7, 104.9],
            "volume": [100.0, 300.0, 120.0, 110.0, 130.0],
        }
    )
    execution_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02 11:35:00",
                    "2024-01-02 11:40:00",
                    "2024-01-02 11:45:00",
                    "2024-01-02 11:50:00",
                ]
            ),
            "stock_code": ["000001.SZ"] * 4,
            "open": [104.9, 105.0, 105.1, 105.4],
            "high": [105.0, 105.2, 105.5, 105.8],
            "low": [104.8, 104.9, 105.0, 105.2],
            "close": [105.0, 105.1, 105.4, 105.7],
            "volume": [120.0, 130.0, 280.0, 210.0],
        }
    )

    monkeypatch.setattr(analyzer, "load_local_parquet_data", lambda **_: execution_data)

    detail_df, signal_trace_df, rejected_signal_df, stats = (
        analyzer.scan_trade_candidates(
            setup_data,
            make_params(
                entry_factor="early_surge_high_base",
                timeframe="30m",
                start_date="2024-01-02",
                end_date="2024-01-02",
                eshb_open_window_bars=3,
                eshb_base_min_bars=2,
                eshb_base_max_bars=4,
                eshb_surge_min_pct=3.0,
                eshb_max_base_pullback_pct=2.5,
                eshb_max_base_range_pct=1.0,
                eshb_max_anchor_breaks=0,
                eshb_max_anchor_break_depth_pct=0.5,
                eshb_min_open_volume_ratio=1.5,
                eshb_min_breakout_volume_ratio=1.0,
                eshb_trigger_buffer_pct=0.0,
                time_stop_days=1,
            ),
        )
    )

    assert len(detail_df) == 1
    assert int(stats["signal_count"]) == 1
    assert int(stats["closed_trade_candidates"]) == 1
    assert not signal_trace_df.empty
    assert rejected_signal_df.empty
    assert detail_df.iloc[0]["entry_factor"] == "early_surge_high_base"
    assert detail_df.iloc[0]["entry_fill_type"] == "open"


def test_scan_trade_candidates_keeps_rejected_signal_reason_chain(monkeypatch) -> None:
    market_data = make_market_data()

    def fake_apply_gap_filters(
        stock_df: pd.DataFrame, _params: AnalysisParams
    ) -> pd.DataFrame:
        enriched = stock_df.sort_values("date").reset_index(drop=True).copy()
        enriched["entry_factor"] = "trend_breakout"
        enriched["entry_trigger_price"] = [pd.NA] * len(enriched)
        enriched["gap_pct_vs_prev_close"] = [0.0] * len(enriched)
        enriched["core_signal_pass"] = [False, True]
        enriched["filter_pass"] = [False, False]
        enriched["reject_reason_chain"] = ["", "快慢线过滤未通过"]
        enriched["is_signal"] = [False, False]
        return enriched

    monkeypatch.setattr(analyzer, "apply_gap_filters", fake_apply_gap_filters)

    detail_df, signal_trace_df, rejected_signal_df, stats = (
        analyzer.scan_trade_candidates(
            market_data,
            make_params(entry_factor="trend_breakout", trend_breakout_lookback=1),
        )
    )

    assert detail_df.empty
    assert not signal_trace_df.empty
    assert int(stats["core_signal_count"]) >= 1
    assert int(stats["rejected_signal_count"]) >= 1
    assert not rejected_signal_df.empty
    assert "快慢线过滤未通过" in str(rejected_signal_df.iloc[0]["reject_reason_chain"])


def test_scan_trade_candidates_keeps_short_gross_return_from_rules() -> None:
    market_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-02-01", "2024-02-02", "2024-02-03", "2024-02-04"]
            ),
            "stock_code": ["000002.SZ"] * 4,
            "open": [100.0, 98.8, 96.8, 96.2],
            "high": [100.2, 99.0, 97.0, 96.5],
            "low": [98.5, 96.8, 95.8, 94.0],
            "close": [98.8, 97.0, 96.2, 94.5],
            "volume": [1000.0, 1100.0, 1200.0, 1250.0],
        }
    )

    detail_df, signal_trace_df, _, stats = analyzer.scan_trade_candidates(
        market_data,
        make_params(
            gap_direction="down",
            entry_factor="candle_run",
            candle_run_length=2,
            candle_run_min_body_pct=1.0,
            candle_run_total_move_pct=2.0,
        ),
    )

    assert int(stats["closed_trade_candidates"]) == 1
    assert not signal_trace_df.empty
    assert float(detail_df.iloc[0]["gross_return_pct"]) > 0.0


def test_eshb_execution_loads_required_indicator_keys(monkeypatch) -> None:
    captured: dict[str, object] = {}
    execution_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02 11:35:00"]),
            "stock_code": ["000001.SZ"],
            "open": [104.9],
            "high": [105.0],
            "low": [104.8],
            "close": [105.0],
            "volume": [120.0],
            "custom_strength_score": [60.0],
        }
    )

    def fake_load_local_parquet_data(**kwargs):
        captured.update(kwargs)
        return execution_data

    monkeypatch.setattr(
        analyzer, "load_local_parquet_data", fake_load_local_parquet_data
    )

    params = make_params(
        entry_factor="early_surge_high_base",
        timeframe="30m",
        partial_exit_enabled=True,
        partial_exit_count=2,
        partial_exit_rules=(
            analyzer.PartialExitRule(
                True,
                50,
                "indicator_threshold",
                1,
                indicator_key="custom_strength",
                indicator_column="custom_strength_score",
                indicator_operator=">=",
                indicator_threshold=50.0,
            ),
            analyzer.PartialExitRule(True, 50, "fixed_tp", 2, target_profit_pct=5.0),
        ),
    )

    analyzer._build_scan_execution_context(make_market_data(), params)
    assert captured["timeframe"] == "5m"
    assert captured["indicator_keys"] == ("custom_strength",)


def test_build_trade_behavior_overview_summarizes_behavior_metrics() -> None:
    detail_df = pd.DataFrame(
        {
            "trade_no": [1, 2],
            "entry_fill_type": ["trigger", "open"],
            "fill_count": [2, 1],
            "win_flag": [1, 0],
            "net_return_pct": [5.0, -2.0],
            "mfe_pct": [8.0, 1.0],
            "mae_pct": [-1.5, -4.0],
            "profit_drawdown_ratio": [0.4, 0.0],
        }
    )

    overview = analyzer.build_trade_behavior_overview(detail_df)

    assert len(overview) == 1
    row = overview.iloc[0]
    assert int(row["executed_trades"]) == 2
    assert abs(float(row["win_rate_pct"]) - 50.0) < 1e-12
    assert abs(float(row["avg_give_back_pct"]) - 3.0) < 1e-12
    assert abs(float(row["trigger_fill_share_pct"]) - 50.0) < 1e-12
    assert abs(float(row["multi_fill_trade_share_pct"]) - 50.0) < 1e-12


def test_build_drawdown_diagnostics_returns_episodes_and_reason_contributors() -> None:
    equity_df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                ]
            ),
            "net_value": [1.0, 1.05, 1.0, 0.95, 1.05],
            "drawdown_pct": [0.0, 0.0, -4.7619, -9.5238, 0.0],
        }
    )
    strategy_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "sell_date": pd.to_datetime(["2024-01-03", "2024-01-05"]),
            "stock_code": ["000001.SZ", "000002.SZ"],
            "entry_reason": ["trend_breakout.up", "candle_run.up"],
            "net_return_pct": [-4.0, -6.0],
            "mfe_pct": [1.0, 0.5],
            "mae_pct": [-5.0, -7.0],
        }
    )

    episodes_df, contributors_df = analyzer.build_drawdown_diagnostics(
        equity_df, strategy_df
    )

    assert len(episodes_df) == 1
    assert float(episodes_df.iloc[0]["peak_to_trough_pct"]) > 9.0
    assert int(episodes_df.iloc[0]["trade_count"]) == 2
    assert episodes_df.iloc[0]["dominant_entry_reason"] == "candle_run.up"
    assert not contributors_df.empty
    assert contributors_df.iloc[0]["entry_reason"] == "candle_run.up"


def test_build_drawdown_diagnostics_by_batch_keeps_per_stock_isolation() -> None:
    equity_df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]
            ),
            "net_value": [1.0, 0.9, 1.0, 1.1],
            "drawdown_pct": [0.0, -10.0, 0.0, 0.0],
            "batch_stock_code": ["000001.SZ", "000001.SZ", "000002.SZ", "000002.SZ"],
        }
    )
    strategy_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01"]),
            "sell_date": pd.to_datetime(["2024-01-02"]),
            "stock_code": ["000001.SZ"],
            "batch_stock_code": ["000001.SZ"],
            "entry_reason": ["trend_breakout.up"],
            "net_return_pct": [-10.0],
            "mfe_pct": [0.0],
            "mae_pct": [-10.0],
        }
    )

    episodes_df, contributors_df = analyzer.build_drawdown_diagnostics_by_batch(
        equity_df, strategy_df
    )

    assert len(episodes_df) == 1
    assert episodes_df.iloc[0]["batch_stock_code"] == "000001.SZ"
    assert contributors_df.iloc[0]["batch_stock_code"] == "000001.SZ"


def test_build_trade_anomaly_queue_flags_giveback_loss_and_stall_patterns() -> None:
    detail_df = pd.DataFrame(
        {
            "trade_no": [1, 2, 3],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "stock_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
            "entry_reason": [
                "trend_breakout.up",
                "candle_run.up",
                "trend_breakout.up",
            ],
            "exit_reason": [
                "profit_drawdown: 利润回撤触发",
                "stop_loss: 全仓止损触发",
                "time_exit: 时间退出触发",
            ],
            "holding_days": [2, 1, 10],
            "net_return_pct": [1.0, -8.0, 0.2],
            "mfe_pct": [9.0, 0.5, 0.8],
            "mae_pct": [-1.0, -9.5, -0.5],
        }
    )

    params = make_params(
        enable_take_profit=True,
        take_profit_pct=5.0,
        enable_profit_drawdown_exit=True,
        enable_atr_trailing_exit=True,
        atr_trailing_period=14,
        atr_trailing_multiplier=3.0,
        time_stop_days=5,
        stop_loss_pct=5.0,
    )

    anomaly_df = analyzer.build_trade_anomaly_queue(detail_df, params, limit=10)

    assert not anomaly_df.empty
    assert set(anomaly_df["anomaly_type"]).issuperset(
        {
            "fixed_tp_review",
            "profit_drawdown_review",
            "atr_trailing_review",
            "long_hold_stall",
        }
    )
    fixed_tp_row = anomaly_df.loc[anomaly_df["anomaly_type"] == "fixed_tp_review"].iloc[
        0
    ]
    assert abs(float(fixed_tp_row["activation_threshold_pct"]) - 5.0) < 1e-12
    assert float(fixed_tp_row["holding_anchor_mfe_pct"]) > 4.0
