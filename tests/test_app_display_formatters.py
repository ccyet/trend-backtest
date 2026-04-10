from __future__ import annotations

from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

import app
from ui.components import results_view


def test_summarize_trade_decision_chain_exposes_entry_and_exit_flow() -> None:
    detail_df = pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "stock_code": "000001.SZ",
                "entry_reason": "gap.strict_break.up",
                "entry_fill_type": "trigger",
                "entry_trigger_price": 10.5,
                "exit_reason": "固定止盈+ATR 跟踪止盈",
                "fill_detail_json": '[{"exit_type":"fixed_tp","weight":0.5,"sell_price":11.2},{"exit_type":"atr_trailing","weight":0.5,"sell_price":11.0}]',
            }
        ]
    )

    summary_df = app.summarize_trade_decision_chain(detail_df)

    assert list(summary_df.columns) == [
        "信号日期",
        "股票代码",
        "开仓决策链",
        "离场决策链",
    ]
    assert "gap.strict_break.up" in summary_df.iloc[0]["开仓决策链"]
    assert "触发成交 @ 10.5" in summary_df.iloc[0]["开仓决策链"]
    assert "固定止盈+ATR 跟踪止盈" in summary_df.iloc[0]["离场决策链"]
    assert "fixed_tp: 50% @ 11.2" in summary_df.iloc[0]["离场决策链"]


def test_summarize_local_inventory_returns_compact_overview() -> None:
    inventory_df = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "timeframe": "1d",
                "row_count": 120,
                "updated_at": "2024-01-03 15:00:00",
            },
            {
                "symbol": "000002.SZ",
                "timeframe": "1d",
                "row_count": 80,
                "updated_at": "2024-01-04 15:00:00",
            },
            {
                "symbol": "000001.SZ",
                "timeframe": "30m",
                "row_count": 300,
                "updated_at": "2024-01-04 15:05:00",
            },
        ]
    )

    summary_df = app.summarize_local_inventory(inventory_df)

    assert list(summary_df.columns) == ["周期", "标的数", "总行数", "最近更新"]
    assert set(summary_df["周期"]) == {"1d", "30m"}
    daily_row = summary_df.loc[summary_df["周期"] == "1d"].iloc[0]
    assert int(daily_row["标的数"]) == 2
    assert int(daily_row["总行数"]) == 200


def test_clear_update_log_removes_existing_file(tmp_path: Path, monkeypatch) -> None:
    metadata_dir = tmp_path / "data" / "market" / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    log_file = metadata_dir / "update_log.parquet"
    log_file.write_bytes(b"demo")

    monkeypatch.chdir(tmp_path)

    assert app.clear_update_log() is True
    assert not log_file.exists()


def test_summarize_secondary_take_profit_logic_lists_execution_order() -> None:
    lines = results_view.summarize_secondary_take_profit_logic()

    assert any("未启用分批止盈" in line for line in lines)
    assert any("ATR 跟踪止盈之后" in line for line in lines)


def test_format_signal_trace_for_display_formats_filter_track() -> None:
    signal_trace_df = pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "stock_code": "000001.SZ",
                "entry_factor": "trend_breakout",
                "entry_trigger_price": 10.5,
                "setup_pass": True,
                "setup_reason": "趋势突破 setup 成立：已生成过去窗口突破触发价",
                "trigger_pass": True,
                "trigger_reason": "趋势突破真实触发：当根价格突破 trigger 价",
                "filter_pass": False,
                "ma_filter_pass": True,
                "atr_filter_pass": False,
                "board_ma_filter_pass": pd.NA,
                "imported_filter_pass": pd.NA,
                "trade_closed": False,
                "reject_reason_chain": "ATR过滤未通过",
                "execution_skip_reason": "",
            }
        ]
    )

    display_df = results_view.format_signal_trace_for_display(
        signal_trace_df,
        format_timestamp=app.format_timestamp_for_display,
        format_number=app.format_number,
    )

    assert "快慢线过滤" in display_df.columns
    assert display_df.iloc[0]["形态原因"].startswith("趋势突破 setup 成立")
    assert display_df.iloc[0]["触发原因"].startswith("趋势突破真实触发")
    assert display_df.iloc[0]["快慢线过滤"] == "通过"
    assert display_df.iloc[0]["ATR过滤"] == "未通过"
    assert display_df.iloc[0]["板块均线过滤"] == "未启用"


def test_format_rejected_signal_for_display_formats_basic_columns() -> None:
    rejected_df = pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "stock_code": "000001.SZ",
                "entry_factor": "gap",
                "entry_trigger_price": 10.5,
                "gap_pct_vs_prev_close": 3.2,
                "reject_reason_chain": "ATR过滤未通过",
            }
        ]
    )

    display_df = results_view.format_rejected_signal_for_display(
        rejected_df,
        format_timestamp=app.format_timestamp_for_display,
        format_number=app.format_number,
        format_percent=app.format_percent,
    )

    assert list(display_df.columns) == [
        "信号日期",
        "股票代码",
        "入场因子",
        "触发价",
        "相对昨收跳空幅度",
        "拦截原因链",
    ]


def test_render_trade_explanations_keeps_trace_sections_without_trades() -> None:
    signal_trace_df = pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "stock_code": "000001.SZ",
                "entry_factor": "trend_breakout",
                "entry_trigger_price": 10.5,
                "setup_pass": True,
                "setup_reason": "趋势突破 setup 成立：已生成过去窗口突破触发价",
                "trigger_pass": True,
                "trigger_reason": "趋势突破真实触发：当根价格突破 trigger 价",
                "filter_pass": False,
                "ma_filter_pass": True,
                "atr_filter_pass": False,
                "board_ma_filter_pass": pd.NA,
                "imported_filter_pass": pd.NA,
                "trade_closed": False,
                "reject_reason_chain": "ATR过滤未通过",
                "execution_skip_reason": "",
            }
        ]
    )

    results_view.render_trade_explanations(
        signal_trace_df=signal_trace_df,
        rejected_signal_df=pd.DataFrame(),
        detail_df=pd.DataFrame(),
        stats={},
        section_header=lambda title, desc: None,
        summarize_signal_funnel=lambda stats, df: pd.DataFrame(
            [{"阶段": "真实触发", "数量": 1}]
        ),
        summarize_filter_stack=lambda **kwargs: ["趋势突破", "ATR过滤"],
        summarize_trade_decision_chain=lambda df: pd.DataFrame(),
        dataframe_stretch=lambda *args, **kwargs: None,
        format_timestamp=app.format_timestamp_for_display,
        format_number=app.format_number,
        format_percent=app.format_percent,
        entry_factor="trend_breakout",
        use_ma_filter=False,
        fast_ma_period=5,
        slow_ma_period=20,
        enable_atr_filter=True,
        min_atr_filter_pct=1.0,
        max_atr_filter_pct=5.0,
        enable_board_ma_filter=False,
        board_ma_filter_line="20",
        board_ma_filter_operator=">=",
        board_ma_filter_threshold=50.0,
        imported_filter_count=0,
    )


def test_filter_signal_trace_tolerates_missing_optional_columns() -> None:
    def _render() -> None:
        import pandas as local_pd

        signal_trace_df = local_pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "entry_factor": "trend_breakout",
                }
            ]
        )
        from ui.components import results_view as local_results_view

        local_results_view.filter_signal_trace(signal_trace_df)

    at = AppTest.from_function(_render, default_timeout=10)
    at.run()

    assert len(at.exception) == 0
