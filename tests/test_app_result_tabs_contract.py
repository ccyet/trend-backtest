from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _between(source: str, start: str, end: str) -> str:
    start_idx = source.index(start)
    end_idx = source.index(end, start_idx)
    return source[start_idx:end_idx]


def test_result_tabs_include_dedicated_diagnostics_partition() -> None:
    source = (ROOT / "app.py").read_text(encoding="utf-8")

    assert "tab_names = build_result_tab_names(" in source
    assert "tab_summary, tab_curve, tab_diagnostics, tab_details = tabs[:4]" in source


def test_diagnostics_blocks_are_centralized_under_diagnostics_tab() -> None:
    source = (ROOT / "app.py").read_text(encoding="utf-8")

    summary_block = _between(source, "    with tab_summary:", "    with tab_curve:")
    curve_block = _between(source, "    with tab_curve:", "    with tab_diagnostics:")
    diagnostics_block = _between(
        source, "    with tab_diagnostics:", "    with tab_details:"
    )
    details_block = _between(source, "    with tab_details:", "    if len(tabs) ==")

    assert "交易行为总览" not in summary_block
    assert "回撤诊断" not in curve_block
    assert "异常交易队列" not in details_block

    assert "交易行为总览" in diagnostics_block
    assert "回撤诊断" in diagnostics_block
    assert "异常交易队列" in diagnostics_block
    assert "format_trade_behavior_for_display" in diagnostics_block
    assert "format_drawdown_episodes_for_display" in diagnostics_block
    assert "format_drawdown_contributors_for_display" in diagnostics_block
    assert "format_anomaly_queue_for_display" in diagnostics_block


def test_per_stock_equity_curve_uses_identifiable_grouped_traces() -> None:
    source = (ROOT / "app.py").read_text(encoding="utf-8")

    assert 'batch_backtest_mode == "per_stock"' in source
    assert '"batch_stock_code" in chart_df.columns' in source
    assert 'chart_df["batch_stock_display"] = chart_df["batch_stock_code"].map(' in source
    assert 'color="batch_stock_display"' in source
    assert 'line_group="batch_stock_display"' in source
    assert "hovermode=\"x unified\"" in source


def test_data_prep_and_backtest_overview_are_sidebar_only_contract() -> None:
    source = (ROOT / "app.py").read_text(encoding="utf-8")

    assert 'page_mode_options = ["回测工作台", "数据准备页", "交易配置说明"]' in source
    assert 'if page_mode == "数据准备页":' in source
    assert 'key="goto_data_prep"' in source
    assert 'on_click=request_page_change' in source
    assert 'section_header("配置摘要"' in source
    assert 'section_header(ADVANCED_SECTION_TITLE, ADVANCED_SECTION_CAPTION)' in source
