from __future__ import annotations

import streamlit as st


def render_backtest_summary_cards(
    *,
    stock_scope: str,
    backtest_range: str,
    source_period: str,
    source_caption: str,
    strategy_summary: str,
    strategy_caption: str,
) -> None:
    cols = st.columns(4)
    cols[0].metric("股票池", stock_scope)
    cols[0].caption("当前研究标的范围")
    cols[1].metric("回测区间", backtest_range)
    cols[1].caption("左侧快捷区间和日期输入同步生效")
    cols[2].metric("数据源/周期", source_period)
    cols[2].caption(source_caption)
    cols[3].metric("当前策略", strategy_summary)
    cols[3].caption(strategy_caption)
