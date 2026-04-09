from __future__ import annotations

import streamlit as st


def render_backtest_page_header() -> None:
    st.markdown(
        """
        <div class='app-hero'>
            <h1>Gap_test 回测系统</h1>
            <p>按范围、策略、风控到结果的顺序完成研究回测。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_backtest_sidebar_intro() -> None:
    st.sidebar.header("运行设置")
    st.sidebar.caption("左侧聚焦回测范围与启动入口，详细规则放在主区域。")
