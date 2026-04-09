from __future__ import annotations

import streamlit as st


def render_data_prep_page_header() -> None:
    st.markdown(
        """
        <div class='app-hero'>
            <h1>数据准备页</h1>
            <p>在这里完成离线行情更新、TDX 路径选择和本地指标导入。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_data_prep_sidebar_intro() -> None:
    st.sidebar.header("运行设置")
    st.sidebar.caption("左侧聚焦数据更新与指标导入，回测规则留在回测工作台。")
