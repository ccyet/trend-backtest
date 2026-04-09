from __future__ import annotations

import pandas as pd
import streamlit as st


def build_result_tab_names(has_scan_result: bool) -> list[str]:
    tab_names = ["📊 绩效总览", "📈 资金曲线", "🩺 交易诊断", "📝 交易明细"]
    if has_scan_result:
        tab_names.append("🔎 参数扫描")
    return tab_names


def render_results_empty_state(section_header: callable) -> None:
    st.divider()
    section_header(
        "结果区域", "运行回测后，这里会保留总览、曲线、诊断、明细和参数扫描标签页。"
    )
    st.info("请先在左侧确认回测范围与数据源，再点击“开始回测”。")


def summarize_secondary_take_profit_logic() -> list[str]:
    return [
        "次级固定止盈只作用于未启用分批止盈的整笔仓位。",
        "执行顺序位于全仓止损、分批退出、板块均线离场、导入指标整笔离场之后。",
        "在旧版整笔退出链内，它排在利润回撤、均线离场、ATR 跟踪止盈之后。",
        "一旦触发，会按固定止盈价对剩余全仓一次性平掉，不会和分批 fixed_tp 叠加执行。",
    ]


def build_rejected_signal_column_config() -> dict[str, object]:
    return {
        "信号日期": st.column_config.TextColumn("信号日期", width="small"),
        "股票代码": st.column_config.TextColumn("股票代码", width="small"),
        "入场因子": st.column_config.TextColumn("入场因子", width="medium"),
        "触发价": st.column_config.TextColumn("触发价", width="small"),
        "相对昨收跳空幅度": st.column_config.TextColumn("相对昨收跳空幅度", width="small"),
        "拦截原因链": st.column_config.TextColumn("拦截原因链", width="large"),
    }


def build_execution_skip_column_config() -> dict[str, object]:
    return {
        "信号日期": st.column_config.TextColumn("信号日期", width="small"),
        "股票代码": st.column_config.TextColumn("股票代码", width="small"),
        "入场因子": st.column_config.TextColumn("入场因子", width="medium"),
        "触发价": st.column_config.TextColumn("触发价", width="small"),
        "失败原因": st.column_config.TextColumn("失败原因", width="large"),
    }


def build_signal_trace_column_config() -> dict[str, object]:
    return {
        "信号日期": st.column_config.TextColumn("信号日期", width="small"),
        "股票代码": st.column_config.TextColumn("股票代码", width="small"),
        "入场因子": st.column_config.TextColumn("入场因子", width="medium"),
        "触发价": st.column_config.TextColumn("触发价", width="small"),
        "形态成立": st.column_config.TextColumn("形态成立", width="small"),
        "形态原因": st.column_config.TextColumn("形态原因", width="large"),
        "真实触发": st.column_config.TextColumn("真实触发", width="small"),
        "触发原因": st.column_config.TextColumn("触发原因", width="large"),
        "过滤放行": st.column_config.TextColumn("过滤放行", width="small"),
        "快慢线过滤": st.column_config.TextColumn("快慢线过滤", width="small"),
        "ATR过滤": st.column_config.TextColumn("ATR过滤", width="small"),
        "板块均线过滤": st.column_config.TextColumn("板块均线过滤", width="small"),
        "导入指标过滤": st.column_config.TextColumn("导入指标过滤", width="small"),
        "形成平仓交易": st.column_config.TextColumn("形成平仓交易", width="small"),
        "拦截原因链": st.column_config.TextColumn("拦截原因链", width="large"),
        "成交失败原因": st.column_config.TextColumn("成交失败原因", width="large"),
    }


def _format_filter_pass_cell(value: object) -> str:
    if pd.isna(value):
        return "未启用"
    return "通过" if bool(value) else "未通过"


def format_rejected_signal_for_display(
    rejected_df: pd.DataFrame,
    *,
    format_timestamp: callable,
    format_number: callable,
    format_percent: callable,
) -> pd.DataFrame:
    if rejected_df.empty:
        return rejected_df
    display_df = rejected_df.copy()
    if "date" in display_df.columns:
        display_df["date"] = display_df["date"].map(format_timestamp)
    if "entry_trigger_price" in display_df.columns:
        display_df["entry_trigger_price"] = display_df["entry_trigger_price"].map(format_number)
    if "gap_pct_vs_prev_close" in display_df.columns:
        display_df["gap_pct_vs_prev_close"] = display_df["gap_pct_vs_prev_close"].map(format_percent)
    return display_df.rename(
        columns={
            "date": "信号日期",
            "stock_code": "股票代码",
            "entry_factor": "入场因子",
            "entry_trigger_price": "触发价",
            "gap_pct_vs_prev_close": "相对昨收跳空幅度",
            "reject_reason_chain": "拦截原因链",
        }
    )


def format_execution_skip_for_display(
    signal_trace_df: pd.DataFrame,
    *,
    format_timestamp: callable,
    format_number: callable,
) -> pd.DataFrame:
    if signal_trace_df.empty:
        return signal_trace_df
    filtered = signal_trace_df.loc[
        signal_trace_df["execution_skip_reason"].fillna("").astype(str).ne(""),
        ["date", "stock_code", "entry_factor", "entry_trigger_price", "execution_skip_reason"],
    ].copy()
    if filtered.empty:
        return filtered
    filtered["date"] = filtered["date"].map(format_timestamp)
    filtered["entry_trigger_price"] = filtered["entry_trigger_price"].map(format_number)
    return filtered.rename(
        columns={
            "date": "信号日期",
            "stock_code": "股票代码",
            "entry_factor": "入场因子",
            "entry_trigger_price": "触发价",
            "execution_skip_reason": "失败原因",
        }
    )


def format_signal_trace_for_display(
    signal_trace_df: pd.DataFrame,
    *,
    format_timestamp: callable,
    format_number: callable,
) -> pd.DataFrame:
    if signal_trace_df.empty:
        return signal_trace_df
    display_df = signal_trace_df.copy()
    display_df["date"] = display_df["date"].map(format_timestamp)
    display_df["entry_trigger_price"] = display_df["entry_trigger_price"].map(format_number)
    for column in [
        "setup_pass",
        "trigger_pass",
        "filter_pass",
        "ma_filter_pass",
        "atr_filter_pass",
        "board_ma_filter_pass",
        "imported_filter_pass",
        "trade_closed",
    ]:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(_format_filter_pass_cell)
    return display_df.rename(
        columns={
            "date": "信号日期",
            "stock_code": "股票代码",
            "entry_factor": "入场因子",
            "entry_trigger_price": "触发价",
            "setup_pass": "形态成立",
            "setup_reason": "形态原因",
            "trigger_pass": "真实触发",
            "trigger_reason": "触发原因",
            "filter_pass": "过滤放行",
            "ma_filter_pass": "快慢线过滤",
            "atr_filter_pass": "ATR过滤",
            "board_ma_filter_pass": "板块均线过滤",
            "imported_filter_pass": "导入指标过滤",
            "trade_closed": "形成平仓交易",
            "reject_reason_chain": "拦截原因链",
            "execution_skip_reason": "成交失败原因",
        }
    )


def filter_signal_trace(signal_trace_df: pd.DataFrame) -> pd.DataFrame:
    if signal_trace_df.empty:
        return signal_trace_df

    trace_status_options = {
        "全部": lambda df: pd.Series([True] * len(df), index=df.index),
        "被拦截": lambda df: df["trigger_pass"].fillna(False)
        & (~df["filter_pass"].fillna(False)),
        "成交失败": lambda df: df["execution_skip_reason"].fillna("").astype(str).ne(""),
        "形成平仓": lambda df: df["trade_closed"].fillna(False),
        "仅触发": lambda df: df["trigger_pass"].fillna(False),
    }
    filter_cols = st.columns(3)
    status_filter = filter_cols[0].selectbox(
        "轨迹状态",
        options=list(trace_status_options.keys()),
        key="signal_trace_status_filter",
    )
    factor_options = ["全部"] + sorted(signal_trace_df["entry_factor"].dropna().astype(str).unique().tolist())
    factor_filter = filter_cols[1].selectbox(
        "轨迹因子",
        options=factor_options,
        key="signal_trace_factor_filter",
    )
    stock_options = ["全部"] + sorted(signal_trace_df["stock_code"].dropna().astype(str).unique().tolist())
    stock_filter = filter_cols[2].selectbox(
        "轨迹股票",
        options=stock_options,
        key="signal_trace_stock_filter",
    )

    filtered_df = signal_trace_df.loc[trace_status_options[status_filter](signal_trace_df)].copy()
    if factor_filter != "全部":
        filtered_df = filtered_df.loc[filtered_df["entry_factor"].astype(str) == factor_filter]
    if stock_filter != "全部":
        filtered_df = filtered_df.loc[filtered_df["stock_code"].astype(str) == stock_filter]
    return filtered_df.reset_index(drop=True)


def render_trade_explanations(
    *,
    signal_trace_df: pd.DataFrame,
    rejected_signal_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    stats: dict,
    section_header: callable,
    summarize_signal_funnel: callable,
    summarize_filter_stack: callable,
    summarize_trade_decision_chain: callable,
    dataframe_stretch: callable,
    format_timestamp: callable,
    format_number: callable,
    format_percent: callable,
    entry_factor: str,
    use_ma_filter: bool,
    fast_ma_period: int,
    slow_ma_period: int,
    enable_atr_filter: bool,
    min_atr_filter_pct: float,
    max_atr_filter_pct: float,
    enable_board_ma_filter: bool,
    board_ma_filter_line: str,
    board_ma_filter_operator: str,
    board_ma_filter_threshold: float,
    imported_filter_count: int,
) -> None:
    section_header("交易明细", "表头已转中文，优先展示交易关键信息与成交明细。")
    detail_meta_cols = st.columns(3)
    detail_meta_cols[0].metric("交易笔数", f"{len(detail_df) if isinstance(detail_df, pd.DataFrame) else 0}")
    detail_meta_cols[1].metric("平均持有天数", f"{float(stats.get('avg_holding_days', 0.0)):.2f}")
    detail_meta_cols[2].metric("净收益中位数", f"{float(stats.get('median_net_return_pct', 0.0)):.2f}%")

    st.markdown("**信号放行/拦截概览**")
    funnel_cols = st.columns([1.2, 1.8])
    with funnel_cols[0]:
        dataframe_stretch(summarize_signal_funnel(stats, signal_trace_df), hide_index=True, height=220)
    with funnel_cols[1]:
        active_filters = summarize_filter_stack(
            entry_factor=str(entry_factor),
            use_ma_filter=bool(use_ma_filter),
            fast_ma_period=int(fast_ma_period),
            slow_ma_period=int(slow_ma_period),
            enable_atr_filter=bool(enable_atr_filter),
            min_atr_filter_pct=float(min_atr_filter_pct),
            max_atr_filter_pct=float(max_atr_filter_pct),
            enable_board_ma_filter=bool(enable_board_ma_filter),
            board_ma_filter_line=str(board_ma_filter_line),
            board_ma_filter_operator=str(board_ma_filter_operator),
            board_ma_filter_threshold=float(board_ma_filter_threshold),
            imported_filter_count=int(imported_filter_count),
        )
        st.caption("当前启用的决策链顺序")
        for idx, filter_label in enumerate(active_filters, start=1):
            st.markdown(f"{idx}. {filter_label}")
        st.caption("核心信号先由入场因子给出，再依次经过已启用过滤条件；只有放行后的信号才会进入成交与持仓模拟。")
        st.markdown("**次级固定止盈说明**")
        for line in summarize_secondary_take_profit_logic():
            st.caption(line)

    if isinstance(rejected_signal_df, pd.DataFrame) and not rejected_signal_df.empty:
        st.markdown("**被拦截信号**")
        st.caption("这里展示已形成核心信号、但在过滤链中被拦下且未进入成交模拟的样本。")
        dataframe_stretch(
            format_rejected_signal_for_display(
                rejected_signal_df,
                format_timestamp=format_timestamp,
                format_number=format_number,
                format_percent=format_percent,
            ),
            hide_index=True,
            column_config=build_rejected_signal_column_config(),
            height=240,
        )

    execution_skip_df = format_execution_skip_for_display(
        signal_trace_df,
        format_timestamp=format_timestamp,
        format_number=format_number,
    )
    if isinstance(execution_skip_df, pd.DataFrame) and not execution_skip_df.empty:
        st.markdown("**成交模拟失败信号**")
        st.caption("这里展示已通过过滤、但在成交模拟阶段失败的信号。")
        dataframe_stretch(
            execution_skip_df,
            hide_index=True,
            column_config=build_execution_skip_column_config(),
            height=220,
        )

    if isinstance(signal_trace_df, pd.DataFrame) and not signal_trace_df.empty:
        st.markdown("**信号轨迹下钻**")
        st.caption("逐条查看形态成立、真实触发、各过滤器 pass/fail、是否形成平仓交易以及失败原因。")
        filtered_signal_trace_df = filter_signal_trace(signal_trace_df)
        dataframe_stretch(
            format_signal_trace_for_display(
                filtered_signal_trace_df,
                format_timestamp=format_timestamp,
                format_number=format_number,
            ),
            hide_index=True,
            column_config=build_signal_trace_column_config(),
            height=260,
        )
        trace_csv_bytes = filtered_signal_trace_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "导出轨迹 CSV",
            data=trace_csv_bytes,
            file_name="signal_trace.csv",
            mime="text/csv",
        )

    decision_chain_df = summarize_trade_decision_chain(detail_df)
    if not decision_chain_df.empty:
        st.markdown("**决策链速览**")
        st.caption("先看哪条开仓规则触发、如何成交，再看最终由哪条离场规则完成退出。")
        dataframe_stretch(decision_chain_df, hide_index=True, height=260)
    elif not isinstance(detail_df, pd.DataFrame) or detail_df.empty:
        st.info("当前没有形成平仓交易，可优先查看上方漏斗、被拦截信号、成交失败信号和轨迹下钻。")
