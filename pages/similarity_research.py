from __future__ import annotations

from datetime import timedelta
from html import escape

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from data_loader import load_bars, read_data_source_config
from research.similarity import SimilarityConfig, run_similarity_research
from research.similarity.report import (
    build_html_report_bytes,
    cases_to_csv_bytes,
    cases_to_excel_bytes,
    current_path_png_bytes,
)


INDEX_PRESETS = (
    ("沪深300", "000300.SH"),
    ("上证指数", "000001.SH"),
    ("创业板指", "399006.SZ"),
    ("中证500", "000905.SH"),
    ("中证1000", "000852.SH"),
    ("上证50", "000016.SH"),
)

WINDOWS_BY_TIMEFRAME = {
    "1d": (5, 10, 20, 60, 120),
    "30m": (5, 10, 16, 40, 80, 160),
}
FORWARD_WINDOWS_BY_TIMEFRAME = {
    "1d": (5, 20, 60),
    "30m": (8, 24, 40, 80),
}
INDEX_SYMBOL_BY_LABEL = dict(INDEX_PRESETS)
TOP_CANDLESTICK_CHARTS = 4
SIMILARITY_INTERNAL_COLUMNS = {
    "window_start_index",
    "window_end_index",
    "path_distance",
    "feature_distance",
    "external_distance",
    "dtw_distance",
}


def render_similarity_research_page() -> None:
    _render_header()
    settings = _render_sidebar()
    _render_workflow_intro()

    data_check = _load_similarity_data(settings)
    _render_data_check(data_check, settings)
    if data_check["bars"].empty:
        st.button("运行相似阶段扫描", type="primary", key="run_similarity_research", disabled=True)
        st.info("请先到“数据准备页”更新当前周期数据，再回到这里运行相似阶段扫描。")
        _render_help_section()
        return

    if st.button("运行相似阶段扫描", type="primary", key="run_similarity_research"):
        _run_and_render(data_check["bars"], settings)
    elif "similarity_result" in st.session_state:
        cached = st.session_state["similarity_result"]
        _render_result(cached["result"], cached["settings"])
    else:
        st.info("确认数据覆盖范围后，点击“运行相似阶段扫描”。结果会在下方展示。")
    _render_help_section()


def _render_header() -> None:
    st.markdown(
        """
        <div class='app-hero'>
            <h1>A股指数相似阶段研究</h1>
            <p>默认先用日线观察阶段结构；需要更细节时切换 30m。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar() -> dict[str, object]:
    st.sidebar.header("运行设置")
    st.sidebar.caption("默认先用日线观察阶段结构；需要更细节时切换 30m。")
    timeframe = st.sidebar.selectbox(
        "研究周期",
        options=["1d", "30m"],
        index=0,
        key="similarity_timeframe",
    )
    preset_labels = [label for label, _ in INDEX_PRESETS]
    preset = st.sidebar.selectbox(
        "研究指数",
        options=preset_labels,
        index=0,
        key="similarity_index_preset",
        on_change=_sync_similarity_symbol_from_preset,
    )
    symbol_value, last_preset = resolve_similarity_symbol_value(
        preset=str(preset),
        current_symbol=str(st.session_state.get("similarity_symbol", "")),
        last_preset=str(st.session_state.get("similarity_last_index_preset", "")),
    )
    st.session_state["similarity_symbol"] = symbol_value
    st.session_state["similarity_last_index_preset"] = last_preset
    symbol = st.sidebar.text_input("指数代码", key="similarity_symbol").strip().upper()

    today = pd.Timestamp.today().date()
    default_start = today - (timedelta(days=3650) if timeframe == "1d" else timedelta(days=365))
    start_date = st.sidebar.date_input("数据开始", value=default_start, key="similarity_start_date")
    as_of = st.sidebar.date_input("截至日期", value=today, key="similarity_as_of")

    windows = WINDOWS_BY_TIMEFRAME[str(timeframe)]
    default_window_index = 2 if str(timeframe) == "1d" else 3
    window_mode = st.sidebar.radio(
        "主走势区间",
        options=["按窗口长度", "自定义起止"],
        key="similarity_window_mode",
    )
    if window_mode == "按窗口长度":
        window_size = st.sidebar.selectbox(
            "主走势窗口",
            options=list(windows),
            index=default_window_index,
            key="similarity_window_size",
        )
        interval_start = None
        interval_end = None
    else:
        default_interval_start = today - timedelta(days=30 if timeframe == "1d" else 10)
        interval_cols = st.sidebar.columns(2)
        interval_start = interval_cols[0].date_input(
            "区间开始",
            value=default_interval_start,
            key="similarity_interval_start",
        )
        interval_end = interval_cols[1].date_input(
            "区间结束",
            value=today,
            key="similarity_interval_end",
        )
        window_size = 0
    candidate_n = st.sidebar.number_input("候选样本数", min_value=20, max_value=500, value=100, step=10, key="similarity_candidate_n")
    top_n = st.sidebar.number_input("展示样本数", min_value=3, max_value=30, value=10, step=1, key="similarity_top_n")
    min_valid_cases = st.sidebar.number_input("最小有效样本", min_value=3, max_value=30, value=8, step=1, key="similarity_min_valid_cases")
    variable_groups = st.sidebar.multiselect(
        "独立变量组",
        options=["成交", "板块", "拥挤度"],
        default=["成交", "板块", "拥挤度"],
        key="similarity_variable_groups",
    )
    st.sidebar.caption("首版成交变量直接进入相似度；板块和拥挤度先作为产品入口保留，数据缺失时会明确标记。")
    return {
        "timeframe": str(timeframe),
        "symbol": symbol,
        "start_date": start_date,
        "as_of": as_of,
        "window_size": int(window_size),
        "window_mode": str(window_mode),
        "interval_start": interval_start,
        "interval_end": interval_end,
        "candidate_n": int(candidate_n),
        "top_n": int(top_n),
        "min_valid_cases": int(min_valid_cases),
        "forward_windows": FORWARD_WINDOWS_BY_TIMEFRAME[str(timeframe)],
        "variable_groups": tuple(variable_groups),
    }


def _sync_similarity_symbol_from_preset() -> None:
    preset = str(st.session_state.get("similarity_index_preset", ""))
    symbol = INDEX_SYMBOL_BY_LABEL.get(preset)
    if symbol:
        st.session_state["similarity_symbol"] = symbol
        st.session_state["similarity_last_index_preset"] = preset


def resolve_similarity_symbol_value(
    *,
    preset: str,
    current_symbol: str,
    last_preset: str,
) -> tuple[str, str]:
    preset_symbol = INDEX_SYMBOL_BY_LABEL.get(preset, "")
    normalized_current = str(current_symbol).strip().upper()
    if preset_symbol and (not normalized_current or preset != last_preset):
        return preset_symbol, preset
    return normalized_current, preset


def _render_workflow_intro() -> None:
    steps = pd.DataFrame(
        [
            ("1 选择周期", "默认日线；需要看近期节奏时切 30m。"),
            ("2 检查数据", "确认覆盖范围、行数和最近时间。"),
            ("3 运行扫描", "先找主走势相似，再用独立变量过滤。"),
            ("4 解读结果", "看走势像不像、环境像不像、历史之后怎么走。"),
            ("5 导出复盘", "下载 HTML/CSV/XLSX，或把摘要写入 Notion。"),
        ],
        columns=["步骤", "用户操作"],
    )
    st.markdown("**操作路径**")
    _centered_dataframe(steps)


def _render_algorithm_guide() -> None:
    st.markdown("**算法怎么跑**")
    st.info("一句话理解：把当前这段 K 线当作模板，去过去找形状和成交环境都接近的片段，再统计这些片段之后怎么走。")
    _centered_dataframe(similarity_algorithm_steps_for_display())


def similarity_algorithm_steps_for_display() -> pd.DataFrame:
    return pd.DataFrame(
        [
            (
                "1 取模板",
                "按截至日期向前取当前窗口；如果用户选择自定义起止，就用区间内真实 K 线数作为窗口长度。",
                "这段就是拿去历史里寻找相似阶段的样板。",
                "截至日期 / 主走势窗口 / 自定义起止",
            ),
            (
                "2 找历史",
                "遍历历史上同样长度的窗口，只使用截至日期之前的数据，并自动排除当前附近样本。",
                "只和过去比，不偷看未来；离当前太近的重复片段也不算。",
                "数据开始 / 截至日期 / 主走势窗口",
            ),
            (
                "3 先初筛",
                "先比较归一化收盘路径、波动率、最大回撤、趋势斜率和成交环境，留下最像的一批候选。",
                "先快速缩小范围，不直接对全历史做慢速精排。",
                "候选样本数 / 独立变量组",
            ),
            (
                "4 再精排",
                "在候选样本里用 DTW 路径距离重排，允许节奏略有快慢，但要求整体形状接近。",
                "它解决的是同样上涨或下跌，但中间拐点早几根或晚几根的问题。",
                "候选样本数 / 展示样本数",
            ),
            (
                "5 算总分",
                "总相似度 = 主走势相似度 65% + 独立变量相似度 35%，按总分展示 Top 历史区间。",
                "分数越高，代表形态和环境越接近；不是收益预测概率。",
                "展示样本数 / 独立变量组",
            ),
            (
                "6 看后验",
                "对这些历史相似区间，统计它们之后 T+N 的收益、最大回撤、最大浮盈。",
                "真正用于决策的是历史之后怎么走，而不是只看像不像。",
                "后验观察窗口 / 最小有效样本",
            ),
            (
                "7 给状态",
                "如果样本数不够，输出不可交易；样本够时再按胜率、中位收益和中位回撤给偏多、震荡或偏防守。",
                "样本不足时不强行给方向，避免看图讲故事。",
                "最小有效样本",
            ),
        ],
        columns=["步骤", "算法在做什么", "用户怎么理解", "受哪些参数影响"],
    )


def _render_feature_guide() -> None:
    st.markdown("**特征与参数说明**")
    st.caption("先用主走势找形态，再用成交环境做过滤；总相似度按主走势 65% + 独立变量 35% 合成。")
    _centered_dataframe(similarity_feature_guide_for_display())


def _render_help_section() -> None:
    st.divider()
    with st.expander("算法与参数说明（展开查看）", expanded=False):
        _render_algorithm_guide()
        _render_feature_guide()


def similarity_feature_guide_for_display() -> pd.DataFrame:
    return pd.DataFrame(
        [
            (
                "评分",
                "总相似度",
                "按主走势相似度 65% + 独立变量相似度 35% 合成，排序输出 Top 历史样本。",
                "候选样本数 / 展示样本数 / 最小有效样本",
                "候选样本数越大越宽但更慢；展示样本数越小越严格；策略方向不稳时提高最小有效样本。",
            ),
            (
                "主走势",
                "归一化收盘路径",
                "主走势先把当前区间和历史区间都从 100 起算，比较一维走势形状。",
                "主走势窗口 / 自定义起止 / 候选样本数",
                "想找更近似的短节奏，缩短窗口；想找阶段结构，拉长窗口；样本太少时放宽候选样本数。",
            ),
            (
                "主走势",
                "DTW 路径距离",
                "对候选样本精排，允许相似走势在局部速度上有轻微错位。",
                "展示样本数 / 候选样本数",
                "展示样本数越小越严格；候选样本数越大，精排范围越宽但运行更慢。",
            ),
            (
                "波动",
                "实现波动率",
                "衡量窗口内收益波动，避免把平稳走势和剧烈震荡误判为相似。",
                "主走势窗口",
                "波动状态不稳定时缩短窗口；想比较完整行情阶段时拉长窗口。",
            ),
            (
                "波动",
                "最大回撤",
                "描述窗口内最大下行压力，用于区分顺滑上涨和深回撤后的修复。",
                "主走势窗口 / 最小有效样本",
                "回撤特征太严导致样本少时，降低最小有效样本只用于观察，不建议直接给策略方向。",
            ),
            (
                "趋势",
                "趋势斜率",
                "衡量归一化路径的整体上行或下行方向，辅助判断阶段方向是否一致。",
                "主走势窗口",
                "短窗口更敏感，长窗口更稳定；方向频繁变化时优先看 5/10 或 30m。",
            ),
            (
                "成交",
                "下跌放量占比",
                "观察下跌 K 线消耗了多少成交，用来识别抛压或承接环境。",
                "独立变量组：成交",
                "若成交数据缺失，结果会降级为主走势观察；成交差异大时不要只看形态相似。",
            ),
            (
                "成交",
                "量价相关",
                "比较收益和成交额/成交量的相关性，用来判断上涨放量或下跌放量环境。",
                "独立变量组：成交",
                "想降低成交过滤影响，可先取消成交组；想提高环境一致性，则保留成交组并提高样本要求。",
            ),
        ],
        columns=["模块", "特征", "怎么用", "相关参数", "调参方向"],
    )


def _load_similarity_data(settings: dict[str, object]) -> dict[str, object]:
    config = read_data_source_config()
    try:
        bars = load_bars(
            symbol=str(settings["symbol"]),
            timeframe=str(settings["timeframe"]),
            start_date=pd.Timestamp(settings["start_date"]).strftime("%Y-%m-%d"),
            end_date=pd.Timestamp(settings["as_of"]).strftime("%Y-%m-%d"),
            local_data_root=config["local_data_root"],
            adjust=config["default_adjust"],
        )
        message = "数据可用"
    except Exception as exc:  # noqa: BLE001
        bars = pd.DataFrame()
        message = str(exc)
    return {"bars": bars, "message": message}


def _render_data_check(data_check: dict[str, object], settings: dict[str, object]) -> None:
    st.markdown("**数据检查**")
    bars = data_check["bars"]
    if isinstance(bars, pd.DataFrame) and not bars.empty:
        date_series = pd.to_datetime(bars["date"], errors="coerce").dropna()
        cols = st.columns(4)
        cols[0].metric("周期", str(settings["timeframe"]))
        cols[1].metric("行数", f"{len(bars):,}")
        cols[2].metric("最早时间", date_series.min().strftime("%Y-%m-%d %H:%M"))
        cols[3].metric("最近时间", date_series.max().strftime("%Y-%m-%d %H:%M"))
        selected_rows = _selected_window_row_count(bars, settings)
        if settings.get("window_mode") == "自定义起止":
            st.caption(f"当前自定义主走势区间内共有 {selected_rows} 根 K 线。")
        effective_window_size = selected_rows if settings.get("window_mode") == "自定义起止" else int(settings["window_size"])
        required = effective_window_size + max(tuple(settings["forward_windows"])) + int(settings["min_valid_cases"])
        if len(bars) < required:
            st.warning(f"当前数据行数偏少，建议至少准备 {required} 行以上再运行。")
        else:
            st.success("当前数据满足扫描的最低行数要求。")
    else:
        st.error(f"数据不可用：{data_check['message']}")


def _run_and_render(bars: pd.DataFrame, settings: dict[str, object]) -> None:
    config, message = resolve_similarity_config_from_settings(bars, settings)
    if config is None:
        st.error(message)
        return
    result = run_similarity_research(bars, config)
    st.session_state["similarity_result"] = {"result": result, "settings": dict(settings)}
    _render_result(result, settings)


def _render_result(result, settings: dict[str, object]) -> None:
    st.divider()
    st.markdown("**结果总览**")
    primary_horizon = tuple(settings["forward_windows"])[min(1, len(tuple(settings["forward_windows"])) - 1)]
    primary_return = f"t_plus_{primary_horizon}_return"
    primary_drawdown = f"t_plus_{primary_horizon}_max_drawdown"
    cases = result.cases.copy()
    returns = pd.to_numeric(cases.get(primary_return), errors="coerce").dropna()
    drawdowns = pd.to_numeric(cases.get(primary_drawdown), errors="coerce").dropna()
    cols = st.columns(5)
    cols[0].metric("策略状态", result.status)
    cols[1].metric("有效样本", str(len(cases)))
    cols[2].metric("胜率", _format_pct(float((returns > 0).mean())) if len(returns) else "NA")
    cols[3].metric("中位收益", _format_pct(float(returns.median())) if len(returns) else "NA")
    cols[4].metric("中位回撤", _format_pct(float(drawdowns.median())) if len(drawdowns) else "NA")
    if result.status == "不可交易":
        st.warning(result.message)
    else:
        st.info(result.message)

    _render_charts(result, settings)
    _render_tables(result)
    _render_exports(result, settings)


def resolve_similarity_config_from_settings(
    bars: pd.DataFrame,
    settings: dict[str, object],
) -> tuple[SimilarityConfig | None, str]:
    if str(settings.get("window_mode", "按窗口长度")) == "自定义起止":
        interval_start = pd.Timestamp(settings["interval_start"])
        interval_end = pd.Timestamp(settings["interval_end"])
        if interval_start > interval_end:
            return None, "自定义区间开始日期不能晚于结束日期。"
        start_bound, end_bound = _custom_interval_bounds(settings)
        date_series = pd.to_datetime(bars["date"], errors="coerce")
        selected = bars.loc[date_series.between(start_bound, end_bound)]
        if selected.empty:
            return None, "自定义区间内没有可用交易数据，请调整开始或结束交易日。"
        window_size = int(len(selected))
        as_of = pd.Timestamp(pd.to_datetime(selected["date"], errors="coerce").max())
    else:
        window_size = int(settings["window_size"])
        as_of = pd.Timestamp(settings["as_of"])

    if window_size < 2:
        return None, "主走势区间至少需要 2 根 K 线。"

    return (
        SimilarityConfig(
            symbol=str(settings["symbol"]),
            timeframe=str(settings["timeframe"]),
            as_of=as_of,
            window_size=window_size,
            forward_windows=tuple(settings["forward_windows"]),
            candidate_n=int(settings["candidate_n"]),
            top_n=int(settings["top_n"]),
            min_valid_cases=int(settings["min_valid_cases"]),
        ),
        "",
    )


def _selected_window_row_count(bars: pd.DataFrame, settings: dict[str, object]) -> int:
    if str(settings.get("window_mode", "按窗口长度")) != "自定义起止":
        return int(settings["window_size"])
    interval_start, interval_end = _custom_interval_bounds(settings)
    date_series = pd.to_datetime(bars["date"], errors="coerce")
    return int(date_series.between(interval_start, interval_end).sum())


def _custom_interval_bounds(settings: dict[str, object]) -> tuple[pd.Timestamp, pd.Timestamp]:
    interval_start = pd.Timestamp(settings["interval_start"]).normalize()
    interval_end = pd.Timestamp(settings["interval_end"]).normalize()
    return interval_start, interval_end + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)


def _render_charts(result, settings: dict[str, object]) -> None:
    if result.current_window.empty:
        return
    st.markdown("**图形刻画**")
    current = result.current_window.copy()
    current["normalized_close"] = current["close"].astype(float) / float(current["close"].iloc[0]) * 100
    st.plotly_chart(
        px.line(current, x="date", y="normalized_close", title="当前窗口归一化路径"),
        use_container_width=True,
    )
    candlestick_grid = build_top_similarity_candlestick_grid(
        result,
        top_k=TOP_CANDLESTICK_CHARTS,
    )
    if candlestick_grid is not None:
        st.markdown("**Top 相似区间 K 线**")
        st.caption("按总相似度展示前 4 个历史区间；横轴按连续交易序号排列，已去掉周末和节假日空档。")
        st.plotly_chart(candlestick_grid, use_container_width=True)

    if not result.outcome_summary.empty:
        summary = result.outcome_summary.copy()
        st.plotly_chart(
            px.bar(summary, x="horizon", y="median_return", title="后验窗口中位收益"),
            use_container_width=True,
        )


def build_top_similarity_candlestick_grid(
    result,
    top_k: int = TOP_CANDLESTICK_CHARTS,
) -> go.Figure | None:
    cases = getattr(result, "cases", pd.DataFrame())
    historical_windows = getattr(
        result,
        "historical_windows_with_forward",
        getattr(result, "historical_windows", []),
    )
    if cases.empty or not historical_windows:
        return None

    chart_items = [
        (rank, case, window)
        for rank, (_, case), window in zip(
            range(1, top_k + 1),
            cases.head(top_k).iterrows(),
            historical_windows[:top_k],
            strict=False,
        )
        if not window.empty
    ]
    if not chart_items:
        return None

    subplot_titles = [_candlestick_subplot_title(rank, case) for rank, case, _ in chart_items]
    figure = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=subplot_titles,
        vertical_spacing=0.12,
        horizontal_spacing=0.06,
    )
    figure.update_layout(grid={"rows": 2, "columns": 2, "pattern": "independent"})

    for rank, case, window in chart_items:
        row = 1 if rank <= 2 else 2
        col = 1 if rank % 2 == 1 else 2
        figure.add_trace(_compact_candlestick_trace(window), row=row, col=col)
        _add_phase_background(figure, window, row=row, col=col)
        tickvals, ticktext = _compact_axis_ticks(window)
        figure.update_xaxes(
            row=row,
            col=col,
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
            rangeslider_visible=False,
            title_text="交易序号",
        )
        figure.update_yaxes(row=row, col=col, title_text="价格")

    figure.update_layout(
        title="Top 相似区间 K 线对照",
        height=760,
        showlegend=False,
        margin={"l": 20, "r": 20, "t": 90, "b": 30},
    )
    return figure


def _add_phase_background(figure: go.Figure, window: pd.DataFrame, *, row: int, col: int) -> None:
    match_length = _match_phase_length(window)
    if match_length == 0:
        return
    figure.add_vrect(
        x0=0.5,
        x1=match_length + 0.5,
        fillcolor="rgba(37, 99, 235, 0.08)",
        line_width=0,
        layer="below",
        row=row,
        col=col,
    )
    if len(window) > match_length:
        figure.add_vrect(
            x0=match_length + 0.5,
            x1=len(window) + 0.5,
            fillcolor="rgba(245, 158, 11, 0.12)",
            line_width=0,
            layer="below",
            row=row,
            col=col,
        )
        figure.add_vline(
            x=match_length + 0.5,
            line_width=1,
            line_dash="dot",
            line_color="rgba(107, 114, 128, 0.85)",
            row=row,
            col=col,
        )


def _match_phase_length(window: pd.DataFrame) -> int:
    if "similarity_phase" not in window.columns:
        return len(window)
    return int((window["similarity_phase"] == "相似窗口").sum())


def _candlestick_subplot_title(rank: int, case: pd.Series) -> str:
    similarity = _format_ratio_as_percent(case.get("total_similarity", float("nan")))
    return (
        f"Top {rank} "
        f"{pd.Timestamp(case['window_start']).strftime('%Y-%m-%d')} - "
        f"{pd.Timestamp(case['window_end']).strftime('%Y-%m-%d')} "
        f"{similarity}"
    )


def _compact_candlestick_trace(window: pd.DataFrame) -> go.Candlestick:
    compact_x = list(range(1, len(window) + 1))
    date_labels = pd.to_datetime(window["date"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    phase_labels = (
        window["similarity_phase"].astype(str)
        if "similarity_phase" in window.columns
        else pd.Series(["相似窗口"] * len(window))
    )
    hovertext = [
        (
            f"阶段={phase}<br>"
            f"时间={date}<br>"
            f"开={open_price}<br>"
            f"高={high_price}<br>"
            f"低={low_price}<br>"
            f"收={close_price}"
        )
        for phase, date, open_price, high_price, low_price, close_price in zip(
            phase_labels,
            date_labels,
            window["open"],
            window["high"],
            window["low"],
            window["close"],
            strict=False,
        )
    ]
    return go.Candlestick(
        x=compact_x,
        open=window["open"],
        high=window["high"],
        low=window["low"],
        close=window["close"],
        hoverinfo="text",
        hovertext=hovertext,
        name="K线",
    )


def _compact_axis_ticks(window: pd.DataFrame) -> tuple[list[int], list[str]]:
    if window.empty:
        return [], []
    date_labels = pd.to_datetime(window["date"], errors="coerce").dt.strftime("%m-%d")
    if len(window) == 1:
        return [1], [str(date_labels.iloc[0])]
    midpoint = max(1, (len(window) + 1) // 2)
    tickvals = sorted({1, midpoint, len(window)})
    ticktext = [str(date_labels.iloc[value - 1]) for value in tickvals]
    return tickvals, ticktext


def build_top_similarity_candlestick_figures(result, top_k: int = TOP_CANDLESTICK_CHARTS) -> list[go.Figure]:
    grid = build_top_similarity_candlestick_grid(result, top_k=top_k)
    return [grid] if grid is not None else []


def _legacy_candlestick_figures(result, top_k: int = TOP_CANDLESTICK_CHARTS) -> list[go.Figure]:
    cases = getattr(result, "cases", pd.DataFrame())
    historical_windows = getattr(result, "historical_windows", [])
    if cases.empty or not historical_windows:
        return []

    figures: list[go.Figure] = []
    for rank, (_, case), window in zip(
        range(1, top_k + 1),
        cases.head(top_k).iterrows(),
        historical_windows[:top_k],
        strict=False,
    ):
        if window.empty:
            continue
        similarity = _format_ratio_as_percent(case.get("total_similarity", float("nan")))
        title = (
            f"Top {rank} 相似区间 K 线 "
            f"({pd.Timestamp(case['window_start']).strftime('%Y-%m-%d')} - "
            f"{pd.Timestamp(case['window_end']).strftime('%Y-%m-%d')}，相似度 {similarity})"
        )
        figures.append(_candlestick_figure(window, title))
    return figures


def _candlestick_figure(window: pd.DataFrame, title: str) -> go.Figure:
    figure = go.Figure(
        data=[
            go.Candlestick(
                x=window["date"],
                open=window["open"],
                high=window["high"],
                low=window["low"],
                close=window["close"],
                name="K线",
            )
        ]
    )
    figure.update_layout(
        title=title,
        height=360,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_rangeslider_visible=False,
    )
    return figure


def _render_tables(result) -> None:
    if not result.outcome_summary.empty:
        st.markdown("**后验分布**")
        _centered_dataframe(format_outcome_summary_for_display(result.outcome_summary))
    st.markdown("**历史相似阶段明细**")
    if result.cases.empty:
        st.info("暂无有效历史样本。")
    else:
        _centered_dataframe(format_similarity_cases_for_display(result.cases))


def _centered_dataframe(display_df: pd.DataFrame) -> None:
    st.markdown(
        """
        <style>
        .similarity-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.92rem;
        }
        .similarity-table th {
            background: #f6f7f9;
            color: #1f2937;
            font-weight: 650;
        }
        .similarity-table th,
        .similarity-table td {
            border: 1px solid #e5e7eb;
            padding: 0.55rem 0.65rem;
            text-align: center;
            vertical-align: middle;
            white-space: normal;
            line-height: 1.45;
        }
        .similarity-table tbody tr:nth-child(even) {
            background: #fafafa;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(_dataframe_to_centered_html(display_df), unsafe_allow_html=True)


def _dataframe_to_centered_html(display_df: pd.DataFrame) -> str:
    headers = "".join(f"<th>{escape(str(column))}</th>" for column in display_df.columns)
    rows = []
    for _, row in display_df.iterrows():
        cells = "".join(f"<td>{escape(str(value))}</td>" for value in row)
        rows.append(f"<tr>{cells}</tr>")
    body = "".join(rows)
    return f"<table class='similarity-table'><thead><tr>{headers}</tr></thead><tbody>{body}</tbody></table>"


def format_outcome_summary_for_display(summary: pd.DataFrame) -> pd.DataFrame:
    display = summary.copy()
    rename_map = {
        "horizon": "观察窗口",
        "sample_count": "样本数",
        "win_rate": "胜率",
        "median_return": "中位收益",
        "median_max_drawdown": "中位最大回撤",
    }
    for column in ["win_rate", "median_return", "median_max_drawdown"]:
        if column in display.columns:
            display[column] = display[column].map(_format_ratio_as_percent)
    return display.rename(columns=rename_map)


def format_similarity_cases_for_display(cases: pd.DataFrame) -> pd.DataFrame:
    display = cases.drop(columns=list(SIMILARITY_INTERNAL_COLUMNS), errors="ignore").copy()
    for column in ["window_start", "window_end"]:
        if column in display.columns:
            display[column] = pd.to_datetime(display[column], errors="coerce").dt.strftime(
                "%Y-%m-%d %H:%M"
            )
    rename_map = {
        "window_start": "窗口开始",
        "window_end": "窗口结束",
        "total_similarity": "总相似度",
        "main_similarity": "主走势相似度",
        "external_similarity": "独立变量相似度",
        "similar_reason": "相似原因",
        "external_difference": "主要差异",
    }
    for column in list(display.columns):
        if column.startswith("t_plus_") and column.endswith("_return"):
            horizon = column.removeprefix("t_plus_").removesuffix("_return")
            rename_map[column] = f"后验收益 T+{horizon}"
        elif column.startswith("t_plus_") and column.endswith("_max_drawdown"):
            horizon = column.removeprefix("t_plus_").removesuffix("_max_drawdown")
            rename_map[column] = f"最大回撤 T+{horizon}"
        elif column.startswith("t_plus_") and column.endswith("_max_favorable"):
            horizon = column.removeprefix("t_plus_").removesuffix("_max_favorable")
            rename_map[column] = f"最大浮盈 T+{horizon}"
    preferred_columns = [
        column
        for column in [
            "window_start",
            "window_end",
            "total_similarity",
            "main_similarity",
            "external_similarity",
            "similar_reason",
            "external_difference",
        ]
        if column in display.columns
    ]
    outcome_columns = [
        column
        for column in display.columns
        if column.startswith("t_plus_")
        and (
            column.endswith("_return")
            or column.endswith("_max_drawdown")
            or column.endswith("_max_favorable")
        )
    ]
    other_columns = [
        column for column in display.columns if column not in {*preferred_columns, *outcome_columns}
    ]
    percent_columns = [
        column
        for column in display.columns
        if column in {"total_similarity", "main_similarity", "external_similarity"}
        or (
            column.startswith("t_plus_")
            and (
                column.endswith("_return")
                or column.endswith("_max_drawdown")
                or column.endswith("_max_favorable")
            )
        )
    ]
    for column in percent_columns:
        display[column] = display[column].map(_format_ratio_as_percent)
    return display[[*preferred_columns, *outcome_columns, *other_columns]].rename(
        columns=rename_map
    )


def _format_ratio_as_percent(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value) * 100:.2f}%"


def _render_exports(result, settings: dict[str, object]) -> None:
    st.markdown("**导出复盘**")
    title = f"{settings['symbol']} {settings['timeframe']} 相似阶段研究"
    cols = st.columns(4)
    cols[0].download_button(
        "下载 CSV 样本",
        data=cases_to_csv_bytes(result.cases),
        file_name="similar_cases.csv",
        mime="text/csv",
        disabled=result.cases.empty,
    )
    cols[1].download_button(
        "下载 Excel 样本",
        data=cases_to_excel_bytes(result.cases) if not result.cases.empty else b"",
        file_name="similar_cases.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        disabled=result.cases.empty,
    )
    cols[2].download_button(
        "下载 HTML 报告",
        data=build_html_report_bytes(
            title=title,
            status=result.status,
            message=result.message,
            cases=result.cases,
        ),
        file_name="similarity_report.html",
        mime="text/html",
    )
    try:
        png_data = current_path_png_bytes(result.current_window)
        png_error = ""
    except Exception as exc:  # noqa: BLE001
        png_data = b""
        png_error = str(exc)
    cols[3].download_button(
        "下载 PNG 图表",
        data=png_data,
        file_name="similarity_path.png",
        mime="image/png",
        disabled=not bool(png_data),
    )
    if png_error:
        st.caption("PNG 导出需要本地安装 kaleido；当前图表仍可在页面中查看。")
    notion_md = f"{title}\n\n策略状态：{result.status}\n\n{result.message}"
    st.text_area("Notion 复盘摘要", value=notion_md, height=120, key="similarity_notion_summary")


def _format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"
