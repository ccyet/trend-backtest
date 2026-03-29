from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any, cast

import pandas as pd
import plotly.express as px
import streamlit as st

from analyzer import analyze_all_stocks, run_parameter_scan
from data_loader import (
    describe_file_source,
    describe_tables,
    list_candidate_tables,
    list_file_sheets,
    load_market_data,
    quote_ident,
)
from data.services.local_inventory_service import load_inventory
from exporter import export_to_excel_bytes
from models import (
    AnalysisParams,
    ENTRY_FACTORS,
    FACTOR_SCAN_ELIGIBLE_FIELDS,
    GAP_ENTRY_MODES,
    ParamScanAxis,
    ParamScanConfig,
    PartialExitRule,
    SCAN_FIELD_CASTERS,
    SCAN_METRICS,
    normalize_column_overrides,
    normalize_stock_codes,
    parse_scan_values,
    validate_params,
)


st.set_page_config(layout="wide", page_title="Gap_test 回测系统")

RESULT_STATE_KEYS = [
    "detail_df",
    "daily_df",
    "equity_df",
    "stats",
    "scan_df",
    "scan_metric",
    "scan_axis_fields",
    "best_scan_overrides",
    "excel_bytes",
    "download_name",
]

DETAIL_PRICE_COLUMNS = [
    "prev_close",
    "prev_high",
    "prev_low",
    "open",
    "close",
    "buy_price",
    "sell_price",
    "exit_ma_value",
]
DETAIL_PERCENT_COLUMNS = [
    "gap_pct_vs_prev_close",
    "gross_return_pct",
    "net_return_pct",
    "mfe_pct",
    "mae_pct",
    "max_profit_pct",
    "profit_drawdown_ratio",
]
DETAIL_NAV_COLUMNS = ["nav_before_trade", "nav_after_trade"]
DETAIL_COUNT_COLUMNS = ["holding_days", "fill_count"]
SUMMARY_PERCENT_COLUMNS = [
    "win_rate_pct",
    "avg_net_return_pct",
    "median_net_return_pct",
]
EQUITY_PERCENT_COLUMNS = ["drawdown_pct"]
SCAN_FIELD_LABELS = {
    "gap_pct": "跳空幅度",
    "max_gap_filter_pct": "最大高开/低开过滤",
    "trend_breakout_lookback": "趋势突破回看天数",
    "vcb_range_lookback": "波动收缩区间回看天数",
    "vcb_breakout_lookback": "波动收缩突破回看天数",
    "candle_run_length": "连续K线根数",
    "candle_run_min_body_pct": "单根最小实体幅度",
    "candle_run_total_move_pct": "组合最小累计涨跌幅",
    "eshb_open_window_bars": "早盘观察窗口K数",
    "eshb_base_min_bars": "高位横盘最少K数",
    "eshb_base_max_bars": "高位横盘最多K数",
    "eshb_surge_min_pct": "早盘冲高最小涨幅",
    "eshb_max_base_pullback_pct": "横盘最大回撤",
    "eshb_max_base_range_pct": "横盘最大振幅",
    "eshb_max_anchor_breaks": "锚点跌破次数上限",
    "eshb_max_anchor_break_depth_pct": "锚点跌破深度上限",
    "eshb_min_open_volume_ratio": "冲高量能倍数下限",
    "eshb_min_breakout_volume_ratio": "突破量能倍数下限",
    "eshb_trigger_buffer_pct": "突破触发缓冲",
    "atr_filter_period": "ATR过滤周期",
    "min_atr_filter_pct": "最小ATR波动过滤",
    "max_atr_filter_pct": "最大ATR波动过滤",
    "time_stop_days": "最多持有天数",
    "time_stop_target_pct": "时间退出收益阈值",
    "stop_loss_pct": "全仓止损",
    "take_profit_pct": "固定止盈",
    "profit_drawdown_pct": "盈利回撤",
    "exit_ma_period": "离场均线周期",
    "atr_trailing_period": "ATR跟踪周期",
    "atr_trailing_multiplier": "ATR跟踪倍数",
    "buy_slippage_pct": "买入滑点",
    "sell_slippage_pct": "卖出滑点",
    "partial_rule_1_target_profit_pct": "第1批目标收益",
    "partial_rule_2_target_profit_pct": "第2批目标收益",
    "partial_rule_3_target_profit_pct": "第3批目标收益",
    "partial_rule_1_ma_period": "第1批均线周期",
    "partial_rule_2_ma_period": "第2批均线周期",
    "partial_rule_3_ma_period": "第3批均线周期",
    "partial_rule_1_atr_period": "第1批ATR周期",
    "partial_rule_2_atr_period": "第2批ATR周期",
    "partial_rule_3_atr_period": "第3批ATR周期",
    "partial_rule_1_atr_multiplier": "第1批ATR倍数",
    "partial_rule_2_atr_multiplier": "第2批ATR倍数",
    "partial_rule_3_atr_multiplier": "第3批ATR倍数",
    "partial_rule_1_drawdown_pct": "第1批回撤比例",
    "partial_rule_2_drawdown_pct": "第2批回撤比例",
    "partial_rule_3_drawdown_pct": "第3批回撤比例",
    "partial_rule_1_min_profit_to_activate_drawdown": "第1批激活浮盈",
    "partial_rule_2_min_profit_to_activate_drawdown": "第2批激活浮盈",
    "partial_rule_3_min_profit_to_activate_drawdown": "第3批激活浮盈",
}
SCAN_METRIC_LABELS = {
    "signal_count": "信号数",
    "closed_trade_candidates": "候选平仓交易数",
    "executed_trades": "实际执行交易数",
    "strategy_win_rate_pct": "策略胜率",
    "total_return_pct": "总收益率",
    "max_drawdown_pct": "最大回撤",
    "final_net_value": "最终净值",
    "avg_holding_days": "平均持有天数",
    "profit_risk_ratio": "收益风险比",
    "trade_return_volatility_pct": "单笔收益波动率",
}
DETAIL_COLUMN_LABELS = {
    "date": "信号日期",
    "stock_code": "股票代码",
    "prev_close": "前收盘价",
    "prev_high": "前最高价",
    "prev_low": "前最低价",
    "open": "开盘价",
    "close": "当日收盘价",
    "volume": "成交量",
    "gap_pct_vs_prev_close": "相对昨收跳空幅度",
    "buy_date": "买入日期",
    "buy_price": "买入价",
    "sell_price": "卖出均价",
    "sell_date": "卖出日期",
    "exit_type": "退出方式",
    "holding_days": "持有天数",
    "gross_return_pct": "毛收益率",
    "net_return_pct": "净收益率",
    "win_flag": "是否盈利",
    "mfe_pct": "最大有利波动",
    "mae_pct": "最大不利波动",
    "max_profit_pct": "最大浮盈",
    "exit_ma_value": "离场均线值",
    "profit_drawdown_ratio": "利润回撤比例",
    "fill_count": "成交批次数",
    "fill_detail_json": "成交明细",
    "entry_reason": "开仓原因",
    "exit_reason": "离场原因",
    "nav_before_trade": "交易前净值",
    "nav_after_trade": "交易后净值",
}
SCAN_COLUMN_LABELS = {
    "scan_id": "扫描编号",
    "rank": "排名",
    "signal_count": "信号数",
    "closed_trade_candidates": "候选平仓交易数",
    "executed_trades": "实际执行交易数",
    "strategy_win_rate_pct": "策略胜率",
    "total_return_pct": "总收益率",
    "max_drawdown_pct": "最大回撤",
    "final_net_value": "最终净值",
    "avg_holding_days": "平均持有天数",
    "profit_risk_ratio": "收益风险比",
    "trade_return_volatility_pct": "单笔收益波动率",
}
PARTIAL_EXIT_MODE_LABELS = {
    "fixed_tp": "固定止盈",
    "ma_exit": "均线离场",
    "profit_drawdown": "利润回撤",
    "atr_trailing": "ATR 跟踪止盈",
}
BACKTEST_RANGE_PRESETS = (
    ("10年至今", 10),
    ("7年至今", 7),
    ("5年至今", 5),
    ("3年至今", 3),
)
ENTRY_FACTOR_LABELS = {
    "gap": "跳空",
    "trend_breakout": "趋势突破",
    "volatility_contraction_breakout": "波动收缩突破",
    "candle_run": "连续K线追势",
    "candle_run_acceleration": "连续K线加速追势",
    "early_surge_high_base": "早盘冲高高位横盘突破",
}
FACTOR_SPECIFIC_WIDGET_KEYS = {
    "gap": ("gap_entry_mode", "gap_pct", "max_gap_filter_pct"),
    "trend_breakout": ("trend_breakout_lookback",),
    "volatility_contraction_breakout": (
        "vcb_range_lookback",
        "vcb_breakout_lookback",
    ),
    "candle_run": (
        "candle_run_length",
        "candle_run_min_body_pct",
        "candle_run_total_move_pct",
    ),
    "candle_run_acceleration": (
        "candle_run_length",
        "candle_run_min_body_pct",
        "candle_run_total_move_pct",
    ),
    "early_surge_high_base": (
        "eshb_open_window_bars",
        "eshb_base_min_bars",
        "eshb_base_max_bars",
        "eshb_surge_min_pct",
        "eshb_max_base_pullback_pct",
        "eshb_max_base_range_pct",
        "eshb_max_anchor_breaks",
        "eshb_max_anchor_break_depth_pct",
        "eshb_min_open_volume_ratio",
        "eshb_min_breakout_volume_ratio",
        "eshb_trigger_buffer_pct",
    ),
}
FACTOR_CONTROL_DEFAULTS: dict[str, str | int | float] = {
    "gap_entry_mode": "strict_break",
    "gap_pct": 2.0,
    "max_gap_filter_pct": 9.9,
    "trend_breakout_lookback": 20,
    "vcb_range_lookback": 7,
    "vcb_breakout_lookback": 20,
    "candle_run_length": 2,
    "candle_run_min_body_pct": 1.0,
    "candle_run_total_move_pct": 2.0,
    "eshb_open_window_bars": 6,
    "eshb_base_min_bars": 2,
    "eshb_base_max_bars": 8,
    "eshb_surge_min_pct": 3.0,
    "eshb_max_base_pullback_pct": 2.5,
    "eshb_max_base_range_pct": 2.0,
    "eshb_max_anchor_breaks": 1,
    "eshb_max_anchor_break_depth_pct": 0.8,
    "eshb_min_open_volume_ratio": 1.2,
    "eshb_min_breakout_volume_ratio": 1.0,
    "eshb_trigger_buffer_pct": 0.05,
    "atr_filter_period": 14,
    "min_atr_filter_pct": 0.0,
    "max_atr_filter_pct": 100.0,
    "atr_trailing_period": 14,
    "atr_trailing_multiplier": 3.0,
}
SCAN_AXIS_STATE_KEYS = (
    ("scan_axis_1_field", "scan_axis_1_values"),
    ("scan_axis_2_field", "scan_axis_2_values"),
)
SUMMARY_COLUMN_LABELS = {
    "date": "开仓日期",
    "signal_count": "信号数",
    "executed_trades": "实际执行交易数",
    "win_rate_pct": "胜率",
    "avg_net_return_pct": "平均净收益率",
    "median_net_return_pct": "净收益率中位数",
    "avg_holding_days": "平均持有天数",
}
EQUITY_COLUMN_LABELS = {
    "date": "日期",
    "net_value": "净值",
    "drawdown_pct": "回撤",
}
UPDATE_LOG_COLUMN_LABELS = {
    "symbol": "股票代码",
    "timeframe": "周期",
    "adjust": "复权方式",
    "start_date": "开始日期",
    "end_date": "结束日期",
    "rows": "更新行数",
    "updated_at": "更新时间",
    "status": "状态",
    "error_message": "错误信息",
}
ENTRY_DIRECTION_OPTIONS = {
    "gap": (("向上跳空", "up"), ("向下跳空", "down")),
    "trend_breakout": (("向上突破", "up"), ("向下突破", "down")),
    "volatility_contraction_breakout": (("向上突破", "up"), ("向下突破", "down")),
    "candle_run": (("连续阳线追涨", "up"), ("连续阴线追空", "down")),
    "candle_run_acceleration": (
        ("连续阳线加速追涨", "up"),
        ("连续阴线加速追空", "down"),
    ),
    "early_surge_high_base": (("早盘冲高高位横盘突破", "up"),),
}
LOCAL_INVENTORY_COLUMN_LABELS = {
    "symbol": "股票",
    "timeframe": "周期",
    "row_count": "行数",
    "date_range": "时间范围",
    "updated_at": "更新时间",
    "last_update_status": "状态",
}
TIMEFRAME_OPTIONS = ("1d", "30m", "15m", "5m")


def dataframe_stretch(
    data: Any,
    *,
    hide_index: bool = False,
    column_config: Any = None,
    height: int | None = None,
) -> None:
    kwargs: dict[str, Any] = {
        "hide_index": hide_index,
        "width": "stretch",
    }
    if column_config is not None:
        kwargs["column_config"] = column_config
    if height is not None:
        kwargs["height"] = height
    try:
        st.dataframe(data, **kwargs)
    except TypeError as exc:
        if "interpreted as an integer" not in str(exc):
            raise
        legacy_kwargs = dict(kwargs)
        legacy_kwargs.pop("width", None)
        legacy_kwargs["use_container_width"] = True
        st.dataframe(data, **legacy_kwargs)


def inject_custom_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            max-width: 1280px;
            padding-top: 1.8rem;
            padding-bottom: 3rem;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f8fafc 0%, #f3f6fb 100%);
            border-right: 1px solid #e4e9f2;
        }
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e6ebf2;
            border-radius: 18px;
            padding: 0.9rem 1rem;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
        }
        [data-testid="stExpander"] {
            border: 1px solid #e6ebf2;
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.04);
            background: #ffffff;
        }
        .stButton > button, .stDownloadButton > button {
            border-radius: 12px;
            font-weight: 600;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 999px;
            padding: 0.45rem 0.9rem;
            background: #f4f7fb;
        }
        .stTabs [aria-selected="true"] {
            background: #e8f0ff;
            color: #1d4ed8;
        }
        .app-hero {
            padding: 1.2rem 1.35rem;
            border: 1px solid #e6ebf2;
            border-radius: 22px;
            background: linear-gradient(135deg, #ffffff 0%, #f7faff 100%);
            box-shadow: 0 16px 40px rgba(15, 23, 42, 0.06);
            margin-bottom: 1rem;
        }
        .app-hero h1 {
            margin: 0;
            font-size: 2rem;
            font-weight: 700;
            color: #0f172a;
        }
        .app-hero p {
            margin: 0.4rem 0 0;
            color: #475569;
            font-size: 0.98rem;
        }
        .section-title {
            margin: 0.2rem 0 0;
            font-size: 1.18rem;
            font-weight: 700;
            color: #0f172a;
        }
        .section-caption {
            margin: 0.25rem 0 0.85rem;
            color: #64748b;
            font-size: 0.94rem;
        }
        .guide-card {
            background: #ffffff;
            border: 1px solid #e6ebf2;
            border-radius: 18px;
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.04);
            margin-bottom: 0.9rem;
        }
        .guide-card h3 {
            margin: 0 0 0.5rem;
            font-size: 1.02rem;
            color: #0f172a;
        }
        .guide-card p, .guide-card li {
            color: #475569;
            line-height: 1.65;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def section_header(title: str, caption: str | None = None) -> None:
    st.markdown(f"<div class='section-title'>{title}</div>", unsafe_allow_html=True)
    if caption:
        st.markdown(
            f"<div class='section-caption'>{caption}</div>",
            unsafe_allow_html=True,
        )


def preset_start_date(today: date, years: int) -> date:
    years_int = int(years)
    try:
        return today.replace(year=today.year - years_int)
    except ValueError:
        return today.replace(year=today.year - years_int, month=2, day=28)


def get_direction_options(entry_factor: str) -> list[str]:
    return [
        label
        for label, _ in ENTRY_DIRECTION_OPTIONS.get(
            entry_factor, ENTRY_DIRECTION_OPTIONS["gap"]
        )
    ]


def normalize_direction_label(entry_factor: str, current_label: str | None) -> str:
    options = get_direction_options(entry_factor)
    if current_label in options:
        return str(current_label)
    return options[0]


def direction_label_to_internal(entry_factor: str, direction_label: str) -> str:
    for label, internal_value in ENTRY_DIRECTION_OPTIONS.get(
        entry_factor, ENTRY_DIRECTION_OPTIONS["gap"]
    ):
        if label == direction_label:
            return internal_value
    return ENTRY_DIRECTION_OPTIONS[entry_factor][0][1]


def render_direction_selectbox(entry_factor: str, container: Any | None = None) -> str:
    st.session_state["direction_label"] = normalize_direction_label(
        entry_factor, st.session_state.get("direction_label")
    )
    selectbox = container.selectbox if container is not None else st.selectbox
    return str(
        selectbox(
            "交易方向",
            options=get_direction_options(entry_factor),
            key="direction_label",
        )
    )


def render_trading_guide_page() -> None:
    st.markdown(
        """
        <div class='app-hero'>
            <h1>交易配置说明</h1>
            <p>如果你是第一次用，照着“先选数据 → 再选买点 → 再设卖点 → 最后看结果”的顺序配，最不容易出错。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class='guide-card'>
            <h3>1. 先把回测范围定清楚</h3>
            <p>先决定“看哪些股票、看多长时间、用哪份数据”。股票池留空就是全市场；时间拉得越长，回测越慢。数据源优先推荐本地 Parquet，因为速度和稳定性都更好。当前支持 1d 日线研究，也支持早盘冲高高位横盘突破因子的 30m 形态 + 5m 执行路径。</p>
        </div>
        <div class='guide-card'>
            <h3>2. 入场因子可以直接理解成“什么时候开仓”</h3>
            <ul>
                <li><b>跳空</b>：适合看“今天一开盘就明显高开 / 低开”的机会。</li>
                <li><b>趋势突破</b>：适合看“价格冲过过去 N 天最高点 / 跌破过去 N 天最低点”的机会。</li>
                <li><b>波动收缩突破</b>：先找“前面波动越来越小”，再等后面放量突破，适合做蓄势后的发力段。</li>
                <li><b>连续K线追势</b>：前面已经连续上涨 / 下跌几根，下一根开盘顺着方向追进去。</li>
                <li><b>连续K线加速追势</b>：和上面类似，但要求走势不是普通连涨连跌，而是越来越强。</li>
            </ul>
            <p>简单选法：想抓 <b>开盘异动</b> 就先试跳空；想抓 <b>突破新高/新低</b> 就试趋势突破；想抓 <b>整理后突然启动</b> 就试波动收缩突破。</p>
        </div>
        <div class='guide-card'>
            <h3>3. 卖点和风控，先用最容易理解的几个</h3>
            <ul>
                <li><b>全仓止损</b>：做错了最多亏多少，先把底线定住。</li>
                <li><b>时间退出</b>：拿到第 N 天还没走出你想要的表现，就按规则结束，不一直死扛。</li>
                <li><b>固定止盈</b>：涨到 / 跌到你设的目标价就走，最直观。</li>
                <li><b>均线离场</b>：走势转弱 / 转强到某条均线另一侧时离场。</li>
                <li><b>ATR 跟踪止盈</b>：价格先往有利方向走，再按“波动幅度”动态抬高止盈线，适合让利润奔跑。</li>
                <li><b>利润回撤</b>：先允许仓位赚起来，等利润从高点回撤到你设的比例再走，适合防止“赚过很多最后吐回去”。</li>
            </ul>
            <p>如果你只想先跑通一版，建议先配 <b>止损 + 时间退出 + 固定止盈</b>。这三个最容易理解，也最方便排查回测结果。</p>
        </div>
        <div class='guide-card'>
            <h3>4. 分批止盈可以理解成“分几次卖”</h3>
            <p>不开启分批时，默认是一笔买入、一笔卖出。开启后，你可以把仓位拆成 2~3 批，比如“先卖 50% 锁利润，剩下 50% 继续拿”。</p>
            <ul>
                <li><b>仓位比例</b>：这一批卖多少。</li>
                <li><b>执行优先级</b>：同一天多个条件都满足时，先执行谁。数字越小越先执行。</li>
                <li><b>固定止盈 / 均线离场 / ATR 跟踪 / 利润回撤</b>：表示这一批用哪种卖法。</li>
            </ul>
            <p>实用例子：第 1 批用固定止盈先落袋，第 2 批用 ATR 跟踪或利润回撤去吃后面的趋势。</p>
        </div>
        <div class='guide-card'>
            <h3>5. 参数扫描不是“越多越好”，而是先小后大</h3>
            <p>参数扫描就是批量帮你试很多组参数。建议先拿 1~2 个参数、小范围、少组合试跑，确认逻辑没问题，再扩大范围。不然很容易跑很久，还看不出重点。</p>
            <p>看结果时，先看三件事：<b>收益率</b>、<b>最大回撤</b>、<b>交易明细</b>。如果总收益看起来不错，但回撤很大、成交方式也不符合预期，那这组参数通常不值得继续深挖。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def form_submit_button_stretch(label: str) -> bool:
    return st.form_submit_button(label)


def clear_result_state() -> None:
    for key in RESULT_STATE_KEYS:
        st.session_state.pop(key, None)


def factor_control_default(field_name: str) -> Any:
    return FACTOR_CONTROL_DEFAULTS[field_name]


def reset_inactive_factor_controls(active_factor: str) -> None:
    active_widget_keys = set(FACTOR_SPECIFIC_WIDGET_KEYS.get(active_factor, ()))
    for factor_name, widget_keys in FACTOR_SPECIFIC_WIDGET_KEYS.items():
        if factor_name == active_factor:
            continue
        for widget_key in widget_keys:
            if widget_key in active_widget_keys:
                continue
            st.session_state[widget_key] = factor_control_default(widget_key)


def build_factor_scan_field_options(active_factor: str) -> list[str]:
    eligible_fields = FACTOR_SCAN_ELIGIBLE_FIELDS.get(active_factor, frozenset())
    ordered_fields = [
        field_name for field_name in SCAN_FIELD_CASTERS if field_name in eligible_fields
    ]
    return [""] + ordered_fields


def reset_invalid_scan_axis_state(scan_field_options: list[str]) -> None:
    valid_fields = set(scan_field_options)
    for field_key, values_key in SCAN_AXIS_STATE_KEYS:
        current_field = str(st.session_state.get(field_key, ""))
        if current_field not in valid_fields:
            st.session_state[field_key] = ""
            st.session_state[values_key] = ""


def build_data_format_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "列名建议": "交易日期",
                "是否必填": "必填",
                "说明": "交易日，一行代表一只股票在一个交易日的数据",
                "示例": "2026-03-13",
            },
            {
                "列名建议": "股票代码",
                "是否必填": "必填",
                "说明": "股票唯一标识",
                "示例": "000001.SZ",
            },
            {
                "列名建议": "开盘价",
                "是否必填": "必填",
                "说明": "当天开盘价",
                "示例": "10.52",
            },
            {
                "列名建议": "最高价",
                "是否必填": "必填",
                "说明": "当天最高价",
                "示例": "10.88",
            },
            {
                "列名建议": "最低价",
                "是否必填": "必填",
                "说明": "当天最低价",
                "示例": "10.31",
            },
            {
                "列名建议": "收盘价",
                "是否必填": "必填",
                "说明": "当天收盘价",
                "示例": "10.66",
            },
            {
                "列名建议": "成交量",
                "是否必填": "选填",
                "说明": "当天成交量，不填也可以分析",
                "示例": "1256300",
            },
        ]
    )


def build_sample_input_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "交易日期": "2026-03-10",
                "股票代码": "000001.SZ",
                "开盘价": 10.20,
                "最高价": 10.45,
                "最低价": 10.08,
                "收盘价": 10.32,
                "成交量": 1856200,
            },
            {
                "交易日期": "2026-03-11",
                "股票代码": "000001.SZ",
                "开盘价": 10.58,
                "最高价": 10.90,
                "最低价": 10.50,
                "收盘价": 10.84,
                "成交量": 2365400,
            },
            {
                "交易日期": "2026-03-10",
                "股票代码": "600000.SH",
                "开盘价": 8.85,
                "最高价": 8.93,
                "最低价": 8.72,
                "收盘价": 8.80,
                "成交量": 3124500,
            },
        ]
    )


def build_template_note() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "说明": [
                "请把表头放在第 1 行，不要在上方再加标题行。",
                "一行代表一只股票在一个交易日的数据，不能把多天数据横着放。",
                "至少需要交易日期、股票代码、开盘价、最高价、最低价、收盘价 6 列。",
                "支持的日期格式包括 2026-03-13、20260313、Excel 日期单元格。",
                "如果您的列名不同，可以在页面里用“字段映射”手动指定。",
            ]
        }
    )


def build_template_bytes() -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(cast(Any, buffer), engine="openpyxl") as writer:
        build_sample_input_data().to_excel(
            writer, sheet_name="行情数据模板", index=False
        )
        build_data_format_table().to_excel(writer, sheet_name="字段说明", index=False)
        build_template_note().to_excel(writer, sheet_name="填写说明", index=False)
    buffer.seek(0)
    return buffer.getvalue()


def format_number(value: Any, digits: int = 2) -> str:
    if pd.isna(value):
        return ""
    formatted = f"{float(value):,.{digits}f}"
    if "." not in formatted:
        return formatted
    return formatted.rstrip("0").rstrip(".")


def format_percent(value: Any, digits: int = 2) -> str:
    text = format_number(value, digits=digits)
    return f"{text}%" if text else ""


def format_timestamp_for_display(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    timestamp = cast(pd.Timestamp, pd.Timestamp(ts))
    if timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0:
        return timestamp.strftime("%Y-%m-%d")
    return timestamp.strftime("%Y-%m-%d %H:%M")


def format_detail_for_display(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return detail_df

    display_df = detail_df.copy()
    display_df["date"] = display_df["date"].map(format_timestamp_for_display)
    display_df["sell_date"] = display_df["sell_date"].map(format_timestamp_for_display)

    for column in DETAIL_PRICE_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_number)

    for column in DETAIL_PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)

    for column in DETAIL_NAV_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: format_number(value, 4)
            )

    for column in DETAIL_COUNT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )

    if "win_flag" in display_df.columns:
        display_df["win_flag"] = display_df["win_flag"].map(
            lambda value: (
                "是"
                if pd.notna(value) and int(value) == 1
                else ("否" if pd.notna(value) else "")
            )
        )

    if "volume" in display_df.columns:
        display_df["volume"] = display_df["volume"].map(
            lambda value: f"{value:,.0f}" if pd.notna(value) else ""
        )

    display_df = display_df.rename(
        columns={
            column: DETAIL_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in DETAIL_COLUMN_LABELS
        }
    )

    return display_df


def format_summary_for_display(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return daily_df

    display_df = daily_df.copy()
    display_df["date"] = display_df["date"].map(format_timestamp_for_display)
    for column in ("signal_count", "executed_trades"):
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
    for column in SUMMARY_PERCENT_COLUMNS:
        display_df[column] = display_df[column].map(format_percent)
    display_df["avg_holding_days"] = display_df["avg_holding_days"].map(format_number)
    display_df = display_df.rename(
        columns={
            column: SUMMARY_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in SUMMARY_COLUMN_LABELS
        }
    )
    return display_df


def format_equity_for_display(equity_df: pd.DataFrame) -> pd.DataFrame:
    if equity_df.empty:
        return equity_df

    display_df = equity_df.copy()
    display_df["date"] = display_df["date"].map(format_timestamp_for_display)
    display_df["net_value"] = display_df["net_value"].map(
        lambda value: format_number(value, 4)
    )
    for column in EQUITY_PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)
    display_df = display_df.rename(
        columns={
            column: EQUITY_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in EQUITY_COLUMN_LABELS
        }
    )
    return display_df


def build_download_name(start_date: str, end_date: str) -> str:
    return f"gap_analysis_{start_date}_{end_date}.xlsx"


def build_scan_axis(field_name: str, raw_text: str) -> ParamScanAxis | None:
    if field_name == "":
        return None
    values = parse_scan_values(field_name, raw_text)
    if not values:
        return None
    return ParamScanAxis(field_name=field_name, values=values)


def format_scan_for_display(scan_df: pd.DataFrame) -> pd.DataFrame:
    if scan_df.empty:
        return scan_df
    display_df = scan_df.copy()
    for column in display_df.columns:
        if column.endswith("_pct") and column != "rank":
            display_df[column] = display_df[column].map(format_percent)
        elif column == "final_net_value":
            display_df[column] = display_df[column].map(
                lambda value: format_number(value, 4)
            )
        elif column == "rank":
            display_df[column] = display_df[column].map(
                lambda value: int(value) if pd.notna(value) else ""
            )
        elif column in {
            "scan_id",
            "signal_count",
            "closed_trade_candidates",
            "executed_trades",
        }:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
        elif column in {
            "gap_pct",
            "max_gap_filter_pct",
            "trend_breakout_lookback",
            "vcb_range_lookback",
            "vcb_breakout_lookback",
            "candle_run_length",
            "candle_run_min_body_pct",
            "candle_run_total_move_pct",
            "eshb_open_window_bars",
            "eshb_base_min_bars",
            "eshb_base_max_bars",
            "eshb_surge_min_pct",
            "eshb_max_base_pullback_pct",
            "eshb_max_base_range_pct",
            "eshb_max_anchor_breaks",
            "eshb_max_anchor_break_depth_pct",
            "eshb_min_open_volume_ratio",
            "eshb_min_breakout_volume_ratio",
            "eshb_trigger_buffer_pct",
            "atr_filter_period",
            "min_atr_filter_pct",
            "max_atr_filter_pct",
            "time_stop_days",
            "time_stop_target_pct",
            "stop_loss_pct",
            "take_profit_pct",
            "profit_drawdown_pct",
            "exit_ma_period",
            "atr_trailing_period",
            "atr_trailing_multiplier",
            "buy_slippage_pct",
            "sell_slippage_pct",
            "partial_rule_1_target_profit_pct",
            "partial_rule_2_target_profit_pct",
            "partial_rule_3_target_profit_pct",
            "partial_rule_1_ma_period",
            "partial_rule_2_ma_period",
            "partial_rule_3_ma_period",
            "partial_rule_1_atr_period",
            "partial_rule_2_atr_period",
            "partial_rule_3_atr_period",
            "partial_rule_1_atr_multiplier",
            "partial_rule_2_atr_multiplier",
            "partial_rule_3_atr_multiplier",
            "partial_rule_1_drawdown_pct",
            "partial_rule_2_drawdown_pct",
            "partial_rule_3_drawdown_pct",
            "partial_rule_1_min_profit_to_activate_drawdown",
            "partial_rule_2_min_profit_to_activate_drawdown",
            "partial_rule_3_min_profit_to_activate_drawdown",
            "avg_holding_days",
            "profit_risk_ratio",
            "trade_return_volatility_pct",
        }:
            display_df[column] = display_df[column].map(format_number)
    display_df = display_df.rename(
        columns={
            column: (
                SCAN_COLUMN_LABELS.get(column)
                or SCAN_FIELD_LABELS.get(column)
                or SCAN_METRIC_LABELS.get(column)
                or column
            )
            for column in display_df.columns
        }
    )
    return display_df


def format_update_log_for_display(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return preview_df

    display_df = preview_df.copy()
    for column in ("start_date", "end_date"):
        if column in display_df.columns:
            display_df[column] = pd.to_datetime(
                display_df[column], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
    if "updated_at" in display_df.columns:
        display_df["updated_at"] = pd.to_datetime(
            display_df["updated_at"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M")
    if "rows" in display_df.columns:
        display_df["rows"] = display_df["rows"].map(
            lambda value: f"{int(value):,}" if pd.notna(value) else ""
        )
    display_df = display_df.rename(
        columns={
            column: UPDATE_LOG_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in UPDATE_LOG_COLUMN_LABELS
        }
    )
    return display_df


def summarize_stock_scope(stock_scope_text: str) -> str:
    stock_codes = normalize_stock_codes(stock_scope_text)
    if not stock_codes:
        return "全市场"
    preview = "、".join(stock_codes[:3])
    if len(stock_codes) <= 3:
        return preview
    return f"{preview} 等 {len(stock_codes)} 只"


def summarize_data_source(
    data_source_label: str,
    *,
    adjust_label: str,
    local_data_root: str,
    db_path: str,
    table_name: str,
    input_file_path: str,
    uploaded_market_file: Any | None,
) -> tuple[str, str]:
    if data_source_label == "本地 Parquet（AKShare 离线）":
        return "本地 Parquet", f"目录：{local_data_root} · 复权：{adjust_label}"
    if data_source_label == "SQLite 数据库":
        db_name = Path(db_path).name if db_path.strip() else "未填写路径"
        table_text = table_name.strip() or "自动识别表"
        return "SQLite", f"数据库：{db_name} · 数据表：{table_text}"

    file_name = "待选择文件"
    if uploaded_market_file is not None:
        file_name = uploaded_market_file.name
    elif input_file_path.strip():
        file_name = Path(input_file_path.strip()).name
    return "文件导入", f"文件：{file_name} · 复权：{adjust_label}"


def build_update_log_column_config() -> dict[str, object]:
    return {
        "股票代码": st.column_config.TextColumn("股票代码", width="small"),
        "周期": st.column_config.TextColumn("周期", width="small"),
        "复权方式": st.column_config.TextColumn("复权方式", width="small"),
        "开始日期": st.column_config.TextColumn("开始日期", width="small"),
        "结束日期": st.column_config.TextColumn("结束日期", width="small"),
        "更新行数": st.column_config.TextColumn("更新行数", width="small"),
        "更新时间": st.column_config.TextColumn("更新时间", width="medium"),
        "状态": st.column_config.TextColumn("状态", width="small"),
        "错误信息": st.column_config.TextColumn("错误信息", width="large"),
    }


def build_summary_column_config() -> dict[str, object]:
    return {
        "开仓日期": st.column_config.TextColumn("开仓日期", width="small"),
        "信号数": st.column_config.TextColumn("信号数", width="small"),
        "实际执行交易数": st.column_config.TextColumn("实际执行交易数", width="small"),
        "胜率": st.column_config.TextColumn("胜率", width="small"),
        "平均净收益率": st.column_config.TextColumn("平均净收益率", width="small"),
        "净收益率中位数": st.column_config.TextColumn("净收益率中位数", width="small"),
        "平均持有天数": st.column_config.TextColumn("平均持有天数", width="small"),
    }


def build_equity_column_config() -> dict[str, object]:
    return {
        "日期": st.column_config.TextColumn("日期", width="small"),
        "净值": st.column_config.TextColumn("净值", width="small"),
        "回撤": st.column_config.TextColumn("回撤", width="small"),
    }


def build_detail_column_config() -> dict[str, object]:
    return {
        "信号日期": st.column_config.TextColumn("信号日期", width="small"),
        "股票代码": st.column_config.TextColumn("股票代码", width="small"),
        "开仓原因": st.column_config.TextColumn("开仓原因", width="medium"),
        "离场原因": st.column_config.TextColumn("离场原因", width="medium"),
        "相对昨收跳空幅度": st.column_config.TextColumn(
            "相对昨收跳空幅度", width="small"
        ),
        "买入日期": st.column_config.TextColumn("买入日期", width="small"),
        "卖出日期": st.column_config.TextColumn("卖出日期", width="small"),
        "退出方式": st.column_config.TextColumn("退出方式", width="small"),
        "持有天数": st.column_config.TextColumn("持有天数", width="small"),
        "净收益率": st.column_config.TextColumn("净收益率", width="small"),
        "最大有利波动": st.column_config.TextColumn("最大有利波动", width="small"),
        "最大不利波动": st.column_config.TextColumn("最大不利波动", width="small"),
        "成交明细": st.column_config.TextColumn("成交明细", width="large"),
    }


def build_scan_column_config() -> dict[str, object]:
    return {
        "扫描编号": st.column_config.TextColumn("扫描编号", width="small"),
        "排名": st.column_config.TextColumn("排名", width="small"),
        "跳空幅度": st.column_config.TextColumn("跳空幅度", width="small"),
        "最大高开/低开过滤": st.column_config.TextColumn(
            "最大高开/低开过滤", width="small"
        ),
        "趋势突破回看天数": st.column_config.TextColumn(
            "趋势突破回看天数", width="small"
        ),
        "波动收缩区间回看天数": st.column_config.TextColumn(
            "波动收缩区间回看天数", width="small"
        ),
        "波动收缩突破回看天数": st.column_config.TextColumn(
            "波动收缩突破回看天数", width="small"
        ),
        "连续K线根数": st.column_config.TextColumn("连续K线根数", width="small"),
        "ATR过滤周期": st.column_config.TextColumn("ATR过滤周期", width="small"),
        "最小ATR波动过滤": st.column_config.TextColumn(
            "最小ATR波动过滤", width="small"
        ),
        "最大ATR波动过滤": st.column_config.TextColumn(
            "最大ATR波动过滤", width="small"
        ),
        "单根最小实体幅度": st.column_config.TextColumn(
            "单根最小实体幅度", width="small"
        ),
        "组合最小累计涨跌幅": st.column_config.TextColumn(
            "组合最小累计涨跌幅", width="small"
        ),
        "早盘观察窗口K数": st.column_config.TextColumn("早盘观察窗口K数", width="small"),
        "高位横盘最少K数": st.column_config.TextColumn("高位横盘最少K数", width="small"),
        "高位横盘最多K数": st.column_config.TextColumn("高位横盘最多K数", width="small"),
        "早盘冲高最小涨幅": st.column_config.TextColumn("早盘冲高最小涨幅", width="small"),
        "横盘最大回撤": st.column_config.TextColumn("横盘最大回撤", width="small"),
        "横盘最大振幅": st.column_config.TextColumn("横盘最大振幅", width="small"),
        "锚点跌破次数上限": st.column_config.TextColumn("锚点跌破次数上限", width="small"),
        "锚点跌破深度上限": st.column_config.TextColumn("锚点跌破深度上限", width="small"),
        "冲高量能倍数下限": st.column_config.TextColumn("冲高量能倍数下限", width="small"),
        "突破量能倍数下限": st.column_config.TextColumn("突破量能倍数下限", width="small"),
        "突破触发缓冲": st.column_config.TextColumn("突破触发缓冲", width="small"),
        "ATR跟踪周期": st.column_config.TextColumn("ATR跟踪周期", width="small"),
        "ATR跟踪倍数": st.column_config.TextColumn("ATR跟踪倍数", width="small"),
        "最多持有天数": st.column_config.TextColumn("最多持有天数", width="small"),
        "策略胜率": st.column_config.TextColumn("策略胜率", width="small"),
        "总收益率": st.column_config.TextColumn("总收益率", width="small"),
        "最大回撤": st.column_config.TextColumn("最大回撤", width="small"),
        "最终净值": st.column_config.TextColumn("最终净值", width="small"),
        "平均持有天数": st.column_config.TextColumn("平均持有天数", width="small"),
        "收益风险比": st.column_config.TextColumn("收益风险比", width="small"),
    }


def build_local_inventory_column_config() -> dict[str, object]:
    return {
        "股票": st.column_config.TextColumn("股票", width="small"),
        "周期": st.column_config.TextColumn("周期", width="small"),
        "行数": st.column_config.TextColumn("行数", width="small"),
        "时间范围": st.column_config.TextColumn("时间范围", width="medium"),
        "更新时间": st.column_config.TextColumn("更新时间", width="medium"),
        "状态": st.column_config.TextColumn("状态", width="small"),
    }


def run_local_data_update(
    symbols_text: str,
    start_date: str,
    end_date: str,
    adjust: str,
    timeframe: str,
    refresh_symbols: bool,
    export_excel: bool,
) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        "scripts/update_data.py",
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]
    if adjust:
        cmd.extend(["--adjust", adjust])
    if timeframe:
        cmd.extend(["--timeframe", timeframe])
    if symbols_text.strip():
        cmd.extend(["--symbols", symbols_text.strip()])
    if refresh_symbols:
        cmd.append("--refresh-symbols")
    if export_excel:
        cmd.append("--export-excel")

    result = subprocess.run(cmd, capture_output=True, text=True)
    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode == 0, output.strip()


def build_export_dir_hint(timeframe: str, adjust: str) -> str:
    return f"data/market/exports/{adjust}/" if timeframe == "1d" else f"data/market/exports/{timeframe}/{adjust}/"


def load_update_log_preview(limit: int = 20) -> pd.DataFrame:
    log_file = Path("data/market/metadata/update_log.parquet")
    if not log_file.exists():
        return pd.DataFrame(
            columns=pd.Index(
                [
                    "symbol",
                    "adjust",
                    "start_date",
                    "end_date",
                    "rows",
                    "updated_at",
                    "status",
                    "error_message",
                ]
            )
        )
    try:
        df = pd.read_parquet(log_file)
    except Exception:
        return pd.DataFrame(
            columns=pd.Index(
                [
                    "symbol",
                    "adjust",
                    "start_date",
                    "end_date",
                    "rows",
                    "updated_at",
                    "status",
                    "error_message",
                ]
            )
        )

    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df = df.sort_values("updated_at", ascending=False)
    return df.head(limit).reset_index(drop=True)


def load_local_inventory_preview(limit: int = 20) -> pd.DataFrame:
    inventory = load_inventory()
    if inventory.empty:
        return pd.DataFrame(columns=pd.Index(["symbol", "timeframe", "row_count", "date_range", "updated_at", "last_update_status"]))
    preview = inventory.copy()
    for column in ("min_date", "max_date", "updated_at"):
        if column not in preview.columns:
            preview[column] = pd.NaT
        preview[column] = pd.to_datetime(preview[column], errors="coerce")
    def _format_inventory_range(row: pd.Series) -> str:
        min_date = row.get("min_date")
        max_date = row.get("max_date")
        if bool(pd.isna(min_date)) or bool(pd.isna(max_date)):
            return ""
        min_ts = cast(pd.Timestamp, pd.to_datetime(cast(Any, min_date), errors="coerce"))
        max_ts = cast(pd.Timestamp, pd.to_datetime(cast(Any, max_date), errors="coerce"))
        return f"{min_ts.strftime('%Y-%m-%d %H:%M')} → {max_ts.strftime('%Y-%m-%d %H:%M')}"

    preview["date_range"] = preview.apply(_format_inventory_range, axis=1)
    preview = preview.sort_values("updated_at", ascending=False)
    return cast(
        pd.DataFrame,
        preview[["symbol", "timeframe", "row_count", "date_range", "updated_at", "last_update_status"]]
        .head(limit)
        .reset_index(drop=True),
    )


def format_local_inventory_for_display(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return preview_df
    display_df = preview_df.copy()
    if "row_count" in display_df.columns:
        display_df["row_count"] = display_df["row_count"].map(lambda value: f"{int(value):,}" if pd.notna(value) else "")
    if "updated_at" in display_df.columns:
        display_df["updated_at"] = pd.to_datetime(display_df["updated_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    return display_df.rename(columns={column: LOCAL_INVENTORY_COLUMN_LABELS[column] for column in display_df.columns if column in LOCAL_INVENTORY_COLUMN_LABELS})


def _read_sqlite_preview_rows(db_path: str, table_name: str, limit: int = 20) -> pd.DataFrame:
    if not db_path.strip() or not table_name.strip():
        return pd.DataFrame()
    with sqlite3.connect(db_path.strip()) as conn:
        query = f"SELECT * FROM {quote_ident(table_name.strip())} LIMIT {int(limit)}"
        return pd.read_sql_query(query, conn)


def _read_file_preview_rows(
    *,
    file_path: str | None,
    file_bytes: bytes | None,
    file_name: str | None,
    sheet_name: str | None,
    limit: int = 20,
) -> pd.DataFrame:
    source_name = file_name or file_path or ""
    suffix = Path(source_name).suffix.lower()
    if not source_name:
        return pd.DataFrame()
    if suffix == ".csv":
        encodings = ("utf-8-sig", "gb18030", "utf-8")
        for encoding in encodings:
            try:
                if file_bytes is not None:
                    return pd.read_csv(BytesIO(file_bytes), encoding=encoding).head(limit)
                if file_path:
                    return pd.read_csv(file_path, encoding=encoding).head(limit)
            except UnicodeDecodeError:
                continue
        raise ValueError("CSV 文件预览失败。")

    if file_bytes is not None:
        with pd.ExcelFile(BytesIO(file_bytes)) as workbook:
            target_sheet = sheet_name.strip() if sheet_name and sheet_name.strip() else workbook.sheet_names[0]
            return pd.read_excel(workbook, sheet_name=target_sheet).head(limit)
    if file_path:
        with pd.ExcelFile(file_path) as workbook:
            target_sheet = sheet_name.strip() if sheet_name and sheet_name.strip() else workbook.sheet_names[0]
            return pd.read_excel(workbook, sheet_name=target_sheet).head(limit)
    return pd.DataFrame()


def _resolve_uploaded_file_inputs(
    uploaded_market_file: Any | None,
    input_file_path: str,
) -> tuple[bytes | None, str | None, str | None]:
    uploaded_file_bytes = uploaded_market_file.getvalue() if uploaded_market_file is not None else None
    uploaded_file_name = uploaded_market_file.name if uploaded_market_file is not None else None
    normalized_path = input_file_path.strip() or None
    return uploaded_file_bytes, uploaded_file_name, normalized_path


def build_sqlite_probe_payload(db_path: str, selected_table: str) -> dict[str, Any]:
    candidate_tables = list_candidate_tables(db_path)
    overview_df = pd.DataFrame(describe_tables(db_path))
    resolved_table = selected_table.strip() or (candidate_tables[0] if candidate_tables else "")
    preview_df = _read_sqlite_preview_rows(db_path, resolved_table, limit=20) if resolved_table else pd.DataFrame()
    return {
        "candidate_tables": candidate_tables,
        "overview_df": overview_df,
        "selected_table": resolved_table,
        "preview_df": preview_df,
    }


def build_file_probe_payload(
    *,
    file_path: str | None,
    file_bytes: bytes | None,
    file_name: str | None,
    sheet_name: str | None,
) -> dict[str, Any]:
    sheet_names = list_file_sheets(file_path=file_path, file_bytes=file_bytes, file_name=file_name)
    resolved_sheet = sheet_name.strip() if sheet_name and sheet_name.strip() else (sheet_names[0] if sheet_names else None)
    description = describe_file_source(
        file_path=file_path,
        file_bytes=file_bytes,
        file_name=file_name,
        sheet_name=resolved_sheet,
    )
    preview_df = _read_file_preview_rows(
        file_path=file_path,
        file_bytes=file_bytes,
        file_name=file_name,
        sheet_name=resolved_sheet,
        limit=20,
    )
    return {
        "sheet_names": sheet_names,
        "resolved_sheet": resolved_sheet,
        "description": description,
        "preview_df": preview_df,
    }


def build_pre_backtest_source_summary(
    *,
    data_source_label: str,
    timeframe: str,
    adjust_label: str,
    local_data_root: str,
    db_path: str,
    table_name: str,
    input_file_path: str,
    uploaded_market_file: Any | None,
    excel_sheet_name: str,
) -> pd.DataFrame:
    target = local_data_root
    auto_detected = "固定 schema"
    if data_source_label == "SQLite 数据库":
        try:
            sqlite_payload = build_sqlite_probe_payload(db_path, table_name)
            target = sqlite_payload["selected_table"] or "未识别数据表"
            if not sqlite_payload["overview_df"].empty and target:
                matched = sqlite_payload["overview_df"].loc[sqlite_payload["overview_df"]["table_name"] == target]
                auto_detected = str(matched.iloc[0]["auto_detected"]) if not matched.empty else "未探测"
            else:
                auto_detected = "未探测"
        except Exception:
            target = table_name.strip() or Path(db_path).name or "未识别数据表"
            auto_detected = "未探测"
    elif data_source_label == "Excel/CSV 文件":
        file_bytes, file_name, file_path = _resolve_uploaded_file_inputs(uploaded_market_file, input_file_path)
        target = file_name or (Path(file_path).name if file_path else "待选择文件")
        try:
            file_payload = build_file_probe_payload(
                file_path=file_path,
                file_bytes=file_bytes,
                file_name=file_name,
                sheet_name=excel_sheet_name,
            )
            resolved_sheet = file_payload["resolved_sheet"]
            if resolved_sheet:
                target = f"{target} / {resolved_sheet}"
            auto_detected = "是" if bool(file_payload["description"].get("auto_detected")) else "否"
        except Exception:
            auto_detected = "未探测"
    return pd.DataFrame(
        [{
            "数据源类型": data_source_label,
            "timeframe": timeframe,
            "adjust": adjust_label,
            "当前表/文件/sheet": target,
            "是否自动识别成功": auto_detected,
        }]
    )


@st.cache_data(show_spinner=False)
def load_market_data_cached(
    *,
    source_type: str,
    start_date: str,
    end_date: str,
    stock_codes: tuple[str, ...],
    table_name: str | None,
    column_override_items: tuple[tuple[str, str], ...],
    lookback_days: int,
    lookahead_days: int,
    db_path: str | None,
    file_path: str | None,
    file_bytes: bytes | None,
    file_name: str | None,
    sheet_name: str | None,
    local_data_root: str,
    adjust: str,
    timeframe: str,
) -> pd.DataFrame:
    return load_market_data(
        source_type=source_type,
        start_date=start_date,
        end_date=end_date,
        stock_codes=stock_codes,
        table_name=table_name,
        column_overrides=dict(column_override_items),
        lookback_days=lookback_days,
        lookahead_days=lookahead_days,
        db_path=db_path,
        file_path=file_path,
        file_bytes=file_bytes,
        file_name=file_name,
        sheet_name=sheet_name,
        local_data_root=local_data_root,
        adjust=adjust,
        timeframe=timeframe,
    )


inject_custom_styles()

st.sidebar.markdown("**页面导航**")
page_mode = st.sidebar.radio(
    "页面",
    options=["回测工作台", "交易配置说明"],
    label_visibility="collapsed",
)
st.sidebar.caption("可在工作台与说明页之间切换。")

if page_mode == "交易配置说明":
    render_trading_guide_page()
    st.stop()

st.markdown(
    """
    <div class='app-hero'>
        <h1>Gap_test 回测系统</h1>
        <p>离线数据更新、策略配置与结果分析统一在一个研究工作台中完成。</p>
    </div>
    """,
    unsafe_allow_html=True,
)

today = pd.Timestamp.today().date()
default_update_start = today - timedelta(days=30)
default_backtest_start = today - timedelta(days=365)

section_header(
    "数据准备",
    "推荐先离线更新本地 parquet，再进行回测；更新区保留在顶部，避免与回测配置混在一起。",
)

with st.expander("本地行情更新（离线下载）", expanded=False):
    up_c1, up_c2, up_c3 = st.columns(3)
    with up_c1:
        update_symbols = st.text_area(
            "股票代码（可选）", value="", help="多个代码用逗号分隔，空表示全量。"
        )
    with up_c2:
        update_start = st.date_input(
            "更新开始日期",
            value=default_update_start,
            key="offline_update_start",
        )
        update_end = st.date_input(
            "更新结束日期", value=today, key="offline_update_end"
        )
    with up_c3:
        update_timeframe = st.selectbox(
            "更新周期", options=list(TIMEFRAME_OPTIONS), index=0, key="offline_update_timeframe"
        )
        update_adjust = st.selectbox(
            "更新复权方式", options=["qfq", "hfq"], index=0, key="offline_update_adjust"
        )
        refresh_symbol_meta = st.checkbox(
            "刷新股票列表", value=False, key="offline_update_refresh"
        )
        export_excel_after_update = st.checkbox(
            "更新后另存为 Excel", value=False, key="offline_update_export"
        )

    if st.button("开始更新本地数据", key="offline_update_submit"):
        ok, output = run_local_data_update(
            symbols_text=update_symbols,
            start_date=update_start.strftime("%Y-%m-%d"),
            end_date=update_end.strftime("%Y-%m-%d"),
            adjust=update_adjust,
            timeframe=str(update_timeframe),
            refresh_symbols=bool(refresh_symbol_meta),
            export_excel=bool(export_excel_after_update),
        )
        if ok:
            st.success("本地数据更新完成")
        else:
            st.error("本地数据更新失败")
        st.caption(f"导出目录：{build_export_dir_hint(str(update_timeframe), str(update_adjust))}")
        if output:
            st.code(output)

    st.caption("分钟级数据已支持 30m / 15m / 5m 更新；当前策略执行与展示仍以现有模型约束为主。")

    preview = load_update_log_preview(limit=20)
    if not preview.empty:
        st.markdown("**最近更新日志**")
        dataframe_stretch(
            format_update_log_for_display(preview),
            hide_index=True,
            column_config=build_update_log_column_config(),
            height=280,
        )

    inventory_preview = load_local_inventory_preview(limit=20)
    if not inventory_preview.empty:
        st.markdown("**本地数据 inventory**")
        dataframe_stretch(
            format_local_inventory_for_display(inventory_preview),
            hide_index=True,
            column_config=build_local_inventory_column_config(),
            height=280,
        )

st.divider()

# ===== Sidebar: 基础参数 =====
st.sidebar.header("运行设置")
st.sidebar.caption("左侧聚焦回测范围与数据源选择，详细规则放在主区域。")
st.sidebar.markdown("**回测范围**")
stock_scope_text = st.sidebar.text_area(
    "股票池",
    value="",
    help="多个代码可用逗号/空格/换行。留空表示全市场。",
    key="stock_scope_text",
)
if "backtest_start_date" not in st.session_state:
    st.session_state["backtest_start_date"] = default_backtest_start
if "backtest_end_date" not in st.session_state:
    st.session_state["backtest_end_date"] = today
st.sidebar.caption("快捷区间")
preset_cols_top = st.sidebar.columns(2)
preset_cols_bottom = st.sidebar.columns(2)
for button_col, (preset_label, years) in zip(
    [*preset_cols_top, *preset_cols_bottom], BACKTEST_RANGE_PRESETS
):
    if button_col.button(
        preset_label, key=f"backtest_preset_{years}y", use_container_width=True
    ):
        st.session_state["backtest_start_date"] = preset_start_date(today, years)
        st.session_state["backtest_end_date"] = today
sidebar_date_cols = st.sidebar.columns(2)
start_date = sidebar_date_cols[0].date_input("回测开始", key="backtest_start_date")
end_date = sidebar_date_cols[1].date_input("回测结束", key="backtest_end_date")
SOURCE_LABEL_TO_TYPE = {
    "本地 Parquet（AKShare 离线）": "local_parquet",
    "Excel/CSV 文件": "file",
    "SQLite 数据库": "sqlite",
}
st.sidebar.markdown("**数据源**")
data_source_label = st.sidebar.selectbox(
    "数据源", options=list(SOURCE_LABEL_TO_TYPE.keys())
)
adjust_label = st.sidebar.selectbox("复权方式", options=["qfq", "hfq"], index=0)
timeframe = st.sidebar.selectbox(
    "周期",
    options=list(TIMEFRAME_OPTIONS),
    index=0,
    key="timeframe",
)
st.sidebar.caption("1d 为常规日线；early_surge_high_base 使用 30m 形态并自动切换到 5m 执行。")

# 数据源输入（仍放侧边栏，保持小白可见）
default_db_path = str(Path.cwd() / "market_data.sqlite")
db_path = default_db_path
table_name = ""
input_file_path = ""
excel_sheet_name = ""
uploaded_market_file = None

with st.sidebar.expander(
    "数据源附加设置",
    expanded=data_source_label != "本地 Parquet（AKShare 离线）",
):
    local_data_root = st.text_input(
        "本地 Parquet 根目录", value="data/market/daily", key="local_data_root"
    )
    if data_source_label == "SQLite 数据库":
        db_path = st.text_input("SQLite 路径", value=default_db_path)
        table_name = st.text_input("表名（可选）", value="", key="sqlite_table_name")
        if st.button("探测数据表", key="sqlite_probe_tables"):
            try:
                st.session_state["sqlite_probe_payload"] = build_sqlite_probe_payload(db_path, table_name)
                if st.session_state["sqlite_probe_payload"]["selected_table"]:
                    st.session_state["sqlite_table_name"] = st.session_state["sqlite_probe_payload"]["selected_table"]
            except Exception as exc:
                st.error(f"数据表探测失败：{exc}")
        sqlite_probe_payload = st.session_state.get("sqlite_probe_payload")
        if isinstance(sqlite_probe_payload, dict) and sqlite_probe_payload.get("candidate_tables"):
            candidate_options = list(sqlite_probe_payload["candidate_tables"])
            default_index = candidate_options.index(st.session_state.get("sqlite_table_name", candidate_options[0])) if st.session_state.get("sqlite_table_name", candidate_options[0]) in candidate_options else 0
            selected_candidate_table = st.selectbox(
                "候选数据表",
                options=candidate_options,
                index=default_index,
                key="sqlite_candidate_table",
            )
            st.session_state["sqlite_table_name"] = selected_candidate_table
            table_name = selected_candidate_table
    elif data_source_label == "Excel/CSV 文件":
        uploaded_market_file = st.file_uploader(
            "上传行情文件", type=["xlsx", "xlsm", "csv"]
        )
        input_file_path = st.text_input("或本地文件路径（可选）", value="")
        excel_sheet_name = st.text_input("工作表（Excel 可选）", value="", key="excel_sheet_name")
        if st.button("预览文件结构", key="file_probe_preview"):
            file_bytes, file_name, file_path = _resolve_uploaded_file_inputs(uploaded_market_file, input_file_path)
            try:
                st.session_state["file_probe_payload"] = build_file_probe_payload(
                    file_path=file_path,
                    file_bytes=file_bytes,
                    file_name=file_name,
                    sheet_name=excel_sheet_name,
                )
                resolved_sheet = st.session_state["file_probe_payload"].get("resolved_sheet")
                if resolved_sheet:
                    st.session_state["excel_sheet_name"] = resolved_sheet
            except Exception as exc:
                st.error(f"文件结构预览失败：{exc}")
        file_probe_payload = st.session_state.get("file_probe_payload")
        if isinstance(file_probe_payload, dict) and file_probe_payload.get("sheet_names"):
            sheet_options = list(file_probe_payload["sheet_names"])
            active_sheet = st.session_state.get("excel_sheet_name", sheet_options[0])
            default_index = sheet_options.index(active_sheet) if active_sheet in sheet_options else 0
            selected_sheet = st.selectbox("工作表候选", options=sheet_options, index=default_index, key="excel_sheet_picker")
            st.session_state["excel_sheet_name"] = selected_sheet
            excel_sheet_name = selected_sheet
    else:
        st.caption("当前使用本地 parquet 数据源，回测将直接读取本地目录。")

submitted = st.sidebar.button("开始回测", type="primary", key="run_backtest")
st.sidebar.caption("结果会在当前页面下方的标签页中展示。")

# ===== 主界面：配置摘要 =====
section_header(
    "回测概览",
    "先确认研究范围与数据来源，再进入规则配置；摘要区域只显示最关键状态。",
)
source_summary_title, source_summary_desc = summarize_data_source(
    data_source_label,
    adjust_label=adjust_label,
    local_data_root=local_data_root,
    db_path=db_path,
    table_name=str(table_name or ""),
    input_file_path=input_file_path,
    uploaded_market_file=uploaded_market_file,
)
pre_backtest_source_summary = build_pre_backtest_source_summary(
    data_source_label=data_source_label,
    timeframe=str(timeframe),
    adjust_label=adjust_label,
    local_data_root=local_data_root,
    db_path=db_path,
    table_name=str(table_name or ""),
    input_file_path=input_file_path,
    uploaded_market_file=uploaded_market_file,
    excel_sheet_name=str(excel_sheet_name or ""),
)
summary_cols = st.columns(3)
current_entry_factor = str(st.session_state.get("entry_factor", "gap"))
current_direction_label = normalize_direction_label(
    current_entry_factor,
    st.session_state.get("direction_label"),
)
summary_cols[0].metric(
    "交易方向",
    "做多"
    if direction_label_to_internal(current_entry_factor, current_direction_label)
    == "up"
    else "做空",
)
summary_cols[1].metric("股票池", summarize_stock_scope(stock_scope_text))
summary_cols[2].metric("回测区间", f"{start_date} → {end_date}")
summary_cols_2 = st.columns(3)
summary_cols_2[0].metric("数据源", source_summary_title, source_summary_desc)
summary_cols_2[1].metric(
    "分批退出",
    "开启" if st.session_state.get("partial_exit_enabled", False) else "关闭",
)
summary_cols_2[2].metric(
    "时间退出",
    "开启" if st.session_state.get("use_time_stop", True) else "关闭",
)

st.markdown("**回测前数据源摘要**")
dataframe_stretch(pre_backtest_source_summary, hide_index=True)

if str(timeframe) in {"30m", "15m", "5m"} and not normalize_stock_codes(stock_scope_text):
    st.warning("当前选择分钟级数据且未指定股票池，读取本地数据时 IO 开销可能较高。")

sqlite_probe_payload = st.session_state.get("sqlite_probe_payload")
file_probe_payload = st.session_state.get("file_probe_payload")
if data_source_label == "SQLite 数据库" and isinstance(sqlite_probe_payload, dict):
    section_header("SQLite 数据表探测", "展示候选表、字段识别结果和前 20 行预览，便于直接选表。")
    if not sqlite_probe_payload.get("overview_df", pd.DataFrame()).empty:
        dataframe_stretch(sqlite_probe_payload["overview_df"], hide_index=True, height=240)
    if sqlite_probe_payload.get("preview_df") is not None and not sqlite_probe_payload["preview_df"].empty:
        st.markdown("**前 20 行数据预览**")
        dataframe_stretch(sqlite_probe_payload["preview_df"], hide_index=True, height=240)
elif data_source_label == "Excel/CSV 文件" and isinstance(file_probe_payload, dict):
    section_header("文件结构预览", "展示文件结构、字段识别结果和前 20 行预览。")
    description = file_probe_payload.get("description", {})
    if description:
        description_df = pd.DataFrame(
            [{
                "文件名": description.get("file_name", ""),
                "sheet": description.get("selected_sheet") or "-",
                "列数": description.get("column_count", 0),
                "列预览": description.get("columns_preview", ""),
                "字段识别结果": description.get("detected_fields", ""),
                "自动识别成功": "是" if description.get("auto_detected") else "否",
            }]
        )
        dataframe_stretch(description_df, hide_index=True)
        if not bool(description.get("auto_detected")):
            st.warning("文件字段未能自动识别，请检查表头或在下方填写字段映射。")
    if file_probe_payload.get("preview_df") is not None and not file_probe_payload["preview_df"].empty:
        st.markdown("**前 20 行数据预览**")
        dataframe_stretch(file_probe_payload["preview_df"], hide_index=True, height=240)

st.divider()
section_header("策略配置", "核心参数默认展开，高级参数保持次要，避免一屏堆满控件。")

# ===== 主界面：规则配置 =====
with st.expander("⚙️ 核心交易规则配置", expanded=True):
    st.caption("优先配置开仓、止损止盈、时间退出与交易成本。")
    core_entry_col, core_exit_col = st.columns(2)
    with core_entry_col:
        st.markdown("**信号与入场**")
        entry_factor = st.selectbox(
            "入场因子",
            options=list(ENTRY_FACTORS),
            format_func=lambda value: str(
                ENTRY_FACTOR_LABELS.get(value, value) or value
            ),
            key="entry_factor",
        )
        reset_inactive_factor_controls(str(entry_factor))
        if entry_factor == "gap":
            st.caption("保留原有跳空参数布局，只显示 gap 相关阈值。")
            entry_top_cols = st.columns([1, 1.6])
            direction_label = render_direction_selectbox(
                str(entry_factor), entry_top_cols[0]
            )
            entry_top_cols[1].selectbox(
                "开仓模式",
                options=list(GAP_ENTRY_MODES),
                format_func=lambda value: (
                    "严格突破前高/前低"
                    if value == "strict_break"
                    else "开盘相对昨收达阈值"
                ),
                key="gap_entry_mode",
            )
            gap_cols = st.columns(2)
            gap_cols[0].number_input(
                "跳空幅度（%）",
                min_value=0.0,
                value=float(factor_control_default("gap_pct")),
                step=0.1,
                key="gap_pct",
            )
            gap_cols[1].number_input(
                "最大高开/低开过滤（%）",
                min_value=0.0,
                value=float(factor_control_default("max_gap_filter_pct")),
                step=0.1,
                key="max_gap_filter_pct",
            )
        elif entry_factor == "trend_breakout":
            st.caption("趋势突破仅保留方向与回看窗口，避免混入 gap 专属控件。")
            direction_label = render_direction_selectbox(str(entry_factor))
            st.number_input(
                "趋势突破回看天数",
                min_value=1,
                value=int(factor_control_default("trend_breakout_lookback")),
                step=1,
                key="trend_breakout_lookback",
            )
        elif entry_factor == "volatility_contraction_breakout":
            st.caption("波动收缩突破仅显示收缩/突破窗口，保持核心区紧凑。")
            direction_label = render_direction_selectbox(str(entry_factor))
            vcb_cols = st.columns(2)
            vcb_cols[0].number_input(
                "收缩区间回看天数",
                min_value=1,
                value=int(factor_control_default("vcb_range_lookback")),
                step=1,
                key="vcb_range_lookback",
            )
            vcb_cols[1].number_input(
                "突破回看天数",
                min_value=1,
                value=int(factor_control_default("vcb_breakout_lookback")),
                step=1,
                key="vcb_breakout_lookback",
            )
        elif entry_factor == "early_surge_high_base":
            st.caption(
                "先在 30m 识别早盘冲高后的高位横盘，再用 5m 突破确认并在下一根 5m 开盘入场。"
            )
            direction_label = render_direction_selectbox(str(entry_factor))
            eshb_cols_1 = st.columns(3)
            eshb_cols_1[0].number_input(
                "早盘观察窗口K数",
                min_value=1,
                value=int(factor_control_default("eshb_open_window_bars")),
                step=1,
                key="eshb_open_window_bars",
            )
            eshb_cols_1[1].number_input(
                "高位横盘最少K数",
                min_value=1,
                value=int(factor_control_default("eshb_base_min_bars")),
                step=1,
                key="eshb_base_min_bars",
            )
            eshb_cols_1[2].number_input(
                "高位横盘最多K数",
                min_value=1,
                value=int(factor_control_default("eshb_base_max_bars")),
                step=1,
                key="eshb_base_max_bars",
            )
            eshb_cols_2 = st.columns(3)
            eshb_cols_2[0].number_input(
                "早盘冲高最小涨幅（%）",
                min_value=0.0,
                value=float(factor_control_default("eshb_surge_min_pct")),
                step=0.1,
                key="eshb_surge_min_pct",
            )
            eshb_cols_2[1].number_input(
                "横盘最大回撤（%）",
                min_value=0.0,
                value=float(factor_control_default("eshb_max_base_pullback_pct")),
                step=0.1,
                key="eshb_max_base_pullback_pct",
            )
            eshb_cols_2[2].number_input(
                "横盘最大振幅（%）",
                min_value=0.0,
                value=float(factor_control_default("eshb_max_base_range_pct")),
                step=0.1,
                key="eshb_max_base_range_pct",
            )
            eshb_cols_3 = st.columns(3)
            eshb_cols_3[0].number_input(
                "锚点跌破次数上限",
                min_value=0,
                value=int(factor_control_default("eshb_max_anchor_breaks")),
                step=1,
                key="eshb_max_anchor_breaks",
            )
            eshb_cols_3[1].number_input(
                "锚点跌破深度上限（%）",
                min_value=0.0,
                value=float(factor_control_default("eshb_max_anchor_break_depth_pct")),
                step=0.1,
                key="eshb_max_anchor_break_depth_pct",
            )
            eshb_cols_3[2].number_input(
                "冲高量能倍数下限",
                min_value=0.0,
                value=float(factor_control_default("eshb_min_open_volume_ratio")),
                step=0.1,
                key="eshb_min_open_volume_ratio",
            )
            eshb_cols_4 = st.columns(2)
            eshb_cols_4[0].number_input(
                "突破量能倍数下限",
                min_value=0.0,
                value=float(factor_control_default("eshb_min_breakout_volume_ratio")),
                step=0.1,
                key="eshb_min_breakout_volume_ratio",
            )
            eshb_cols_4[1].number_input(
                "突破触发缓冲（%）",
                min_value=0.0,
                value=float(factor_control_default("eshb_trigger_buffer_pct")),
                step=0.01,
                key="eshb_trigger_buffer_pct",
            )
        else:
            is_acceleration_mode = entry_factor == "candle_run_acceleration"
            st.caption(
                "连续K线追势基于前序连续阳线/阴线组合；加速模式额外要求实体强度逐步增强。"
                if is_acceleration_mode
                else "连续K线追势基于前序连续阳线/阴线组合，在下一根K线开盘追势。"
            )
            direction_label = render_direction_selectbox(str(entry_factor))
            candle_cols = st.columns(3)
            candle_cols[0].number_input(
                "连续K线根数",
                min_value=2,
                value=int(factor_control_default("candle_run_length")),
                step=1,
                key="candle_run_length",
            )
            candle_cols[1].number_input(
                "单根最小实体幅度（%）",
                min_value=0.0,
                value=float(factor_control_default("candle_run_min_body_pct")),
                step=0.1,
                key="candle_run_min_body_pct",
            )
            candle_cols[2].number_input(
                "组合最小累计涨跌幅（%）",
                min_value=0.0,
                value=float(factor_control_default("candle_run_total_move_pct")),
                step=0.1,
                key="candle_run_total_move_pct",
            )

        gap_entry_mode = str(
            st.session_state.get(
                "gap_entry_mode", factor_control_default("gap_entry_mode")
            )
        )
        gap_pct = float(
            st.session_state.get("gap_pct", factor_control_default("gap_pct"))
        )
        max_gap_filter_pct = float(
            st.session_state.get(
                "max_gap_filter_pct",
                factor_control_default("max_gap_filter_pct"),
            )
        )
        trend_breakout_lookback = int(
            st.session_state.get(
                "trend_breakout_lookback",
                factor_control_default("trend_breakout_lookback"),
            )
        )
        vcb_range_lookback = int(
            st.session_state.get(
                "vcb_range_lookback",
                factor_control_default("vcb_range_lookback"),
            )
        )
        vcb_breakout_lookback = int(
            st.session_state.get(
                "vcb_breakout_lookback",
                factor_control_default("vcb_breakout_lookback"),
            )
        )
        candle_run_length = int(
            st.session_state.get(
                "candle_run_length",
                factor_control_default("candle_run_length"),
            )
        )
        candle_run_min_body_pct = float(
            st.session_state.get(
                "candle_run_min_body_pct",
                factor_control_default("candle_run_min_body_pct"),
            )
        )
        candle_run_total_move_pct = float(
            st.session_state.get(
                "candle_run_total_move_pct",
                factor_control_default("candle_run_total_move_pct"),
            )
        )
        eshb_open_window_bars = int(
            st.session_state.get(
                "eshb_open_window_bars",
                factor_control_default("eshb_open_window_bars"),
            )
        )
        eshb_base_min_bars = int(
            st.session_state.get(
                "eshb_base_min_bars",
                factor_control_default("eshb_base_min_bars"),
            )
        )
        eshb_base_max_bars = int(
            st.session_state.get(
                "eshb_base_max_bars",
                factor_control_default("eshb_base_max_bars"),
            )
        )
        eshb_surge_min_pct = float(
            st.session_state.get(
                "eshb_surge_min_pct",
                factor_control_default("eshb_surge_min_pct"),
            )
        )
        eshb_max_base_pullback_pct = float(
            st.session_state.get(
                "eshb_max_base_pullback_pct",
                factor_control_default("eshb_max_base_pullback_pct"),
            )
        )
        eshb_max_base_range_pct = float(
            st.session_state.get(
                "eshb_max_base_range_pct",
                factor_control_default("eshb_max_base_range_pct"),
            )
        )
        eshb_max_anchor_breaks = int(
            st.session_state.get(
                "eshb_max_anchor_breaks",
                factor_control_default("eshb_max_anchor_breaks"),
            )
        )
        eshb_max_anchor_break_depth_pct = float(
            st.session_state.get(
                "eshb_max_anchor_break_depth_pct",
                factor_control_default("eshb_max_anchor_break_depth_pct"),
            )
        )
        eshb_min_open_volume_ratio = float(
            st.session_state.get(
                "eshb_min_open_volume_ratio",
                factor_control_default("eshb_min_open_volume_ratio"),
            )
        )
        eshb_min_breakout_volume_ratio = float(
            st.session_state.get(
                "eshb_min_breakout_volume_ratio",
                factor_control_default("eshb_min_breakout_volume_ratio"),
            )
        )
        eshb_trigger_buffer_pct = float(
            st.session_state.get(
                "eshb_trigger_buffer_pct",
                factor_control_default("eshb_trigger_buffer_pct"),
            )
        )
        use_ma_filter = st.checkbox("启用快慢线开单过滤", value=False)
        ma_filter_cols = st.columns(2)
        fast_ma_period = ma_filter_cols[0].number_input(
            "快线周期", min_value=1, value=5, step=1, disabled=not use_ma_filter
        )
        slow_ma_period = ma_filter_cols[1].number_input(
            "慢线周期", min_value=1, value=20, step=1, disabled=not use_ma_filter
        )
        enable_atr_filter = st.checkbox("启用 ATR 波动率过滤", value=False)
        atr_filter_cols = st.columns(3)
        atr_filter_period = atr_filter_cols[0].number_input(
            "ATR 过滤周期",
            min_value=1,
            value=int(factor_control_default("atr_filter_period")),
            step=1,
            disabled=not enable_atr_filter,
        )
        min_atr_filter_pct = atr_filter_cols[1].number_input(
            "最小 ATR 波动（%）",
            min_value=0.0,
            value=float(factor_control_default("min_atr_filter_pct")),
            step=0.1,
            disabled=not enable_atr_filter,
        )
        max_atr_filter_pct = atr_filter_cols[2].number_input(
            "最大 ATR 波动（%）",
            min_value=0.0,
            value=float(factor_control_default("max_atr_filter_pct")),
            step=0.1,
            disabled=not enable_atr_filter,
        )

    with core_exit_col:
        st.markdown("**退出与风控**")
        st.caption(
            "整笔退出保留在这里，ATR 跟踪与其他整笔退出并列；固定止盈放在次级入口。"
        )
        use_time_stop = st.checkbox("启用时间退出", value=True, key="use_time_stop")
        time_stop_cols = st.columns(2)
        time_stop_days = time_stop_cols[0].number_input(
            "最多持有天数 N", min_value=1, value=5, step=1, disabled=not use_time_stop
        )
        time_stop_target_pct = time_stop_cols[1].number_input(
            "时间退出收益阈值（%）", value=1.0, step=0.1, disabled=not use_time_stop
        )
        exit_mode_cols = st.columns([1.8, 1])
        time_exit_mode_label = exit_mode_cols[0].selectbox(
            "到期处理",
            options=["按原规则剔除未达条件信号", "第 N 天按收盘价结束交易"],
        )
        stop_loss_pct = exit_mode_cols[1].number_input(
            "全仓止损（%）", min_value=0.0, value=3.0, step=0.1
        )
        drawdown_cols = st.columns(2)
        enable_profit_drawdown_exit = drawdown_cols[0].checkbox(
            "启用盈利回撤止盈（整笔）", value=False
        )
        profit_drawdown_pct = drawdown_cols[1].number_input(
            "盈利回撤（%）",
            min_value=0.0,
            value=40.0,
            step=1.0,
            disabled=not enable_profit_drawdown_exit,
        )
        ma_exit_cols = st.columns(2)
        enable_ma_exit = ma_exit_cols[0].checkbox("启用均线离场（整笔）", value=False)
        exit_ma_period = ma_exit_cols[1].number_input(
            "离场均线周期", min_value=1, value=10, step=1, disabled=not enable_ma_exit
        )
        atr_exit_cols = st.columns(3)
        enable_atr_trailing_exit = atr_exit_cols[0].checkbox(
            "启用 ATR 跟踪止盈（整笔）", value=False
        )
        atr_trailing_period = atr_exit_cols[1].number_input(
            "ATR 跟踪周期",
            min_value=1,
            value=int(factor_control_default("atr_trailing_period")),
            step=1,
            disabled=not enable_atr_trailing_exit,
        )
        atr_trailing_multiplier = atr_exit_cols[2].number_input(
            "ATR 倍数",
            min_value=0.1,
            value=float(factor_control_default("atr_trailing_multiplier")),
            step=0.1,
            disabled=not enable_atr_trailing_exit,
        )
        take_profit_cols = st.columns(2)
        enable_take_profit = take_profit_cols[0].checkbox(
            "启用固定止盈（次级）", value=True
        )
        take_profit_pct = take_profit_cols[1].number_input(
            "固定止盈（%）",
            min_value=0.0,
            value=5.0,
            step=0.1,
            disabled=not enable_take_profit,
        )
        ma_exit_batches = st.number_input(
            "均线离场分批数",
            min_value=2,
            max_value=3,
            value=2,
            step=1,
            disabled=not enable_ma_exit,
        )

    st.markdown("**交易成本与执行**")
    st.caption("保持成本、滑点等执行参数集中展示，便于快速核对。")
    cost_cols = st.columns(4)
    buy_cost_pct = cost_cols[0].number_input(
        "买入成本（%）", min_value=0.0, value=0.03, step=0.01, format="%.4f"
    )
    sell_cost_pct = cost_cols[1].number_input(
        "卖出成本（%）", min_value=0.0, value=0.13, step=0.01, format="%.4f"
    )
    buy_slippage_pct = cost_cols[2].number_input(
        "买入滑点（%）", min_value=0.0, value=0.0, step=0.01, format="%.4f"
    )
    sell_slippage_pct = cost_cols[3].number_input(
        "卖出滑点（%）", min_value=0.0, value=0.0, step=0.01, format="%.4f"
    )

section_header("进阶配置", "把分批止盈和参数扫描放在并排区域，便于联动调参。")
advanced_cols = st.columns([1.05, 1], gap="large")
with advanced_cols[0]:
    with st.expander("🛠️ 分批止盈高级配置", expanded=False):
        st.caption("仅在需要拆分仓位管理时启用，按 priority 从小到大执行。")
        partial_top_cols = st.columns([1, 1])
        partial_exit_enabled = partial_top_cols[0].checkbox(
            "启用分批止盈", value=False, key="partial_exit_enabled"
        )
        partial_exit_count = partial_top_cols[1].number_input(
            "分批数量",
            min_value=2,
            max_value=3,
            value=2,
            step=1,
            disabled=not partial_exit_enabled,
        )
        partial_rule_inputs = []
        batch_tabs = st.tabs(
            [f"第 {i} 批" for i in range(1, int(partial_exit_count) + 1)]
        )
        for i, batch_tab in enumerate(batch_tabs, start=1):
            with batch_tab:
                c1, c2, c3 = st.columns([1, 1.25, 1.1])
                weight_default = (
                    50.0 if int(partial_exit_count) == 2 else [30.0, 30.0, 40.0][i - 1]
                )
                mode_default = (
                    ["fixed_tp", "ma_exit"][i - 1]
                    if int(partial_exit_count) == 2
                    else ["fixed_tp", "fixed_tp", "ma_exit"][i - 1]
                )
                weight_pct = c1.number_input(
                    f"仓位比例（第{i}批）",
                    min_value=0.0,
                    max_value=100.0,
                    value=weight_default,
                    step=1.0,
                    disabled=not partial_exit_enabled,
                    key=f"p_weight_{i}",
                )
                priority = c1.number_input(
                    f"执行优先级（第{i}批）",
                    min_value=1,
                    max_value=10,
                    value=i,
                    step=1,
                    disabled=not partial_exit_enabled,
                    key=f"p_priority_{i}",
                )
                mode = c2.selectbox(
                    f"退出方式（第{i}批）",
                    options=["fixed_tp", "ma_exit", "profit_drawdown", "atr_trailing"],
                    index=[
                        "fixed_tp",
                        "ma_exit",
                        "profit_drawdown",
                        "atr_trailing",
                    ].index(mode_default),
                    format_func=lambda value: str(
                        PARTIAL_EXIT_MODE_LABELS.get(value, value) or value
                    ),
                    disabled=not partial_exit_enabled,
                    key=f"p_mode_{i}",
                )
                tp = c3.number_input(
                    f"目标收益（第{i}批）%",
                    value=5.0,
                    step=0.1,
                    disabled=(not partial_exit_enabled) or mode != "fixed_tp",
                    key=f"p_tp_{i}",
                )
                ma = c3.number_input(
                    f"均线周期（第{i}批）",
                    min_value=1,
                    value=10,
                    step=1,
                    disabled=(not partial_exit_enabled) or mode != "ma_exit",
                    key=f"p_ma_{i}",
                )
                dd = c3.number_input(
                    f"回撤比例（第{i}批）%",
                    min_value=0.0,
                    value=20.0,
                    step=0.1,
                    disabled=(not partial_exit_enabled) or mode != "profit_drawdown",
                    key=f"p_dd_{i}",
                )
                mpa = c2.number_input(
                    f"激活浮盈（第{i}批）%",
                    min_value=0.0,
                    value=5.0,
                    step=0.1,
                    disabled=(not partial_exit_enabled) or mode != "profit_drawdown",
                    key=f"p_mpa_{i}",
                )
                atr_period = c3.number_input(
                    f"ATR 周期（第{i}批）",
                    min_value=1,
                    value=14,
                    step=1,
                    disabled=(not partial_exit_enabled) or mode != "atr_trailing",
                    key=f"p_atr_period_{i}",
                )
                atr_multiplier = c2.number_input(
                    f"ATR 倍数（第{i}批）",
                    min_value=0.1,
                    value=3.0,
                    step=0.1,
                    disabled=(not partial_exit_enabled) or mode != "atr_trailing",
                    key=f"p_atr_multiplier_{i}",
                )
                partial_rule_inputs.append(
                    {
                        "enabled": bool(partial_exit_enabled),
                        "weight_pct": float(weight_pct),
                        "mode": mode,
                        "priority": int(priority),
                        "target_profit_pct": float(tp) if mode == "fixed_tp" else None,
                        "ma_period": int(ma) if mode == "ma_exit" else None,
                        "drawdown_pct": float(dd)
                        if mode == "profit_drawdown"
                        else None,
                        "min_profit_to_activate_drawdown": float(mpa)
                        if mode == "profit_drawdown"
                        else None,
                        "atr_period": int(atr_period)
                        if mode == "atr_trailing"
                        else None,
                        "atr_multiplier": float(atr_multiplier)
                        if mode == "atr_trailing"
                        else None,
                    }
                )

with advanced_cols[1]:
    with st.expander("🔎 参数敏感性扫描", expanded=False):
        st.caption("适合做参数边界探索；建议先用少量组合验证，再扩大扫描范围。")
        st.caption(
            "当分批止盈开启时，可扫描“第X批目标收益/均线周期/回撤比例/激活浮盈”。"
        )
        scan_field_options = build_factor_scan_field_options(str(entry_factor))
        reset_invalid_scan_axis_state(scan_field_options)
        scan_top_cols = st.columns([1.6, 1])
        with scan_top_cols[0]:
            scan_enabled = st.checkbox("启用参数扫描", value=False)
            scan_metric = st.selectbox(
                "扫描排序指标",
                options=list(SCAN_METRICS),
                format_func=lambda value: str(
                    SCAN_METRIC_LABELS.get(value, value) or value
                ),
                disabled=not scan_enabled,
            )
        with scan_top_cols[1]:
            scan_max_combinations = st.number_input(
                "最大组合数",
                min_value=1,
                max_value=100,
                value=25,
                step=1,
                disabled=not scan_enabled,
            )
        scan_axis_cols = st.columns(2)
        with scan_axis_cols[0]:
            scan_axis_1 = st.selectbox(
                "扫描维度 1",
                options=scan_field_options,
                format_func=lambda value: str(
                    "请选择字段"
                    if value == ""
                    else (SCAN_FIELD_LABELS.get(value, value) or value)
                ),
                disabled=not scan_enabled,
                key="scan_axis_1_field",
            )
            scan_axis_1_values = st.text_input(
                "维度 1 取值",
                value="",
                disabled=not scan_enabled,
                help="使用逗号分隔，例如 2,3,4",
                key="scan_axis_1_values",
            )
        with scan_axis_cols[1]:
            scan_axis_2 = st.selectbox(
                "扫描维度 2（可选）",
                options=scan_field_options,
                format_func=lambda value: str(
                    "不启用第二维"
                    if value == ""
                    else (SCAN_FIELD_LABELS.get(value, value) or value)
                ),
                disabled=not scan_enabled,
                key="scan_axis_2_field",
            )
            scan_axis_2_values = st.text_input(
                "维度 2 取值",
                value="",
                disabled=not scan_enabled,
                help="留空表示只扫描一维",
                key="scan_axis_2_values",
            )

# 字段映射
with st.expander("字段映射（可选）", expanded=False):
    st.caption("仅在导入文件列名不标准时填写，常规本地 parquet 回测无需改动。")
    mc1, mc2, mc3 = st.columns(3)
    date_column = mc1.text_input("日期列名", value="")
    stock_code_column = mc1.text_input("股票代码列名", value="")
    open_column = mc1.text_input("开盘价列名", value="")
    high_column = mc2.text_input("最高价列名", value="")
    low_column = mc2.text_input("最低价列名", value="")
    close_column = mc2.text_input("收盘价列名", value="")
    volume_column = mc3.text_input("成交量列名", value="")

if submitted:
    clear_result_state()
    source_type = SOURCE_LABEL_TO_TYPE[data_source_label]
    uploaded_file_bytes, uploaded_file_name, normalized_input_file_path = _resolve_uploaded_file_inputs(uploaded_market_file, input_file_path)

    column_overrides = normalize_column_overrides(
        {
            "date": date_column,
            "stock_code": stock_code_column,
            "open": open_column,
            "high": high_column,
            "low": low_column,
            "close": close_column,
            "volume": volume_column,
        }
    )
    partial_rules = tuple(PartialExitRule(**rule) for rule in partial_rule_inputs)
    scan_axes = tuple(
        axis
        for axis in (
            build_scan_axis(str(scan_axis_1), scan_axis_1_values),
            build_scan_axis(str(scan_axis_2), scan_axis_2_values),
        )
        if axis is not None
    )
    scan_config = ParamScanConfig(
        enabled=bool(scan_enabled),
        axes=scan_axes,
        metric=str(scan_metric),
        max_combinations=int(scan_max_combinations),
    )
    params = AnalysisParams(
        data_source_type=source_type,
        db_path=db_path.strip(),
        table_name=(table_name or "").strip() or None,
        column_overrides=column_overrides,
        excel_sheet_name=(excel_sheet_name or "").strip() or None,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        stock_codes=normalize_stock_codes(stock_scope_text),
        gap_direction=direction_label_to_internal(
            str(entry_factor), str(direction_label)
        ),
        entry_factor=str(entry_factor),
        gap_entry_mode=str(gap_entry_mode),
        gap_pct=float(gap_pct),
        max_gap_filter_pct=float(max_gap_filter_pct),
        trend_breakout_lookback=int(trend_breakout_lookback),
        vcb_range_lookback=int(vcb_range_lookback),
        vcb_breakout_lookback=int(vcb_breakout_lookback),
        candle_run_length=int(candle_run_length),
        candle_run_min_body_pct=float(candle_run_min_body_pct),
        candle_run_total_move_pct=float(candle_run_total_move_pct),
        eshb_open_window_bars=int(eshb_open_window_bars),
        eshb_base_min_bars=int(eshb_base_min_bars),
        eshb_base_max_bars=int(eshb_base_max_bars),
        eshb_surge_min_pct=float(eshb_surge_min_pct),
        eshb_max_base_pullback_pct=float(eshb_max_base_pullback_pct),
        eshb_max_base_range_pct=float(eshb_max_base_range_pct),
        eshb_max_anchor_breaks=int(eshb_max_anchor_breaks),
        eshb_max_anchor_break_depth_pct=float(eshb_max_anchor_break_depth_pct),
        eshb_min_open_volume_ratio=float(eshb_min_open_volume_ratio),
        eshb_min_breakout_volume_ratio=float(eshb_min_breakout_volume_ratio),
        eshb_trigger_buffer_pct=float(eshb_trigger_buffer_pct),
        use_ma_filter=bool(use_ma_filter),
        fast_ma_period=int(fast_ma_period),
        slow_ma_period=int(slow_ma_period),
        enable_atr_filter=bool(enable_atr_filter),
        atr_filter_period=int(atr_filter_period),
        min_atr_filter_pct=float(min_atr_filter_pct),
        max_atr_filter_pct=float(max_atr_filter_pct),
        time_stop_days=int(time_stop_days),
        time_stop_target_pct=float(time_stop_target_pct),
        stop_loss_pct=float(stop_loss_pct),
        take_profit_pct=float(take_profit_pct),
        enable_take_profit=bool(enable_take_profit),
        enable_profit_drawdown_exit=bool(enable_profit_drawdown_exit),
        profit_drawdown_pct=float(profit_drawdown_pct),
        enable_ma_exit=bool(enable_ma_exit),
        exit_ma_period=int(exit_ma_period),
        enable_atr_trailing_exit=bool(enable_atr_trailing_exit),
        atr_trailing_period=int(atr_trailing_period),
        atr_trailing_multiplier=float(atr_trailing_multiplier),
        ma_exit_batches=int(ma_exit_batches),
        partial_exit_enabled=bool(partial_exit_enabled),
        partial_exit_count=int(partial_exit_count),
        partial_exit_rules=partial_rules,
        buy_cost_pct=float(buy_cost_pct),
        sell_cost_pct=float(sell_cost_pct),
        buy_slippage_pct=float(buy_slippage_pct),
        sell_slippage_pct=float(sell_slippage_pct),
        time_exit_mode="strict"
        if time_exit_mode_label == "按原规则剔除未达条件信号"
        else "force_close",
        timeframe=str(timeframe),
        local_data_root=str(local_data_root),
        adjust=str(adjust_label),
        scan_config=scan_config,
    )

    errors, warnings = validate_params(params)
    for warning in warnings:
        st.warning(warning)

    if params.data_source_type == "file":
        input_file_path = normalized_input_file_path or ""
        if uploaded_file_bytes is not None and input_file_path:
            st.warning("同时提供了上传文件和本地文件路径，当前会优先使用上传文件。")
        if uploaded_file_bytes is None and not input_file_path:
            errors.append("请选择 Excel/CSV 文件，或者填写本地文件路径。")
        if (
            uploaded_file_bytes is None
            and input_file_path
            and not Path(input_file_path).exists()
        ):
            errors.append(f"找不到文件：{input_file_path}")

    if errors:
        st.error("参数校验失败")
        for error in errors:
            st.error(error)
    else:
        try:
            with st.spinner("正在运行回测，请稍候..."):
                all_data = load_market_data_cached(
                    source_type=params.data_source_type,
                    start_date=params.start_date,
                    end_date=params.end_date,
                    stock_codes=tuple(params.stock_codes),
                    table_name=params.table_name,
                    column_override_items=tuple(sorted(params.column_overrides.items())),
                    lookback_days=params.required_lookback_days,
                    lookahead_days=params.required_lookahead_days,
                    db_path=params.db_path,
                    file_path=(input_file_path or None),
                    file_bytes=uploaded_file_bytes,
                    file_name=uploaded_file_name,
                    sheet_name=params.excel_sheet_name,
                    local_data_root=params.local_data_root,
                    adjust=params.adjust,
                    timeframe=params.timeframe,
                )
                scan_df = pd.DataFrame()
                best_scan_overrides: dict[str, int | float] = {}
                if params.scan_config.enabled:
                    (
                        scan_df,
                        detail_df,
                        daily_df,
                        equity_df,
                        stats,
                        best_scan_overrides,
                    ) = run_parameter_scan(all_data, params)
                else:
                    detail_df, daily_df, equity_df, stats = analyze_all_stocks(
                        all_data, params
                    )
                excel_bytes = export_to_excel_bytes(
                    detail_df, daily_df, equity_df, scan_df=scan_df
                )
            st.success("回测完成")
            st.session_state["detail_df"] = detail_df
            st.session_state["daily_df"] = daily_df
            st.session_state["equity_df"] = equity_df
            st.session_state["stats"] = stats
            st.session_state["scan_df"] = scan_df
            st.session_state["scan_metric"] = params.scan_config.metric
            st.session_state["scan_axis_fields"] = [
                axis.field_name for axis in params.scan_config.axes
            ]
            st.session_state["best_scan_overrides"] = best_scan_overrides
            st.session_state["excel_bytes"] = excel_bytes
            st.session_state["download_name"] = build_download_name(
                params.start_date, params.end_date
            )
        except Exception as exc:
            st.error(f"回测失败：{exc}")

detail_df = st.session_state.get("detail_df", pd.DataFrame())
daily_df = st.session_state.get("daily_df", pd.DataFrame())
equity_df = st.session_state.get("equity_df", pd.DataFrame())
stats = st.session_state.get("stats", {})
scan_df = st.session_state.get("scan_df", pd.DataFrame())
scan_metric = str(st.session_state.get("scan_metric", "total_return_pct"))
scan_axis_fields = list(st.session_state.get("scan_axis_fields", []))
best_scan_overrides = dict(st.session_state.get("best_scan_overrides", {}))

if isinstance(detail_df, pd.DataFrame) and "excel_bytes" in st.session_state:
    tab_names = ["📊 绩效总览", "📈 资金曲线", "📝 交易明细"]
    if isinstance(scan_df, pd.DataFrame) and not scan_df.empty:
        tab_names.append("🔎 参数扫描")
    tabs = st.tabs(tab_names)
    tab_summary, tab_curve, tab_details = tabs[:3]
    with tab_summary:
        section_header("绩效总览", "先看关键结果，再决定是否继续展开明细或参数扫描。")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("总收益率", f"{float(stats.get('total_return_pct', 0.0)):.2f}%")
        c2.metric("胜率", f"{float(stats.get('strategy_win_rate_pct', 0.0)):.2f}%")
        c3.metric("最大回撤", f"{float(stats.get('max_drawdown_pct', 0.0)):.2f}%")
        c4.metric("交易笔数", f"{int(stats.get('executed_trades', len(detail_df)))}")
        skip_metric_cols = st.columns(2)
        skip_metric_cols[0].metric(
            "开仓未成交跳过", f"{int(stats.get('skipped_entry_not_filled', 0))}"
        )
        skip_metric_cols[1].metric(
            "持仓重叠跳过", f"{int(stats.get('skipped_overlapping_position', 0))}"
        )
        if best_scan_overrides:
            summary_text = ", ".join(
                f"{SCAN_FIELD_LABELS.get(field_name, field_name) or field_name}={value}"
                for field_name, value in best_scan_overrides.items()
            )
            st.caption(f"最佳参数组合：{summary_text}")
        st.caption("按开仓日汇总保留在同一标签页，便于从总览直接下钻到日度表现。")
        if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
            st.markdown("**按开仓日汇总**")
            dataframe_stretch(
                format_summary_for_display(daily_df),
                hide_index=True,
                column_config=build_summary_column_config(),
                height=260,
            )
        st.download_button(
            "导出 Excel",
            data=st.session_state["excel_bytes"],
            file_name=st.session_state["download_name"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with tab_curve:
        if isinstance(equity_df, pd.DataFrame) and not equity_df.empty:
            section_header("净值与回撤", "默认展示净值曲线，并保留表格视图便于核对。")
            chart_df = equity_df.copy()
            chart_df["date"] = pd.to_datetime(chart_df["date"])
            fig = px.line(chart_df, x="date", y="net_value")
            fig.update_layout(margin=dict(l=0, r=0, t=16, b=0))
            st.plotly_chart(fig, use_container_width=True)
            st.caption("下表保留原始净值序列，适合与图形交叉核对。")
            dataframe_stretch(
                format_equity_for_display(equity_df),
                hide_index=True,
                column_config=build_equity_column_config(),
                height=260,
            )
        else:
            st.info("暂无资金曲线数据")

    with tab_details:
        if isinstance(detail_df, pd.DataFrame) and not detail_df.empty:
            section_header("交易明细", "表头已转中文，优先展示交易关键信息与成交明细。")
            detail_meta_cols = st.columns(3)
            detail_meta_cols[0].metric("交易笔数", f"{len(detail_df)}")
            detail_meta_cols[1].metric(
                "平均持有天数", f"{float(stats.get('avg_holding_days', 0.0)):.2f}"
            )
            detail_meta_cols[2].metric(
                "净收益中位数", f"{float(stats.get('median_net_return_pct', 0.0)):.2f}%"
            )
            dataframe_stretch(
                format_detail_for_display(detail_df),
                hide_index=True,
                column_config=build_detail_column_config(),
                height=420,
            )
            csv_bytes = detail_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "导出 CSV",
                data=csv_bytes,
                file_name="trade_details.csv",
                mime="text/csv",
            )
        else:
            st.info("暂无交易明细")

    if len(tabs) == 4:
        with tabs[3]:
            section_header(
                "参数扫描结果", "先看排名表，再看热力图/折线图判断参数敏感性。"
            )
            scan_summary_cols = st.columns(3)
            scan_summary_cols[0].metric("组合数", f"{len(scan_df)}")
            scan_summary_cols[1].metric(
                "排序指标",
                str(SCAN_METRIC_LABELS.get(scan_metric, scan_metric) or scan_metric),
            )
            scan_summary_cols[2].metric(
                "扫描维度",
                " / ".join(
                    str(SCAN_FIELD_LABELS.get(field_name, field_name) or field_name)
                    for field_name in scan_axis_fields
                )
                or "未设置",
            )
            if best_scan_overrides:
                scan_best_text = "，".join(
                    f"{SCAN_FIELD_LABELS.get(field_name, field_name) or field_name}={value}"
                    for field_name, value in best_scan_overrides.items()
                )
                st.caption(f"本次最优组合：{scan_best_text}")
            dataframe_stretch(
                format_scan_for_display(scan_df),
                hide_index=True,
                column_config=build_scan_column_config(),
                height=360,
            )
            if len(scan_axis_fields) == 2:
                pivot = scan_df.pivot(
                    index=scan_axis_fields[1],
                    columns=scan_axis_fields[0],
                    values=scan_metric,
                )
                fig = px.imshow(
                    pivot,
                    text_auto=True if pivot.size <= 36 else False,
                    aspect="auto",
                    labels={
                        "x": SCAN_FIELD_LABELS.get(
                            scan_axis_fields[0], scan_axis_fields[0]
                        )
                        or scan_axis_fields[0],
                        "y": SCAN_FIELD_LABELS.get(
                            scan_axis_fields[1], scan_axis_fields[1]
                        )
                        or scan_axis_fields[1],
                        "color": SCAN_METRIC_LABELS.get(scan_metric, scan_metric)
                        or scan_metric,
                    },
                )
                fig.update_xaxes(side="top")
                st.plotly_chart(fig, use_container_width=True)
            elif len(scan_axis_fields) == 1:
                chart_df = scan_df.sort_values(scan_axis_fields[0]).copy()
                fig = px.line(
                    chart_df,
                    x=scan_axis_fields[0],
                    y=scan_metric,
                    markers=True,
                    labels={
                        scan_axis_fields[0]: SCAN_FIELD_LABELS.get(
                            scan_axis_fields[0], scan_axis_fields[0]
                        )
                        or scan_axis_fields[0],
                        scan_metric: SCAN_METRIC_LABELS.get(scan_metric, scan_metric)
                        or scan_metric,
                    },
                )
                st.plotly_chart(fig, use_container_width=True)
else:
    st.divider()
    section_header(
        "结果区域", "运行回测后，这里会保留总览、曲线、明细和参数扫描标签页。"
    )
    st.info("请先在左侧确认回测范围与数据源，再点击“开始回测”。")
