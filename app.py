from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO
import json
from pathlib import Path
import sqlite3
from typing import Any, cast

import pandas as pd
import plotly.express as px
import streamlit as st

from analyzer import run_backtest
from config.strategy_capability import (
    get_strategy_capability_summary,
    get_supported_strategy_timeframes,
)
from data_loader import (
    build_export_dir_hint,
    describe_file_source,
    get_supported_update_sources,
    describe_tables,
    list_candidate_tables,
    list_file_sheets,
    load_market_data,
    normalize_tdx_tqcenter_path,
    probe_local_indicator_candidates,
    quote_ident,
    read_data_source_config,
    resolve_offline_update_sources,
    run_local_data_update,
    run_local_indicator_import,
)
from data.services.indicator_catalog_service import (
    load_registry_manifest,
    summarize_indicator_availability,
    summarize_indicator_quality,
)
from data.services.local_inventory_service import load_inventory
from exporter import export_to_excel_bytes
from models import (
    AnalysisParams,
    ENTRY_FACTORS,
    FACTOR_SCAN_ELIGIBLE_FIELDS,
    GAP_ENTRY_MODES,
    ImportedIndicatorRule,
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
from pages.backtest import render_backtest_page_header, render_backtest_sidebar_intro
from pages.data_prep import render_data_prep_page_header, render_data_prep_sidebar_intro
from ui.components.advanced_panels import (
    ADVANCED_SECTION_CAPTION,
    ADVANCED_SECTION_TITLE,
)
from ui.components.results_view import (
    build_result_tab_names,
    render_results_empty_state,
    render_trade_explanations,
)
from ui.components.risk_form import RISK_SECTION_CAPTION, RISK_SECTION_TITLE
from ui.components.strategy_form import (
    STRATEGY_SECTION_CAPTION,
    STRATEGY_SECTION_TITLE,
)
from ui.components.summary_cards import render_backtest_summary_cards


st.set_page_config(layout="wide", page_title="Gap_test 回测系统")

RESULT_STATE_KEYS = [
    "detail_df",
    "daily_df",
    "equity_df",
    "trade_behavior_df",
    "drawdown_episodes_df",
    "drawdown_contributors_df",
    "anomaly_queue_df",
    "stats",
    "scan_df",
    "scan_metric",
    "scan_axis_fields",
    "best_scan_overrides",
    "excel_bytes",
    "download_name",
    "per_stock_stats_df",
    "batch_backtest_mode",
    "result_params_snapshot",
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
    "board_ma_value",
    "imported_indicator_exit_value",
    "partial_indicator_trigger_value",
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
TRADE_BEHAVIOR_PERCENT_COLUMNS = [
    "win_rate_pct",
    "avg_net_return_pct",
    "median_net_return_pct",
    "avg_mfe_pct",
    "avg_mae_pct",
    "avg_give_back_pct",
    "avg_mfe_capture_pct",
    "trigger_fill_share_pct",
    "multi_fill_trade_share_pct",
]
TRADE_BEHAVIOR_NUMBER_COLUMNS = ["executed_trades", "avg_profit_drawdown_ratio"]
DD_EPISODE_PERCENT_COLUMNS = ["peak_to_trough_pct", "worst_trade_return_pct"]
DD_EPISODE_COUNT_COLUMNS = ["episode_no", "underwater_bars", "trade_count"]
DD_CONTRIBUTOR_PERCENT_COLUMNS = [
    "avg_net_return_pct",
    "total_net_return_pct",
    "avg_mae_pct",
    "avg_mfe_pct",
]
DD_CONTRIBUTOR_COUNT_COLUMNS = ["trade_count"]
ANOMALY_PERCENT_COLUMNS = [
    "activation_threshold_pct",
    "threshold_excess_pct",
    "holding_anchor_mfe_pct",
    "holding_anchor_mae_pct",
    "give_back_pct",
    "net_return_pct",
]
ANOMALY_NUMBER_COLUMNS = ["severity_score", "trade_no", "holding_days"]
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
    "min_profit_to_activate_profit_drawdown_pct": "盈利回撤激活浮盈",
    "exit_ma_period": "离场均线周期",
    "atr_trailing_period": "ATR跟踪周期",
    "atr_trailing_multiplier": "ATR跟踪倍数",
    "min_profit_to_activate_atr_trailing_pct": "ATR跟踪激活浮盈",
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
    "board_ma_value": "板块均线值",
    "imported_indicator_exit_value": "导入指标离场值",
    "partial_indicator_rule_label": "分批指标止盈说明",
    "partial_indicator_trigger_value": "分批指标触发值",
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
    "indicator_threshold": "导入指标阈值止盈",
}
BACKTEST_RANGE_PRESETS = (
    ("10年至今", 10),
    ("7年至今", 7),
    ("5年至今", 5),
    ("3年至今", 3),
)
MAJOR_INDEX_PRESETS = (
    ("沪深300", "000300.SH", "hs300"),
    ("创业板指", "399006.SZ", "cyb"),
    ("中证1000", "000852.SH", "zz1000"),
    ("中证500", "000905.SH", "zz500"),
    ("上证50", "000016.SH", "sz50"),
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
    "board_ma_filter_line": "20",
    "board_ma_filter_operator": ">=",
    "board_ma_filter_threshold": 50.0,
    "imported_indicator_filter_operator": ">=",
    "imported_indicator_filter_threshold": 0.0,
    "imported_indicator_exit_operator": "<=",
    "imported_indicator_exit_threshold": 0.0,
    "board_ma_exit_line": "20",
    "board_ma_exit_operator": "<=",
    "board_ma_exit_threshold": 40.0,
    "min_profit_to_activate_profit_drawdown_pct": 5.0,
    "atr_trailing_period": 14,
    "atr_trailing_multiplier": 3.0,
    "min_profit_to_activate_atr_trailing_pct": 5.0,
}
BOARD_MA_LINE_LABELS = {"20": "均20占比", "50": "均50占比"}
BOARD_MA_OPERATOR_LABELS = {">=": "高于/等于阈值", "<=": "低于/等于阈值"}
IMPORTED_INDICATOR_OPERATOR_LABELS = BOARD_MA_OPERATOR_LABELS.copy()
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
TRADE_BEHAVIOR_COLUMN_LABELS = {
    "executed_trades": "交易笔数",
    "win_rate_pct": "胜率",
    "avg_net_return_pct": "平均净收益率",
    "median_net_return_pct": "净收益率中位数",
    "avg_mfe_pct": "平均最大有利波动",
    "avg_mae_pct": "平均最大不利波动",
    "avg_give_back_pct": "平均利润回吐",
    "avg_mfe_capture_pct": "平均 MFE 兑现率",
    "trigger_fill_share_pct": "触发成交占比",
    "multi_fill_trade_share_pct": "多批成交占比",
    "avg_profit_drawdown_ratio": "平均利润回撤比",
}
DD_EPISODE_COLUMN_LABELS = {
    "episode_no": "回撤段编号",
    "drawdown_start_date": "回撤开始",
    "trough_date": "回撤谷值",
    "recovery_date": "恢复日期",
    "peak_to_trough_pct": "峰谷回撤",
    "underwater_bars": "水下周期数",
    "trade_count": "涉及交易数",
    "worst_trade_return_pct": "最差单笔收益",
    "dominant_entry_reason": "主导开仓原因",
    "recovered_flag": "是否恢复",
}
DD_CONTRIBUTOR_COLUMN_LABELS = {
    "entry_reason": "开仓原因",
    "trade_count": "交易数",
    "avg_net_return_pct": "平均净收益率",
    "total_net_return_pct": "累计净收益率",
    "avg_mae_pct": "平均最大不利波动",
    "avg_mfe_pct": "平均最大有利波动",
}
ANOMALY_COLUMN_LABELS = {
    "anomaly_type": "异常类型",
    "severity_score": "严重度",
    "trade_no": "交易编号",
    "date": "信号日期",
    "stock_code": "股票代码",
    "holding_days": "持有天数",
    "activation_threshold_pct": "激发阈值",
    "threshold_excess_pct": "超阈值幅度",
    "holding_anchor_mfe_pct": "日均最大浮盈",
    "holding_anchor_mae_pct": "日均最大不利波动",
    "give_back_pct": "利润回吐",
    "net_return_pct": "净收益率",
    "entry_reason": "开仓原因",
    "exit_reason": "离场原因",
    "anomaly_note": "诊断说明",
}
UPDATE_LOG_COLUMN_LABELS = {
    "symbol": "股票代码",
    "timeframe": "周期",
    "provider": "更新源",
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
UPDATE_SOURCE_LABELS = {
    "akshare": "AKShare 在线",
    "tdx": "通达信 TDX",
}


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


def ytd_start_date(today: date) -> date:
    return today.replace(month=1, day=1)


@st.cache_data(show_spinner=False)
def load_symbol_name_map() -> dict[str, str]:
    symbol_file = Path("data/market/metadata/symbols.parquet")
    if not symbol_file.exists():
        return {}
    try:
        symbol_df = pd.read_parquet(symbol_file)
    except Exception:
        return {}

    symbol_col = next(
        (
            column
            for column in ("symbol", "stock_code", "code")
            if column in symbol_df.columns
        ),
        None,
    )
    name_col = next(
        (
            column
            for column in ("name", "symbol_name", "display_name")
            if column in symbol_df.columns
        ),
        None,
    )
    if symbol_col is None or name_col is None:
        return {}

    symbol_name_map: dict[str, str] = {}
    for row in symbol_df.to_dict("records"):
        symbol_text = str(row.get(symbol_col, "")).strip()
        name_text = str(row.get(name_col, "")).strip()
        if symbol_text and name_text:
            symbol_name_map[symbol_text.upper()] = name_text
    return symbol_name_map


def build_display_symbol_label(
    symbol: Any,
    symbol_name_map: dict[str, str] | None = None,
) -> str:
    symbol_text = str(symbol).strip() if pd.notna(symbol) else ""
    if not symbol_text:
        return ""
    lookup_map = symbol_name_map or {}
    symbol_name = str(lookup_map.get(symbol_text.upper(), "") or "").strip()
    if not symbol_name:
        return symbol_text
    return f"{symbol_name}（{symbol_text}）"


def apply_symbol_text_preset(state_key: str, symbol_code: str) -> None:
    st.session_state[state_key] = symbol_code


def merge_symbol_text_preset(state_key: str, symbol_code: str) -> None:
    existing_codes = normalize_stock_codes(str(st.session_state.get(state_key, "")))
    preset_codes = normalize_stock_codes(symbol_code)

    merged_codes: list[str] = []
    seen: set[str] = set()
    for code in [*existing_codes, *preset_codes]:
        if code in seen:
            continue
        seen.add(code)
        merged_codes.append(code)

    st.session_state[state_key] = ",".join(merged_codes)


def apply_stock_scope_preset(symbol_code: str) -> None:
    merge_symbol_text_preset("stock_scope_text", symbol_code)


def apply_offline_update_scope_preset(symbol_code: str) -> None:
    merge_symbol_text_preset("offline_update_symbols", symbol_code)


def pick_tdx_tqcenter_path(current_path: str) -> tuple[str, str | None]:
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        try:
            root.attributes("-topmost", True)
        except Exception:
            pass

        initial_dir = current_path.strip() or str(Path.home())
        selected_dir = filedialog.askdirectory(
            title="选择通达信安装目录",
            initialdir=initial_dir,
            mustexist=True,
        )
        root.destroy()
        if selected_dir:
            return str(Path(selected_dir)), None
        return current_path, None
    except Exception as exc:  # noqa: BLE001
        return (
            current_path,
            f"无法打开目录选择窗口：{exc}。可继续使用当前路径或稍后重试。",
        )


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


@st.cache_data(show_spinner=False)
def load_market_data_cached_v2(
    *,
    source_type: str,
    start_date: str,
    end_date: str,
    stock_codes: tuple[str, ...] | None = None,
    table_name: str | None = None,
    column_override_items: tuple[tuple[str, str], ...] = (),
    lookback_days: int = 0,
    lookahead_days: int = 0,
    db_path: str | None = None,
    file_path: str | None = None,
    file_bytes: bytes | None = None,
    file_name: str | None = None,
    sheet_name: str | None = None,
    local_data_root: str = "data/market/daily",
    adjust: str = "qfq",
    timeframe: str = "1d",
    indicator_keys: tuple[str, ...] = (),
    indicator_root: str = "data/indicators",
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
        indicator_keys=indicator_keys,
        indicator_root=indicator_root,
    )


def clear_result_state() -> None:
    for key in RESULT_STATE_KEYS:
        st.session_state.pop(key, None)


def build_result_params_snapshot(params: AnalysisParams) -> dict[str, object]:
    return {
        "entry_factor": str(params.entry_factor),
        "use_ma_filter": bool(params.use_ma_filter),
        "fast_ma_period": int(params.fast_ma_period),
        "slow_ma_period": int(params.slow_ma_period),
        "enable_atr_filter": bool(params.enable_atr_filter),
        "min_atr_filter_pct": float(params.min_atr_filter_pct),
        "max_atr_filter_pct": float(params.max_atr_filter_pct),
        "enable_board_ma_filter": bool(params.enable_board_ma_filter),
        "board_ma_filter_line": str(params.board_ma_filter_line),
        "board_ma_filter_operator": str(params.board_ma_filter_operator),
        "board_ma_filter_threshold": float(params.board_ma_filter_threshold),
        "imported_filter_count": int(len(params.effective_imported_indicator_filters)),
    }


def store_backtest_result_state(
    *,
    detail_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    equity_df: pd.DataFrame,
    trade_behavior_df: pd.DataFrame,
    drawdown_episodes_df: pd.DataFrame,
    drawdown_contributors_df: pd.DataFrame,
    anomaly_queue_df: pd.DataFrame,
    stats: dict[str, Any],
    scan_df: pd.DataFrame,
    scan_metric: str,
    scan_axis_fields: list[str],
    best_scan_overrides: dict[str, Any],
    excel_bytes: bytes,
    download_name: str,
    per_stock_stats_df: pd.DataFrame,
    batch_backtest_mode: str,
    result_params_snapshot: dict[str, object],
) -> None:
    st.session_state["detail_df"] = detail_df
    st.session_state["daily_df"] = daily_df
    st.session_state["equity_df"] = equity_df
    st.session_state["trade_behavior_df"] = trade_behavior_df
    st.session_state["drawdown_episodes_df"] = drawdown_episodes_df
    st.session_state["drawdown_contributors_df"] = drawdown_contributors_df
    st.session_state["anomaly_queue_df"] = anomaly_queue_df
    st.session_state["stats"] = stats
    st.session_state["scan_df"] = scan_df
    st.session_state["scan_metric"] = scan_metric
    st.session_state["scan_axis_fields"] = scan_axis_fields
    st.session_state["best_scan_overrides"] = best_scan_overrides
    st.session_state["excel_bytes"] = excel_bytes
    st.session_state["download_name"] = download_name
    st.session_state["per_stock_stats_df"] = per_stock_stats_df
    st.session_state["batch_backtest_mode"] = batch_backtest_mode
    st.session_state["result_params_snapshot"] = result_params_snapshot


def load_backtest_result_state() -> dict[str, Any]:
    return {
        "detail_df": st.session_state.get("detail_df", pd.DataFrame()),
        "signal_trace_df": st.session_state.get("signal_trace_df", pd.DataFrame()),
        "rejected_signal_df": st.session_state.get(
            "rejected_signal_df", pd.DataFrame()
        ),
        "daily_df": st.session_state.get("daily_df", pd.DataFrame()),
        "equity_df": st.session_state.get("equity_df", pd.DataFrame()),
        "trade_behavior_df": st.session_state.get("trade_behavior_df", pd.DataFrame()),
        "drawdown_episodes_df": st.session_state.get(
            "drawdown_episodes_df", pd.DataFrame()
        ),
        "drawdown_contributors_df": st.session_state.get(
            "drawdown_contributors_df", pd.DataFrame()
        ),
        "anomaly_queue_df": st.session_state.get("anomaly_queue_df", pd.DataFrame()),
        "stats": st.session_state.get("stats", {}),
        "scan_df": st.session_state.get("scan_df", pd.DataFrame()),
        "scan_metric": str(st.session_state.get("scan_metric", "total_return_pct")),
        "scan_axis_fields": list(st.session_state.get("scan_axis_fields", [])),
        "best_scan_overrides": dict(st.session_state.get("best_scan_overrides", {})),
        "per_stock_stats_df": st.session_state.get(
            "per_stock_stats_df", pd.DataFrame()
        ),
        "batch_backtest_mode": str(
            st.session_state.get("batch_backtest_mode", "combined")
        ),
        "result_params_snapshot": cast(
            dict[str, object], st.session_state.get("result_params_snapshot", {})
        ),
    }


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


def enforce_entry_factor_timeframe(active_factor: str) -> tuple[str, ...]:
    allowed_timeframes = get_supported_strategy_timeframes(active_factor)
    current_timeframe = str(st.session_state.get("timeframe", allowed_timeframes[0]))
    if current_timeframe not in allowed_timeframes:
        st.session_state["timeframe"] = allowed_timeframes[0]
    return allowed_timeframes


def summarize_strategy_choice(entry_factor: str, direction_label: str) -> str:
    strategy_name = ENTRY_FACTOR_LABELS.get(entry_factor, entry_factor)
    direction = direction_label_to_internal(entry_factor, direction_label)
    direction_text = "做多" if direction == "up" else "做空"
    if entry_factor == "early_surge_high_base":
        direction_text = "30m 形态 / 5m 执行"
    return f"{strategy_name} / {direction_text}"


def request_page_change(target_page: str) -> None:
    st.session_state["pending_page_mode"] = target_page


def apply_pending_page_change() -> None:
    pending_page_mode = str(st.session_state.pop("pending_page_mode", "") or "")
    if pending_page_mode:
        st.session_state["page_mode"] = pending_page_mode


def clear_update_log() -> bool:
    log_file = Path("data/market/metadata/update_log.parquet")
    if not log_file.exists():
        return False
    log_file.unlink()
    return True


def summarize_local_inventory(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return pd.DataFrame(columns=["周期", "标的数", "总行数", "最近更新"])

    summary_df = preview_df.copy()
    summary_df["updated_at"] = pd.to_datetime(
        summary_df.get("updated_at"), errors="coerce"
    )
    grouped = (
        summary_df.groupby("timeframe", dropna=False)
        .agg(
            symbol_count=("symbol", "nunique"),
            total_rows=("row_count", "sum"),
            latest_update=("updated_at", "max"),
        )
        .reset_index()
    )
    grouped["latest_update"] = grouped["latest_update"].map(
        format_timestamp_for_display
    )
    return grouped.rename(
        columns={
            "timeframe": "周期",
            "symbol_count": "标的数",
            "total_rows": "总行数",
            "latest_update": "最近更新",
        }
    )


def summarize_signal_funnel(
    stats: dict[str, Any], signal_trace_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    trace_df = (
        signal_trace_df if isinstance(signal_trace_df, pd.DataFrame) else pd.DataFrame()
    )
    if not trace_df.empty:
        setup_count = int(trace_df["setup_pass"].fillna(False).sum())
        trigger_count = int(trace_df["trigger_pass"].fillna(False).sum())
        filter_pass_count = int(
            (
                trace_df["trigger_pass"].fillna(False)
                & trace_df["filter_pass"].fillna(False)
            ).sum()
        )
        rejected_count = int(
            (
                trace_df["trigger_pass"].fillna(False)
                & (~trace_df["filter_pass"].fillna(False))
            ).sum()
        )
        execution_skip_count = int(
            (
                trace_df["trigger_pass"].fillna(False)
                & trace_df["filter_pass"].fillna(False)
                & trace_df["execution_skip_reason"].fillna("").astype(str).ne("")
            ).sum()
        )
    else:
        setup_count = int(stats.get("core_signal_count", stats.get("signal_count", 0)))
        trigger_count = int(
            stats.get("core_signal_count", stats.get("signal_count", 0))
        )
        filter_pass_count = int(stats.get("signal_count", 0))
        rejected_count = int(stats.get("rejected_signal_count", 0))
        execution_skip_count = (
            int(stats.get("skipped_entry_not_filled", 0))
            + int(stats.get("skipped_locked_bar_unfillable", 0))
            + int(stats.get("skipped_insufficient_future", 0))
            + int(stats.get("skipped_unclosed_trade", 0))
            + int(stats.get("skipped_no_exit", 0))
        )
    funnel_rows = [
        {"阶段": "形态成立", "数量": setup_count},
        {"阶段": "真实触发", "数量": trigger_count},
        {"阶段": "过滤后放行", "数量": filter_pass_count},
        {"阶段": "过滤链拦截", "数量": rejected_count},
        {"阶段": "成交模拟失败", "数量": execution_skip_count},
        {"阶段": "形成平仓交易", "数量": int(stats.get("closed_trade_candidates", 0))},
        {"阶段": "实际执行交易", "数量": int(stats.get("executed_trades", 0))},
        {"阶段": "未成交跳过", "数量": int(stats.get("skipped_entry_not_filled", 0))},
        {
            "阶段": "一字板/无量跳过",
            "数量": int(stats.get("skipped_locked_bar_unfillable", 0)),
        },
        {
            "阶段": "持仓重叠跳过",
            "数量": int(stats.get("skipped_overlapping_position", 0)),
        },
    ]
    return pd.DataFrame(funnel_rows)


def summarize_filter_stack(
    *,
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
) -> list[str]:
    filters = [ENTRY_FACTOR_LABELS.get(entry_factor, entry_factor)]
    if use_ma_filter:
        filters.append(f"快慢线过滤({fast_ma_period}/{slow_ma_period})")
    if enable_atr_filter:
        filters.append(
            f"ATR过滤({format_number(min_atr_filter_pct)}%~{format_number(max_atr_filter_pct)}%)"
        )
    if enable_board_ma_filter:
        filters.append(
            f"板块均线过滤({board_ma_filter_line} / {board_ma_filter_operator} {format_number(board_ma_filter_threshold)})"
        )
    if imported_filter_count > 0:
        filters.append(f"导入指标过滤 x{imported_filter_count}")
    return filters


def summarize_trade_decision_chain(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()

    summary_rows: list[dict[str, str]] = []
    preview_df = detail_df.head(12).copy()
    for _, row in preview_df.iterrows():
        fills_text = str(row.get("fill_detail_json", "") or "").strip()
        fill_steps: list[str] = []
        if fills_text:
            try:
                fill_items = json.loads(fills_text)
                if isinstance(fill_items, list):
                    for fill in fill_items[:3]:
                        if isinstance(fill, dict):
                            fill_steps.append(
                                f"{fill.get('exit_type', '-')}: {format_percent(float(fill.get('weight', 0.0)) * 100, 0)} @ {format_number(fill.get('sell_price', ''), 2)}"
                            )
            except (json.JSONDecodeError, TypeError, ValueError):
                fill_steps = []

        entry_fill_type = str(row.get("entry_fill_type", "") or "").strip()
        trigger_price = row.get("entry_trigger_price")
        entry_execution = "触发成交" if entry_fill_type == "trigger" else "开盘成交"
        if pd.notna(trigger_price):
            entry_execution = f"{entry_execution} @ {format_number(trigger_price, 2)}"

        summary_rows.append(
            {
                "信号日期": format_timestamp_for_display(row.get("date")),
                "股票代码": str(row.get("stock_code", "") or ""),
                "开仓决策链": " -> ".join(
                    [
                        str(row.get("entry_reason", "-") or "-"),
                        entry_execution,
                    ]
                ),
                "离场决策链": " -> ".join(
                    filter(
                        None,
                        [
                            str(row.get("exit_reason", "-") or "-"),
                            " | ".join(fill_steps),
                        ],
                    )
                ),
            }
        )

    return pd.DataFrame(summary_rows)


def render_summary_card(title: str, value: str, help_text: str) -> None:
    st.markdown(f"**{title}**")
    st.metric(title, value)
    st.caption(help_text)


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


def format_trade_behavior_for_display(behavior_df: pd.DataFrame) -> pd.DataFrame:
    if behavior_df.empty:
        return behavior_df

    display_df = behavior_df.copy()
    for column in TRADE_BEHAVIOR_PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)
    for column in TRADE_BEHAVIOR_NUMBER_COLUMNS:
        if column in display_df.columns:
            formatter = (
                format_number
                if column != "executed_trades"
                else lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
            display_df[column] = display_df[column].map(formatter)
    return display_df.rename(
        columns={
            column: TRADE_BEHAVIOR_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in TRADE_BEHAVIOR_COLUMN_LABELS
        }
    )


def format_drawdown_episodes_for_display(episodes_df: pd.DataFrame) -> pd.DataFrame:
    if episodes_df.empty:
        return episodes_df

    display_df = episodes_df.copy()
    for column in ("drawdown_start_date", "trough_date", "recovery_date"):
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_timestamp_for_display)
    for column in DD_EPISODE_PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)
    for column in DD_EPISODE_COUNT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
    if "recovered_flag" in display_df.columns:
        display_df["recovered_flag"] = display_df["recovered_flag"].map(
            lambda value: "是" if bool(value) else "否"
        )
    return display_df.rename(
        columns={
            column: DD_EPISODE_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in DD_EPISODE_COLUMN_LABELS
        }
    )


def format_drawdown_contributors_for_display(
    contributors_df: pd.DataFrame,
) -> pd.DataFrame:
    if contributors_df.empty:
        return contributors_df

    display_df = contributors_df.copy()
    for column in DD_CONTRIBUTOR_PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)
    for column in DD_CONTRIBUTOR_COUNT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
    return display_df.rename(
        columns={
            column: DD_CONTRIBUTOR_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in DD_CONTRIBUTOR_COLUMN_LABELS
        }
    )


def format_anomaly_queue_for_display(
    anomaly_df: pd.DataFrame,
    *,
    symbol_name_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    if anomaly_df.empty:
        return anomaly_df

    display_df = anomaly_df.copy()
    if "date" in display_df.columns:
        display_df["date"] = display_df["date"].map(format_timestamp_for_display)
    if "stock_code" in display_df.columns:
        resolved_symbol_name_map = (
            symbol_name_map if symbol_name_map is not None else load_symbol_name_map()
        )
        display_df["stock_code"] = display_df["stock_code"].map(
            lambda value: build_display_symbol_label(value, resolved_symbol_name_map)
        )
    for column in ANOMALY_PERCENT_COLUMNS:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)
    for column in ANOMALY_NUMBER_COLUMNS:
        if column in display_df.columns:
            digits = 2 if column == "severity_score" else 0
            formatter = (
                (lambda value: format_number(value, digits=2))
                if column == "severity_score"
                else (lambda value: f"{int(value):,}" if pd.notna(value) else "")
            )
            display_df[column] = display_df[column].map(formatter)
    return display_df.rename(
        columns={
            column: ANOMALY_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in ANOMALY_COLUMN_LABELS
        }
    )


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
            "min_profit_to_activate_profit_drawdown_pct",
            "exit_ma_period",
            "atr_trailing_period",
            "atr_trailing_multiplier",
            "min_profit_to_activate_atr_trailing_pct",
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


def format_per_stock_stats_for_display(stats_df: pd.DataFrame) -> pd.DataFrame:
    if stats_df.empty:
        return stats_df
    display_df = stats_df.copy()
    percent_cols = [
        "total_return_pct",
        "strategy_win_rate_pct",
        "max_drawdown_pct",
        "avg_mfe_pct",
        "avg_mae_pct",
        "trade_return_volatility_pct",
    ]
    number_cols = ["final_net_value", "avg_holding_days", "profit_risk_ratio"]
    count_cols = ["executed_trades", "signal_count"]
    for column in percent_cols:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_percent)
    for column in number_cols:
        if column in display_df.columns:
            digits = 4 if column == "final_net_value" else 2
            display_df[column] = display_df[column].map(
                lambda value: format_number(value, digits=digits)
            )
    for column in count_cols:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
    display_df = display_df.rename(
        columns={
            "stock_code": "股票代码",
            "signal_count": "信号数",
            "executed_trades": "交易笔数",
            "total_return_pct": "总收益率",
            "strategy_win_rate_pct": "胜率",
            "max_drawdown_pct": "最大回撤",
            "final_net_value": "期末净值",
            "avg_holding_days": "平均持有天数",
            "profit_risk_ratio": "盈亏比",
            "avg_mfe_pct": "平均最大有利波动",
            "avg_mae_pct": "平均最大不利波动",
            "trade_return_volatility_pct": "单笔收益波动率",
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


def build_trade_behavior_column_config() -> dict[str, object]:
    return {
        "交易笔数": st.column_config.TextColumn("交易笔数", width="small"),
        "胜率": st.column_config.TextColumn("胜率", width="small"),
        "平均净收益率": st.column_config.TextColumn("平均净收益率", width="small"),
        "净收益率中位数": st.column_config.TextColumn("净收益率中位数", width="small"),
        "平均最大有利波动": st.column_config.TextColumn(
            "平均最大有利波动", width="small"
        ),
        "平均最大不利波动": st.column_config.TextColumn(
            "平均最大不利波动", width="small"
        ),
        "平均利润回吐": st.column_config.TextColumn("平均利润回吐", width="small"),
        "平均 MFE 兑现率": st.column_config.TextColumn(
            "平均 MFE 兑现率", width="small"
        ),
        "触发成交占比": st.column_config.TextColumn("触发成交占比", width="small"),
        "多批成交占比": st.column_config.TextColumn("多批成交占比", width="small"),
        "平均利润回撤比": st.column_config.TextColumn("平均利润回撤比", width="small"),
    }


def build_drawdown_episode_column_config() -> dict[str, object]:
    return {
        "回撤段编号": st.column_config.TextColumn("回撤段编号", width="small"),
        "回撤开始": st.column_config.TextColumn("回撤开始", width="medium"),
        "回撤谷值": st.column_config.TextColumn("回撤谷值", width="medium"),
        "恢复日期": st.column_config.TextColumn("恢复日期", width="medium"),
        "峰谷回撤": st.column_config.TextColumn("峰谷回撤", width="small"),
        "水下周期数": st.column_config.TextColumn("水下周期数", width="small"),
        "涉及交易数": st.column_config.TextColumn("涉及交易数", width="small"),
        "最差单笔收益": st.column_config.TextColumn("最差单笔收益", width="small"),
        "主导开仓原因": st.column_config.TextColumn("主导开仓原因", width="medium"),
        "是否恢复": st.column_config.TextColumn("是否恢复", width="small"),
    }


def build_drawdown_contributor_column_config() -> dict[str, object]:
    return {
        "开仓原因": st.column_config.TextColumn("开仓原因", width="medium"),
        "交易数": st.column_config.TextColumn("交易数", width="small"),
        "平均净收益率": st.column_config.TextColumn("平均净收益率", width="small"),
        "累计净收益率": st.column_config.TextColumn("累计净收益率", width="small"),
        "平均最大不利波动": st.column_config.TextColumn(
            "平均最大不利波动", width="small"
        ),
        "平均最大有利波动": st.column_config.TextColumn(
            "平均最大有利波动", width="small"
        ),
    }


def build_anomaly_queue_column_config() -> dict[str, object]:
    return {
        "异常类型": st.column_config.TextColumn("异常类型", width="small"),
        "严重度": st.column_config.TextColumn("严重度", width="small"),
        "交易编号": st.column_config.TextColumn("交易编号", width="small"),
        "信号日期": st.column_config.TextColumn("信号日期", width="medium"),
        "股票代码": st.column_config.TextColumn("股票代码", width="small"),
        "持有天数": st.column_config.TextColumn("持有天数", width="small"),
        "激发阈值": st.column_config.TextColumn("激发阈值", width="small"),
        "超阈值幅度": st.column_config.TextColumn("超阈值幅度", width="small"),
        "日均最大浮盈": st.column_config.TextColumn("日均最大浮盈", width="small"),
        "日均最大不利波动": st.column_config.TextColumn(
            "日均最大不利波动", width="small"
        ),
        "利润回吐": st.column_config.TextColumn("利润回吐", width="small"),
        "净收益率": st.column_config.TextColumn("净收益率", width="small"),
        "开仓原因": st.column_config.TextColumn("开仓原因", width="medium"),
        "离场原因": st.column_config.TextColumn("离场原因", width="medium"),
        "诊断说明": st.column_config.TextColumn("诊断说明", width="large"),
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
        "板块均线值": st.column_config.TextColumn("板块均线值", width="small"),
        "导入指标离场值": st.column_config.TextColumn("导入指标离场值", width="small"),
        "分批指标止盈说明": st.column_config.TextColumn(
            "分批指标止盈说明", width="large"
        ),
        "分批指标触发值": st.column_config.TextColumn("分批指标触发值", width="small"),
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
        "早盘观察窗口K数": st.column_config.TextColumn(
            "早盘观察窗口K数", width="small"
        ),
        "高位横盘最少K数": st.column_config.TextColumn(
            "高位横盘最少K数", width="small"
        ),
        "高位横盘最多K数": st.column_config.TextColumn(
            "高位横盘最多K数", width="small"
        ),
        "早盘冲高最小涨幅": st.column_config.TextColumn(
            "早盘冲高最小涨幅", width="small"
        ),
        "横盘最大回撤": st.column_config.TextColumn("横盘最大回撤", width="small"),
        "横盘最大振幅": st.column_config.TextColumn("横盘最大振幅", width="small"),
        "锚点跌破次数上限": st.column_config.TextColumn(
            "锚点跌破次数上限", width="small"
        ),
        "锚点跌破深度上限": st.column_config.TextColumn(
            "锚点跌破深度上限", width="small"
        ),
        "冲高量能倍数下限": st.column_config.TextColumn(
            "冲高量能倍数下限", width="small"
        ),
        "突破量能倍数下限": st.column_config.TextColumn(
            "突破量能倍数下限", width="small"
        ),
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
        return pd.DataFrame(
            columns=pd.Index(
                [
                    "symbol",
                    "timeframe",
                    "row_count",
                    "date_range",
                    "updated_at",
                    "last_update_status",
                ]
            )
        )
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
        min_ts = cast(
            pd.Timestamp, pd.to_datetime(cast(Any, min_date), errors="coerce")
        )
        max_ts = cast(
            pd.Timestamp, pd.to_datetime(cast(Any, max_date), errors="coerce")
        )
        return (
            f"{min_ts.strftime('%Y-%m-%d %H:%M')} → {max_ts.strftime('%Y-%m-%d %H:%M')}"
        )

    preview["date_range"] = preview.apply(_format_inventory_range, axis=1)
    preview = preview.sort_values("updated_at", ascending=False)
    return cast(
        pd.DataFrame,
        preview[
            [
                "symbol",
                "timeframe",
                "row_count",
                "date_range",
                "updated_at",
                "last_update_status",
            ]
        ]
        .head(limit)
        .reset_index(drop=True),
    )


@st.cache_data(show_spinner=False)
def load_indicator_registry_preview() -> pd.DataFrame:
    return load_registry_manifest()


@st.cache_data(show_spinner=False)
def load_indicator_availability_preview(limit: int = 20) -> pd.DataFrame:
    return summarize_indicator_availability(limit=limit)


@st.cache_data(show_spinner=False)
def load_indicator_quality_preview(limit: int = 20) -> pd.DataFrame:
    return summarize_indicator_quality(limit=limit)


def format_indicator_registry_for_display(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return preview_df
    display_df = preview_df.copy()
    bool_columns = ["allow_scan", "allow_filter", "allow_exit"]
    for column in bool_columns:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: "是" if bool(value) else "否"
            )
    return display_df.rename(
        columns={
            "indicator_key": "指标Key",
            "display_name": "名称",
            "source_type": "来源",
            "output_columns": "输出列",
            "align_rule": "对齐规则",
            "required_timeframe": "依赖粒度",
            "lookahead_policy": "Lookahead规则",
            "allow_scan": "可扫描",
            "allow_filter": "可过滤",
            "allow_exit": "可离场",
            "storage_subdir": "存储目录",
            "formula_name": "公式名",
            "description": "说明",
        }
    )


def format_indicator_availability_for_display(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return preview_df
    display_df = preview_df.copy()
    if "updated_at" in display_df.columns:
        display_df["updated_at"] = pd.to_datetime(
            display_df["updated_at"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M")
    for column in ("symbols", "rows"):
        if column in display_df.columns:
            display_df[column] = display_df[column].map(
                lambda value: f"{int(value):,}" if pd.notna(value) else ""
            )
    return display_df.rename(
        columns={
            "indicator_key": "指标Key",
            "display_name": "名称",
            "source_type": "来源",
            "timeframe": "粒度",
            "adjust": "复权",
            "symbols": "股票数",
            "rows": "总行数",
            "date_range": "覆盖区间",
            "status": "状态",
            "updated_at": "更新时间",
        }
    )


def format_indicator_quality_for_display(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return preview_df
    return preview_df.rename(
        columns={
            "indicator_key": "指标Key",
            "symbol": "股票",
            "timeframe": "粒度",
            "columns_ready": "有效列",
            "date_range": "覆盖区间",
            "status": "状态",
        }
    )


def imported_indicator_options_by_capability(
    registry_df: pd.DataFrame,
    capability_column: str,
) -> tuple[list[str], dict[str, list[str]], dict[str, str]]:
    if registry_df.empty:
        return [""], {}, {}
    filtered = registry_df.loc[registry_df[capability_column].astype(bool)].copy()
    if filtered.empty:
        return [""], {}, {}
    key_to_columns: dict[str, list[str]] = {}
    key_to_label: dict[str, str] = {}
    ordered_keys = [""]
    for row in filtered.to_dict("records"):
        indicator_key = str(row.get("indicator_key", "")).strip()
        if not indicator_key:
            continue
        key_to_label[indicator_key] = str(row.get("display_name", indicator_key))
        output_columns = [
            item.strip()
            for item in str(row.get("output_columns", "")).split(",")
            if item.strip()
        ]
        key_to_columns[indicator_key] = output_columns
        ordered_keys.append(indicator_key)
    return ordered_keys, key_to_columns, key_to_label


def imported_indicator_filter_options(
    registry_df: pd.DataFrame,
) -> tuple[list[str], dict[str, list[str]], dict[str, str]]:
    return imported_indicator_options_by_capability(registry_df, "allow_filter")


def imported_indicator_exit_options(
    registry_df: pd.DataFrame,
) -> tuple[list[str], dict[str, list[str]], dict[str, str]]:
    return imported_indicator_options_by_capability(registry_df, "allow_exit")


def build_imported_indicator_rules(
    *,
    prefix: str,
    count: int,
    default_enabled: bool,
) -> tuple[ImportedIndicatorRule, ...]:
    default_operator = ">=" if prefix == "imported_filter" else "<="
    rules: list[ImportedIndicatorRule] = []
    for index in range(1, count + 1):
        rules.append(
            ImportedIndicatorRule(
                enabled=bool(
                    st.session_state.get(
                        f"{prefix}_rule_enabled_{index}",
                        default_enabled if index == 1 else False,
                    )
                ),
                indicator_key=str(st.session_state.get(f"{prefix}_key_{index}", "")),
                column=str(st.session_state.get(f"{prefix}_column_{index}", "")),
                operator=str(
                    st.session_state.get(
                        f"{prefix}_operator_{index}",
                        default_operator,
                    )
                ),
                threshold=float(
                    st.session_state.get(f"{prefix}_threshold_{index}", 0.0)
                ),
                priority=index,
            )
        )
    return tuple(rules)


def build_partial_indicator_rule(index: int) -> dict[str, Any]:
    return {
        "indicator_key": str(st.session_state.get(f"p_indicator_key_{index}", "")),
        "indicator_column": str(
            st.session_state.get(f"p_indicator_column_{index}", "")
        ),
        "indicator_operator": str(
            st.session_state.get(f"p_indicator_operator_{index}", ">=")
        ),
        "indicator_threshold": float(
            st.session_state.get(f"p_indicator_threshold_{index}", 0.0)
        ),
    }


def format_local_inventory_for_display(preview_df: pd.DataFrame) -> pd.DataFrame:
    if preview_df.empty:
        return preview_df
    display_df = preview_df.copy()
    if "row_count" in display_df.columns:
        display_df["row_count"] = display_df["row_count"].map(
            lambda value: f"{int(value):,}" if pd.notna(value) else ""
        )
    if "updated_at" in display_df.columns:
        display_df["updated_at"] = pd.to_datetime(
            display_df["updated_at"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M")
    return display_df.rename(
        columns={
            column: LOCAL_INVENTORY_COLUMN_LABELS[column]
            for column in display_df.columns
            if column in LOCAL_INVENTORY_COLUMN_LABELS
        }
    )


def _read_sqlite_preview_rows(
    db_path: str, table_name: str, limit: int = 20
) -> pd.DataFrame:
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
                    return pd.read_csv(BytesIO(file_bytes), encoding=encoding).head(
                        limit
                    )
                if file_path:
                    return pd.read_csv(file_path, encoding=encoding).head(limit)
            except UnicodeDecodeError:
                continue
        raise ValueError("CSV 文件预览失败。")

    if file_bytes is not None:
        with pd.ExcelFile(BytesIO(file_bytes)) as workbook:
            target_sheet = (
                sheet_name.strip()
                if sheet_name and sheet_name.strip()
                else workbook.sheet_names[0]
            )
            return pd.read_excel(workbook, sheet_name=target_sheet).head(limit)
    if file_path:
        with pd.ExcelFile(file_path) as workbook:
            target_sheet = (
                sheet_name.strip()
                if sheet_name and sheet_name.strip()
                else workbook.sheet_names[0]
            )
            return pd.read_excel(workbook, sheet_name=target_sheet).head(limit)
    return pd.DataFrame()


def _resolve_uploaded_file_inputs(
    uploaded_market_file: Any | None,
    input_file_path: str,
) -> tuple[bytes | None, str | None, str | None]:
    uploaded_file_bytes = (
        uploaded_market_file.getvalue() if uploaded_market_file is not None else None
    )
    uploaded_file_name = (
        uploaded_market_file.name if uploaded_market_file is not None else None
    )
    normalized_path = input_file_path.strip() or None
    return uploaded_file_bytes, uploaded_file_name, normalized_path


def build_sqlite_probe_payload(db_path: str, selected_table: str) -> dict[str, Any]:
    candidate_tables = list_candidate_tables(db_path)
    overview_df = pd.DataFrame(describe_tables(db_path))
    resolved_table = selected_table.strip() or (
        candidate_tables[0] if candidate_tables else ""
    )
    preview_df = (
        _read_sqlite_preview_rows(db_path, resolved_table, limit=20)
        if resolved_table
        else pd.DataFrame()
    )
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
    sheet_names = list_file_sheets(
        file_path=file_path, file_bytes=file_bytes, file_name=file_name
    )
    resolved_sheet = (
        sheet_name.strip()
        if sheet_name and sheet_name.strip()
        else (sheet_names[0] if sheet_names else None)
    )
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
                matched = sqlite_payload["overview_df"].loc[
                    sqlite_payload["overview_df"]["table_name"] == target
                ]
                auto_detected = (
                    str(matched.iloc[0]["auto_detected"])
                    if not matched.empty
                    else "未探测"
                )
            else:
                auto_detected = "未探测"
        except Exception:
            target = table_name.strip() or Path(db_path).name or "未识别数据表"
            auto_detected = "未探测"
    elif data_source_label == "Excel/CSV 文件":
        file_bytes, file_name, file_path = _resolve_uploaded_file_inputs(
            uploaded_market_file, input_file_path
        )
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
            auto_detected = (
                "是" if bool(file_payload["description"].get("auto_detected")) else "否"
            )
        except Exception:
            auto_detected = "未探测"
    return pd.DataFrame(
        [
            {
                "数据源类型": data_source_label,
                "timeframe": timeframe,
                "adjust": adjust_label,
                "当前表/文件/sheet": target,
                "是否自动识别成功": auto_detected,
            }
        ]
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

apply_pending_page_change()

st.sidebar.markdown("**页面导航**")
page_mode_options = ["回测工作台", "数据准备页", "交易配置说明"]
page_mode_default = str(st.session_state.get("page_mode", "回测工作台"))
if page_mode_default not in page_mode_options:
    page_mode_default = "回测工作台"
page_mode = st.sidebar.radio(
    "页面",
    options=page_mode_options,
    label_visibility="collapsed",
    key="page_mode",
    index=page_mode_options.index(page_mode_default),
)
st.sidebar.caption("回测与数据准备分开，减少单页堆叠。")

if page_mode == "交易配置说明":
    render_trading_guide_page()
    st.stop()

if page_mode == "数据准备页":
    render_data_prep_page_header()
else:
    render_backtest_page_header()

today = pd.Timestamp.today().date()
default_update_start = today - timedelta(days=30)
default_backtest_start = today - timedelta(days=365)

if page_mode == "数据准备页":
    render_data_prep_sidebar_intro()
    st.caption("推荐先离线更新本地 parquet，再进行回测。")
    update_source_defaults = resolve_offline_update_sources(read_data_source_config())
    update_symbols = st.text_area(
        "股票代码（可选）",
        value="",
        help="多个代码用逗号分隔，空表示全量。",
        key="offline_update_symbols",
    )
    st.caption("常用指数")
    update_index_cols_top = st.columns(3)
    update_index_cols_bottom = st.columns(2)
    for button_col, (index_name, index_code, index_key) in zip(
        [*update_index_cols_top, *update_index_cols_bottom], MAJOR_INDEX_PRESETS
    ):
        button_col.button(
            index_name,
            key=f"offline_update_symbol_preset_{index_key}",
            use_container_width=True,
            on_click=apply_offline_update_scope_preset,
            args=(index_code,),
        )
    if "tdx_tqcenter_path" not in st.session_state:
        st.session_state["tdx_tqcenter_path"] = ""
    if "tdx_tqcenter_path_display" not in st.session_state:
        st.session_state["tdx_tqcenter_path_display"] = st.session_state[
            "tdx_tqcenter_path"
        ]
    st.caption("通达信安装目录（在使用 TDX 作为更新源时生效）")
    picker_cols = st.columns(2)
    if picker_cols[0].button(
        "选择通达信目录",
        key="pick_tdx_install_dir",
        use_container_width=True,
    ):
        selected_path, picker_warning = pick_tdx_tqcenter_path(
            str(st.session_state.get("tdx_tqcenter_path", ""))
        )
        st.session_state["tdx_tqcenter_path"] = selected_path
        st.session_state["tdx_tqcenter_path_display"] = selected_path
        if picker_warning:
            st.warning(picker_warning)
    if picker_cols[1].button(
        "清空目录",
        key="clear_tdx_install_dir",
        use_container_width=True,
    ):
        st.session_state["tdx_tqcenter_path"] = ""
        st.session_state["tdx_tqcenter_path_display"] = ""
    st.text_input(
        "已选择目录",
        key="tdx_tqcenter_path_display",
        disabled=True,
        help="通过上方按钮选择目录后，这里会展示当前用于 TDX_TQCENTER_PATH 的路径。",
    )
    if "offline_update_start" not in st.session_state:
        st.session_state["offline_update_start"] = default_update_start
    if "offline_update_end" not in st.session_state:
        st.session_state["offline_update_end"] = today
    st.caption("快捷区间")
    update_preset_cols_top = st.columns(2)
    update_preset_cols_bottom = st.columns(2)
    update_preset_col_ytd = st.columns(1)
    for button_col, (preset_label, years) in zip(
        [*update_preset_cols_top, *update_preset_cols_bottom], BACKTEST_RANGE_PRESETS
    ):
        if button_col.button(
            preset_label,
            key=f"offline_update_preset_{years}y",
            use_container_width=True,
        ):
            st.session_state["offline_update_start"] = preset_start_date(today, years)
            st.session_state["offline_update_end"] = today
    if update_preset_col_ytd[0].button(
        "YTD", key="offline_update_preset_ytd", use_container_width=True
    ):
        st.session_state["offline_update_start"] = ytd_start_date(today)
        st.session_state["offline_update_end"] = today
    update_date_cols = st.columns(2)
    update_start = update_date_cols[0].date_input(
        "更新开始日期", key="offline_update_start"
    )
    update_end = update_date_cols[1].date_input(
        "更新结束日期", key="offline_update_end"
    )
    update_option_cols = st.columns(2)
    with update_option_cols[0]:
        update_timeframes = st.multiselect(
            "更新周期",
            options=list(TIMEFRAME_OPTIONS),
            default=["1d"],
            key="offline_update_timeframe",
        )
        refresh_symbol_meta = st.checkbox(
            "刷新股票列表", value=False, key="offline_update_refresh"
        )
    with update_option_cols[1]:
        update_adjust = st.selectbox(
            "更新复权方式", options=["qfq", "hfq"], index=0, key="offline_update_adjust"
        )
        export_excel_after_update = st.checkbox(
            "更新后另存为 Excel", value=False, key="offline_update_export"
        )
    update_provider_overrides: dict[str, str] = {}
    selected_update_timeframes = [str(item) for item in update_timeframes]
    if selected_update_timeframes:
        st.caption("各周期更新源")
        for timeframe_option in selected_update_timeframes:
            supported_sources = get_supported_update_sources(timeframe_option)
            default_source = update_source_defaults.get(
                timeframe_option, supported_sources[0]
            )
            default_index = (
                supported_sources.index(default_source)
                if default_source in supported_sources
                else 0
            )
            selected_provider = st.selectbox(
                f"{timeframe_option} 更新源",
                options=list(supported_sources),
                index=default_index,
                key=f"offline_update_provider_{timeframe_option}",
                format_func=lambda value: UPDATE_SOURCE_LABELS.get(
                    str(value), str(value)
                ),
            )
            update_provider_overrides[timeframe_option] = str(selected_provider)
    if st.button("开始更新本地数据", key="offline_update_submit"):
        ok, output = run_local_data_update(
            symbols_text=update_symbols,
            start_date=update_start.strftime("%Y-%m-%d"),
            end_date=update_end.strftime("%Y-%m-%d"),
            adjust=update_adjust,
            timeframes=selected_update_timeframes,
            refresh_symbols=bool(refresh_symbol_meta),
            export_excel=bool(export_excel_after_update),
            provider_overrides=update_provider_overrides,
            tdx_tqcenter_path=str(st.session_state.get("tdx_tqcenter_path", "")),
        )
        if ok:
            st.success("本地数据更新完成")
        else:
            st.error("本地数据更新失败")
        st.caption(
            "导出目录："
            + "、".join(
                [
                    build_export_dir_hint(str(timeframe), str(update_adjust))
                    for timeframe in (
                        [str(item) for item in update_timeframes] or ["1d"]
                    )
                ]
            )
        )
        if output:
            st.code(output)
    st.caption(
        "当前支持按周期分别选择 AKShare / TDX 更新源；当前更新链路已覆盖 1d / 30m / 15m / 5m。"
    )
    st.markdown("**指标管理**")
    st.caption("按 探测 → 配置 → 导入 → 状态 → 可用性检查 的顺序管理本地指标。")
    indicator_registry_preview = load_indicator_registry_preview()
    if not indicator_registry_preview.empty:
        st.markdown("**指标探测 / 注册表**")
        dataframe_stretch(
            format_indicator_registry_for_display(indicator_registry_preview),
            hide_index=True,
            height=220,
        )
    probe_action_cols = st.columns([1.1, 1])
    if probe_action_cols[0].button("探测通达信本地指标", key="offline_indicator_probe"):
        candidates, probe_message = probe_local_indicator_candidates(
            str(st.session_state.get("tdx_tqcenter_path", ""))
        )
        st.session_state["offline_indicator_candidates"] = candidates
        st.session_state["offline_indicator_probe_message"] = probe_message
        load_indicator_registry_preview.clear()
        load_indicator_availability_preview.clear()
        load_indicator_quality_preview.clear()

    probe_message = str(
        st.session_state.get("offline_indicator_probe_message", "")
    ).strip()
    if probe_message:
        st.info(probe_message)

    indicator_candidates = cast(
        list[tuple[str, str]],
        st.session_state.get(
            "offline_indicator_candidates", [("board_ma", "板块均线")]
        ),
    )
    indicator_import_cols = st.columns(2)
    with indicator_import_cols[0]:
        indicator_mode = st.radio(
            "指标来源",
            options=["已知指标", "手动指定"],
            horizontal=True,
            key="offline_indicator_mode",
        )
    with indicator_import_cols[1]:
        indicator_adjust = st.selectbox(
            "指标复权方式",
            options=["qfq", "hfq"],
            index=0,
            key="offline_indicator_adjust",
        )
        st.caption(
            "当前 registry 已区分 market_native / tdx_formula_local / computed_feature。"
        )

    selected_indicator_key = "board_ma"
    manual_formula_name = ""
    manual_output_map_text = ""
    if indicator_mode == "已知指标":
        selected_indicator_key = st.selectbox(
            "本地指标",
            options=[item[0] for item in indicator_candidates],
            format_func=lambda value: next(
                (label for key, label in indicator_candidates if key == value),
                str(value),
            ),
            key="offline_indicator_key",
        )
    else:
        selected_indicator_key = (
            st.text_input(
                "指标标识", value="board_ma", key="offline_indicator_manual_key"
            ).strip()
            or "board_ma"
        )
        manual_formula_name = st.text_input(
            "公式名称", value="板块均线", key="offline_indicator_formula_name"
        )
        manual_output_map_text = st.text_area(
            "输出映射",
            value="board_ma_ratio_20=NOTEXT1\nboard_ma_ratio_50=NOTEXT2",
            help="每行一个映射，格式：目标列=公式输出键。例：board_ma_ratio_20=NOTEXT1",
            key="offline_indicator_output_map_text",
        )

    if st.button("导入通达信本地指标", key="offline_indicator_submit"):
        ok, output = run_local_indicator_import(
            indicator_key=str(selected_indicator_key),
            symbols_text=update_symbols,
            start_date=update_start.strftime("%Y-%m-%d"),
            end_date=update_end.strftime("%Y-%m-%d"),
            adjust=str(indicator_adjust),
            formula_name=manual_formula_name,
            output_map_text=manual_output_map_text,
            tdx_tqcenter_path=str(st.session_state.get("tdx_tqcenter_path", "")),
        )
        if ok:
            st.success("本地指标导入完成")
        else:
            st.error("本地指标导入失败")
        if output:
            st.code(output)
        load_indicator_registry_preview.clear()
        load_indicator_availability_preview.clear()
        load_indicator_quality_preview.clear()

    indicator_availability_preview = load_indicator_availability_preview(limit=20)
    if not indicator_availability_preview.empty:
        st.markdown("**导入状态 / 可用性**")
        dataframe_stretch(
            format_indicator_availability_for_display(indicator_availability_preview),
            hide_index=True,
            height=240,
        )
    indicator_quality_preview = load_indicator_quality_preview(limit=20)
    if not indicator_quality_preview.empty:
        st.markdown("**可用性检查**")
        dataframe_stretch(
            format_indicator_quality_for_display(indicator_quality_preview),
            hide_index=True,
            height=220,
        )
    inventory_preview = load_local_inventory_preview(limit=200)
    inventory_summary_df = summarize_local_inventory(inventory_preview)
    if not inventory_summary_df.empty:
        st.markdown("**库内已有数据**")
        st.caption("按周期汇总展示已有标的数、总行数和最近更新时间。")
        dataframe_stretch(inventory_summary_df, hide_index=True, height=180)
    preview = load_update_log_preview(limit=20)
    log_action_cols = st.columns([1, 3])
    if log_action_cols[0].button("清空更新日志", key="clear_update_log"):
        if clear_update_log():
            st.success("更新日志已清空")
        else:
            st.info("当前没有可清空的更新日志")
    if not preview.empty:
        st.markdown("**最近更新日志**")
        dataframe_stretch(
            format_update_log_for_display(preview),
            hide_index=True,
            column_config=build_update_log_column_config(),
            height=220,
        )

if page_mode == "数据准备页":
    st.sidebar.info("当前页面仅保留数据准备功能。切回“回测工作台”开始回测。")
    st.stop()

render_backtest_sidebar_intro()

st.sidebar.markdown("**回测范围**")
stock_scope_text = st.sidebar.text_area(
    "股票池",
    value="",
    help="多个代码可用逗号/空格/换行。留空表示全市场。",
    key="stock_scope_text",
)
st.sidebar.caption("常用指数")
index_preset_cols_top = st.sidebar.columns(3)
index_preset_cols_bottom = st.sidebar.columns(2)
for button_col, (index_name, index_code, index_key) in zip(
    [*index_preset_cols_top, *index_preset_cols_bottom], MAJOR_INDEX_PRESETS
):
    button_col.button(
        index_name,
        key=f"stock_scope_preset_{index_key}",
        use_container_width=True,
        on_click=apply_stock_scope_preset,
        args=(index_code,),
    )
batch_backtest_mode_label = st.sidebar.radio(
    "多股票回测模式",
    options=["组合回测（单账户）", "逐股独立回测（批量）"],
    index=0,
    help="逐股独立回测会对股票池中的每只股票单独回测，不合并持仓与资金曲线。",
)
if "backtest_start_date" not in st.session_state:
    st.session_state["backtest_start_date"] = default_backtest_start
if "backtest_end_date" not in st.session_state:
    st.session_state["backtest_end_date"] = today
st.sidebar.caption("快捷区间")
preset_cols_top = st.sidebar.columns(2)
preset_cols_bottom = st.sidebar.columns(2)
preset_col_ytd = st.sidebar.columns(1)
for button_col, (preset_label, years) in zip(
    [*preset_cols_top, *preset_cols_bottom], BACKTEST_RANGE_PRESETS
):
    if button_col.button(
        preset_label, key=f"backtest_preset_{years}y", use_container_width=True
    ):
        st.session_state["backtest_start_date"] = preset_start_date(today, years)
        st.session_state["backtest_end_date"] = today
if preset_col_ytd[0].button("YTD", key="backtest_preset_ytd", use_container_width=True):
    st.session_state["backtest_start_date"] = ytd_start_date(today)
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
sidebar_entry_factor = str(st.session_state.get("entry_factor", "gap"))
allowed_timeframes = enforce_entry_factor_timeframe(sidebar_entry_factor)
timeframe = st.sidebar.selectbox(
    "周期",
    options=list(allowed_timeframes),
    index=list(allowed_timeframes).index(
        str(st.session_state.get("timeframe", allowed_timeframes[0]))
    ),
    key="timeframe",
)
st.sidebar.caption(get_strategy_capability_summary(sidebar_entry_factor))

st.sidebar.button(
    "进入数据准备页",
    key="goto_data_prep",
    use_container_width=True,
    on_click=request_page_change,
    args=("数据准备页",),
)
st.sidebar.caption("点击后会直接切换到数据准备页。")

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
                st.session_state["sqlite_probe_payload"] = build_sqlite_probe_payload(
                    db_path, table_name
                )
                if st.session_state["sqlite_probe_payload"]["selected_table"]:
                    st.session_state["sqlite_table_name"] = st.session_state[
                        "sqlite_probe_payload"
                    ]["selected_table"]
            except Exception as exc:
                st.error(f"数据表探测失败：{exc}")
        sqlite_probe_payload = st.session_state.get("sqlite_probe_payload")
        if isinstance(sqlite_probe_payload, dict) and sqlite_probe_payload.get(
            "candidate_tables"
        ):
            candidate_options = list(sqlite_probe_payload["candidate_tables"])
            default_index = (
                candidate_options.index(
                    st.session_state.get("sqlite_table_name", candidate_options[0])
                )
                if st.session_state.get("sqlite_table_name", candidate_options[0])
                in candidate_options
                else 0
            )
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
        excel_sheet_name = st.text_input(
            "工作表（Excel 可选）", value="", key="excel_sheet_name"
        )
        if st.button("预览文件结构", key="file_probe_preview"):
            file_bytes, file_name, file_path = _resolve_uploaded_file_inputs(
                uploaded_market_file, input_file_path
            )
            try:
                st.session_state["file_probe_payload"] = build_file_probe_payload(
                    file_path=file_path,
                    file_bytes=file_bytes,
                    file_name=file_name,
                    sheet_name=excel_sheet_name,
                )
                resolved_sheet = st.session_state["file_probe_payload"].get(
                    "resolved_sheet"
                )
                if resolved_sheet:
                    st.session_state["excel_sheet_name"] = resolved_sheet
            except Exception as exc:
                st.error(f"文件结构预览失败：{exc}")
        file_probe_payload = st.session_state.get("file_probe_payload")
        if isinstance(file_probe_payload, dict) and file_probe_payload.get(
            "sheet_names"
        ):
            sheet_options = list(file_probe_payload["sheet_names"])
            active_sheet = st.session_state.get("excel_sheet_name", sheet_options[0])
            default_index = (
                sheet_options.index(active_sheet)
                if active_sheet in sheet_options
                else 0
            )
            selected_sheet = st.selectbox(
                "工作表候选",
                options=sheet_options,
                index=default_index,
                key="excel_sheet_picker",
            )
            st.session_state["excel_sheet_name"] = selected_sheet
            excel_sheet_name = selected_sheet
    else:
        st.caption("当前使用本地 parquet 数据源，回测将直接读取本地目录。")

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
current_entry_factor = str(st.session_state.get("entry_factor", "gap"))
current_direction_label = normalize_direction_label(
    current_entry_factor,
    st.session_state.get("direction_label"),
)
sqlite_probe_payload = st.session_state.get("sqlite_probe_payload")
file_probe_payload = st.session_state.get("file_probe_payload")

submitted = st.sidebar.button("开始回测", type="primary", key="run_backtest")
st.sidebar.caption("结果会在当前页面下方的标签页中展示。")

st.divider()
section_header("配置摘要", "先确认范围与当前策略，再逐段填写入场、风控和高级项。")
render_backtest_summary_cards(
    stock_scope=summarize_stock_scope(stock_scope_text),
    backtest_range=f"{start_date} → {end_date}",
    source_period=f"{source_summary_title} / {timeframe}",
    source_caption=source_summary_desc,
    strategy_summary=summarize_strategy_choice(
        current_entry_factor, current_direction_label
    ),
    strategy_caption=get_strategy_capability_summary(current_entry_factor),
)

if str(timeframe) in {"30m", "15m", "5m"} and not normalize_stock_codes(
    stock_scope_text
):
    st.warning("当前选择分钟级数据且未指定股票池，读取本地数据时 IO 开销可能较高。")

# ===== 主界面：规则配置 =====
section_header(STRATEGY_SECTION_TITLE, STRATEGY_SECTION_CAPTION)
with st.container():
    entry_factor = st.selectbox(
        "入场因子",
        options=list(ENTRY_FACTORS),
        format_func=lambda value: str(ENTRY_FACTOR_LABELS.get(value, value) or value),
        key="entry_factor",
    )
    reset_inactive_factor_controls(str(entry_factor))
    allowed_timeframes = enforce_entry_factor_timeframe(str(entry_factor))
    if str(st.session_state.get("timeframe", "")) not in allowed_timeframes:
        st.session_state["timeframe"] = allowed_timeframes[0]
    st.caption(get_strategy_capability_summary(str(entry_factor)))
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
                "严格突破前高/前低" if value == "strict_break" else "开盘相对昨收达阈值"
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
    st.session_state.get("gap_entry_mode", factor_control_default("gap_entry_mode"))
)
gap_pct = float(st.session_state.get("gap_pct", factor_control_default("gap_pct")))
max_gap_filter_pct = float(
    st.session_state.get(
        "max_gap_filter_pct", factor_control_default("max_gap_filter_pct")
    )
)
trend_breakout_lookback = int(
    st.session_state.get(
        "trend_breakout_lookback", factor_control_default("trend_breakout_lookback")
    )
)
vcb_range_lookback = int(
    st.session_state.get(
        "vcb_range_lookback", factor_control_default("vcb_range_lookback")
    )
)
vcb_breakout_lookback = int(
    st.session_state.get(
        "vcb_breakout_lookback", factor_control_default("vcb_breakout_lookback")
    )
)
candle_run_length = int(
    st.session_state.get(
        "candle_run_length", factor_control_default("candle_run_length")
    )
)
candle_run_min_body_pct = float(
    st.session_state.get(
        "candle_run_min_body_pct", factor_control_default("candle_run_min_body_pct")
    )
)
candle_run_total_move_pct = float(
    st.session_state.get(
        "candle_run_total_move_pct", factor_control_default("candle_run_total_move_pct")
    )
)
eshb_open_window_bars = int(
    st.session_state.get(
        "eshb_open_window_bars", factor_control_default("eshb_open_window_bars")
    )
)
eshb_base_min_bars = int(
    st.session_state.get(
        "eshb_base_min_bars", factor_control_default("eshb_base_min_bars")
    )
)
eshb_base_max_bars = int(
    st.session_state.get(
        "eshb_base_max_bars", factor_control_default("eshb_base_max_bars")
    )
)
eshb_surge_min_pct = float(
    st.session_state.get(
        "eshb_surge_min_pct", factor_control_default("eshb_surge_min_pct")
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
        "eshb_max_base_range_pct", factor_control_default("eshb_max_base_range_pct")
    )
)
eshb_max_anchor_breaks = int(
    st.session_state.get(
        "eshb_max_anchor_breaks", factor_control_default("eshb_max_anchor_breaks")
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
        "eshb_trigger_buffer_pct", factor_control_default("eshb_trigger_buffer_pct")
    )
)

section_header(RISK_SECTION_TITLE, RISK_SECTION_CAPTION)
with st.container():
    st.caption("时间退出为基础规则，当前版本始终生效。")
    use_time_stop = True
    time_stop_cols = st.columns(2)
    time_stop_days = time_stop_cols[0].number_input(
        "最多持有天数 N", min_value=1, value=5, step=1
    )
    time_stop_target_pct = time_stop_cols[1].number_input(
        "时间退出收益阈值（%）", value=1.0, step=0.1
    )
    exit_mode_cols = st.columns([1.8, 1])
    time_exit_mode_label = exit_mode_cols[0].selectbox(
        "到期处理", options=["按原规则剔除未达条件信号", "第 N 天按收盘价结束交易"]
    )
    stop_loss_pct = exit_mode_cols[1].number_input(
        "全仓止损（%）", min_value=0.0, value=3.0, step=0.1
    )
    take_profit_cols = st.columns([1.2, 1, 1, 1])
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
    buy_cost_pct = take_profit_cols[2].number_input(
        "买入成本（%）", min_value=0.0, value=0.03, step=0.01, format="%.4f"
    )
    sell_cost_pct = take_profit_cols[3].number_input(
        "卖出成本（%）", min_value=0.0, value=0.13, step=0.01, format="%.4f"
    )
    slippage_cols = st.columns(2)
    buy_slippage_pct = slippage_cols[0].number_input(
        "买入滑点（%）", min_value=0.0, value=0.0, step=0.01, format="%.4f"
    )
    sell_slippage_pct = slippage_cols[1].number_input(
        "卖出滑点（%）", min_value=0.0, value=0.0, step=0.01, format="%.4f"
    )

section_header(ADVANCED_SECTION_TITLE, ADVANCED_SECTION_CAPTION)
with st.expander("信号过滤", expanded=False):
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
    board_ma_filter_cols = st.columns(4)
    enable_board_ma_filter = board_ma_filter_cols[0].checkbox(
        "启用板块均线占比过滤", value=False
    )
    board_ma_filter_line = board_ma_filter_cols[1].selectbox(
        "占比线",
        options=["20", "50"],
        index=["20", "50"].index(str(factor_control_default("board_ma_filter_line"))),
        format_func=lambda value: str(BOARD_MA_LINE_LABELS.get(str(value), value)),
        disabled=not enable_board_ma_filter,
    )
    board_ma_filter_operator = board_ma_filter_cols[2].selectbox(
        "比较方向",
        options=[">=", "<="],
        index=[">=", "<="].index(
            str(factor_control_default("board_ma_filter_operator"))
        ),
        format_func=lambda value: BOARD_MA_OPERATOR_LABELS.get(str(value), str(value)),
        disabled=not enable_board_ma_filter,
    )
    board_ma_filter_threshold = board_ma_filter_cols[3].number_input(
        "板块均线过滤阈值（%）",
        min_value=0.0,
        max_value=100.0,
        value=float(factor_control_default("board_ma_filter_threshold")),
        step=1.0,
        disabled=not enable_board_ma_filter,
    )
    imported_filter_registry = load_indicator_registry_preview()
    imported_filter_options, imported_filter_columns, imported_filter_labels = (
        imported_indicator_filter_options(imported_filter_registry)
    )
    enable_imported_indicator_filter = st.checkbox(
        "启用导入指标过滤", value=False, key="enable_imported_indicator_filter"
    )
    for rule_index in range(1, 4):
        imported_filter_cols = st.columns([0.8, 1.2, 1.2, 1, 1])
        imported_filter_rule_enabled = imported_filter_cols[0].checkbox(
            f"规则 {rule_index}",
            value=enable_imported_indicator_filter if rule_index == 1 else False,
            disabled=not enable_imported_indicator_filter,
            key=f"imported_filter_rule_enabled_{rule_index}",
        )
        imported_indicator_filter_key = imported_filter_cols[1].selectbox(
            f"导入指标 {rule_index}",
            options=imported_filter_options,
            index=0,
            format_func=lambda value: (
                "请选择指标"
                if str(value) == ""
                else imported_filter_labels.get(str(value), str(value))
            ),
            disabled=(not imported_filter_rule_enabled)
            or len(imported_filter_options) <= 1,
            key=f"imported_filter_key_{rule_index}",
        )
        selected_imported_columns = imported_filter_columns.get(
            str(imported_indicator_filter_key), []
        )
        imported_filter_cols[2].selectbox(
            f"输出列 {rule_index}",
            options=selected_imported_columns or [""],
            index=0,
            format_func=lambda value: (
                "请选择输出列" if str(value) == "" else str(value)
            ),
            disabled=(not imported_filter_rule_enabled)
            or not selected_imported_columns,
            key=f"imported_filter_column_{rule_index}",
        )
        imported_filter_cols[3].selectbox(
            f"比较方向 {rule_index}",
            options=[">=", "<="],
            index=[">=", "<="].index(
                str(factor_control_default("imported_indicator_filter_operator"))
            ),
            format_func=lambda value: IMPORTED_INDICATOR_OPERATOR_LABELS.get(
                str(value), str(value)
            ),
            disabled=not imported_filter_rule_enabled,
            key=f"imported_filter_operator_{rule_index}",
        )
        imported_filter_cols[4].number_input(
            f"阈值 {rule_index}",
            value=float(factor_control_default("imported_indicator_filter_threshold")),
            step=0.1,
            disabled=not imported_filter_rule_enabled,
            key=f"imported_filter_threshold_{rule_index}",
        )

with st.expander("高级离场", expanded=False):
    enable_profit_drawdown_exit = st.checkbox("启用盈利回撤止盈（整笔）", value=False)
    drawdown_cols = st.columns([1.2, 1, 1])
    profit_drawdown_pct = drawdown_cols[0].number_input(
        "盈利回撤（%）",
        min_value=0.0,
        value=40.0,
        step=1.0,
        disabled=not enable_profit_drawdown_exit,
    )
    min_profit_to_activate_profit_drawdown_pct = drawdown_cols[1].number_input(
        "激活浮盈（%）",
        min_value=0.0,
        value=float(
            factor_control_default("min_profit_to_activate_profit_drawdown_pct")
        ),
        step=0.1,
        disabled=not enable_profit_drawdown_exit,
        key="min_profit_to_activate_profit_drawdown_pct",
    )
    board_ma_exit_cols = st.columns(4)
    enable_board_ma_exit = board_ma_exit_cols[0].checkbox(
        "启用板块均线离场（整笔）", value=False
    )
    board_ma_exit_line = board_ma_exit_cols[1].selectbox(
        "离场占比线",
        options=["20", "50"],
        index=["20", "50"].index(str(factor_control_default("board_ma_exit_line"))),
        format_func=lambda value: str(BOARD_MA_LINE_LABELS.get(str(value), value)),
        disabled=not enable_board_ma_exit,
    )
    board_ma_exit_operator = board_ma_exit_cols[2].selectbox(
        "离场比较方向",
        options=[">=", "<="],
        index=[">=", "<="].index(str(factor_control_default("board_ma_exit_operator"))),
        format_func=lambda value: BOARD_MA_OPERATOR_LABELS.get(str(value), str(value)),
        disabled=not enable_board_ma_exit,
    )
    board_ma_exit_threshold = board_ma_exit_cols[3].number_input(
        "板块均线离场阈值（%）",
        min_value=0.0,
        max_value=100.0,
        value=float(factor_control_default("board_ma_exit_threshold")),
        step=1.0,
        disabled=not enable_board_ma_exit,
    )
    imported_exit_registry = load_indicator_registry_preview()
    imported_exit_options, imported_exit_columns, imported_exit_labels = (
        imported_indicator_exit_options(imported_exit_registry)
    )
    enable_imported_indicator_exit = st.checkbox(
        "启用导入指标离场", value=False, key="enable_imported_indicator_exit"
    )
    for rule_index in range(1, 4):
        imported_exit_cols = st.columns([0.8, 1.2, 1.2, 1, 1])
        imported_exit_rule_enabled = imported_exit_cols[0].checkbox(
            f"离场规则 {rule_index}",
            value=enable_imported_indicator_exit if rule_index == 1 else False,
            disabled=not enable_imported_indicator_exit,
            key=f"imported_exit_rule_enabled_{rule_index}",
        )
        imported_indicator_exit_key = imported_exit_cols[1].selectbox(
            f"离场导入指标 {rule_index}",
            options=imported_exit_options,
            index=0,
            format_func=lambda value: (
                "请选择指标"
                if str(value) == ""
                else imported_exit_labels.get(str(value), str(value))
            ),
            disabled=(not imported_exit_rule_enabled)
            or len(imported_exit_options) <= 1,
            key=f"imported_exit_key_{rule_index}",
        )
        selected_imported_exit_columns = imported_exit_columns.get(
            str(imported_indicator_exit_key), []
        )
        imported_exit_cols[2].selectbox(
            f"离场输出列 {rule_index}",
            options=selected_imported_exit_columns or [""],
            index=0,
            format_func=lambda value: (
                "请选择输出列" if str(value) == "" else str(value)
            ),
            disabled=(not imported_exit_rule_enabled)
            or not selected_imported_exit_columns,
            key=f"imported_exit_column_{rule_index}",
        )
        imported_exit_cols[3].selectbox(
            f"离场比较方向 {rule_index}",
            options=[">=", "<="],
            index=[">=", "<="].index(
                str(factor_control_default("imported_indicator_exit_operator"))
            ),
            format_func=lambda value: IMPORTED_INDICATOR_OPERATOR_LABELS.get(
                str(value), str(value)
            ),
            disabled=not imported_exit_rule_enabled,
            key=f"imported_exit_operator_{rule_index}",
        )
        imported_exit_cols[4].number_input(
            f"离场阈值 {rule_index}",
            value=float(factor_control_default("imported_indicator_exit_threshold")),
            step=0.1,
            disabled=not imported_exit_rule_enabled,
            key=f"imported_exit_threshold_{rule_index}",
        )
    ma_exit_cols = st.columns(2)
    enable_ma_exit = ma_exit_cols[0].checkbox("启用均线离场（整笔）", value=False)
    exit_ma_period = ma_exit_cols[1].number_input(
        "离场均线周期", min_value=1, value=10, step=1, disabled=not enable_ma_exit
    )
    atr_exit_cols = st.columns([1.1, 1, 1, 1])
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
    min_profit_to_activate_atr_trailing_pct = atr_exit_cols[3].number_input(
        "激活浮盈（%）",
        min_value=0.0,
        value=float(factor_control_default("min_profit_to_activate_atr_trailing_pct")),
        step=0.1,
        disabled=not enable_atr_trailing_exit,
        key="min_profit_to_activate_atr_trailing_pct",
    )
    ma_exit_batches = st.number_input(
        "均线离场分批数",
        min_value=2,
        max_value=3,
        value=2,
        step=1,
        disabled=not enable_ma_exit,
    )

with st.expander("分批止盈", expanded=False):
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
    partial_indicator_registry = load_indicator_registry_preview()
    (
        partial_indicator_options,
        partial_indicator_columns,
        partial_indicator_labels,
    ) = imported_indicator_exit_options(partial_indicator_registry)
    batch_tabs = st.tabs([f"第 {i} 批" for i in range(1, int(partial_exit_count) + 1)])
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
            mode_options = [
                "fixed_tp",
                "ma_exit",
                "profit_drawdown",
                "atr_trailing",
                "indicator_threshold",
            ]
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
                options=mode_options,
                index=mode_options.index(mode_default),
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
            indicator_selector_cols = st.columns([1.3, 1.4, 1, 1])
            partial_indicator_key = indicator_selector_cols[0].selectbox(
                f"导入指标（第{i}批）",
                options=partial_indicator_options,
                index=0,
                format_func=lambda value: (
                    "请选择指标"
                    if str(value) == ""
                    else partial_indicator_labels.get(str(value), str(value))
                ),
                disabled=(not partial_exit_enabled) or mode != "indicator_threshold",
                key=f"p_indicator_key_{i}",
            )
            selected_partial_indicator_columns = partial_indicator_columns.get(
                str(partial_indicator_key), []
            )
            indicator_selector_cols[1].selectbox(
                f"输出列（第{i}批）",
                options=selected_partial_indicator_columns or [""],
                index=0,
                format_func=lambda value: (
                    "请选择输出列" if str(value) == "" else str(value)
                ),
                disabled=(not partial_exit_enabled)
                or mode != "indicator_threshold"
                or not selected_partial_indicator_columns,
                key=f"p_indicator_column_{i}",
            )
            indicator_selector_cols[2].selectbox(
                f"比较方向（第{i}批）",
                options=[">=", "<="],
                index=0,
                format_func=lambda value: IMPORTED_INDICATOR_OPERATOR_LABELS.get(
                    str(value), str(value)
                ),
                disabled=(not partial_exit_enabled) or mode != "indicator_threshold",
                key=f"p_indicator_operator_{i}",
            )
            indicator_selector_cols[3].number_input(
                f"阈值（第{i}批）",
                value=0.0,
                step=0.1,
                disabled=(not partial_exit_enabled) or mode != "indicator_threshold",
                key=f"p_indicator_threshold_{i}",
            )
            partial_rule_inputs.append(
                {
                    "enabled": bool(partial_exit_enabled),
                    "weight_pct": float(weight_pct),
                    "mode": mode,
                    "priority": int(priority),
                    "target_profit_pct": float(tp) if mode == "fixed_tp" else None,
                    "ma_period": int(ma) if mode == "ma_exit" else None,
                    "drawdown_pct": float(dd) if mode == "profit_drawdown" else None,
                    "min_profit_to_activate_drawdown": float(mpa)
                    if mode == "profit_drawdown"
                    else None,
                    "atr_period": int(atr_period) if mode == "atr_trailing" else None,
                    "atr_multiplier": float(atr_multiplier)
                    if mode == "atr_trailing"
                    else None,
                    **(
                        build_partial_indicator_rule(i)
                        if mode == "indicator_threshold"
                        else {
                            "indicator_key": None,
                            "indicator_column": None,
                            "indicator_operator": None,
                            "indicator_threshold": None,
                        }
                    ),
                }
            )

with st.expander("参数扫描与字段映射", expanded=False):
    st.caption("适合做参数边界探索；建议先用少量组合验证，再扩大扫描范围。")
    st.caption("当分批止盈开启时，可扫描“第X批目标收益/均线周期/回撤比例/激活浮盈”。")
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
    st.markdown("**字段映射（可选）**")
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
    uploaded_file_bytes, uploaded_file_name, normalized_input_file_path = (
        _resolve_uploaded_file_inputs(uploaded_market_file, input_file_path)
    )

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
    imported_filter_rules = build_imported_indicator_rules(
        prefix="imported_filter",
        count=3,
        default_enabled=bool(enable_imported_indicator_filter),
    )
    imported_exit_rules = build_imported_indicator_rules(
        prefix="imported_exit",
        count=3,
        default_enabled=bool(enable_imported_indicator_exit),
    )
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
        enable_board_ma_filter=bool(enable_board_ma_filter),
        board_ma_filter_line=str(board_ma_filter_line),
        board_ma_filter_operator=str(board_ma_filter_operator),
        board_ma_filter_threshold=float(board_ma_filter_threshold),
        enable_imported_indicator_filter=bool(enable_imported_indicator_filter),
        imported_indicator_filter_key=str(
            st.session_state.get("imported_filter_key_1", "")
        ),
        imported_indicator_filter_column=str(
            st.session_state.get("imported_filter_column_1", "")
        ),
        imported_indicator_filter_operator=str(
            st.session_state.get(
                "imported_filter_operator_1",
                factor_control_default("imported_indicator_filter_operator"),
            )
        ),
        imported_indicator_filter_threshold=float(
            st.session_state.get(
                "imported_filter_threshold_1",
                factor_control_default("imported_indicator_filter_threshold"),
            )
        ),
        imported_indicator_filters=imported_filter_rules,
        time_stop_days=int(time_stop_days),
        time_stop_target_pct=float(time_stop_target_pct),
        stop_loss_pct=float(stop_loss_pct),
        take_profit_pct=float(take_profit_pct),
        enable_take_profit=bool(enable_take_profit),
        enable_profit_drawdown_exit=bool(enable_profit_drawdown_exit),
        profit_drawdown_pct=float(profit_drawdown_pct),
        min_profit_to_activate_profit_drawdown_pct=float(
            min_profit_to_activate_profit_drawdown_pct
        ),
        enable_board_ma_exit=bool(enable_board_ma_exit),
        board_ma_exit_line=str(board_ma_exit_line),
        board_ma_exit_operator=str(board_ma_exit_operator),
        board_ma_exit_threshold=float(board_ma_exit_threshold),
        enable_imported_indicator_exit=bool(enable_imported_indicator_exit),
        imported_indicator_exit_key=str(
            st.session_state.get("imported_exit_key_1", "")
        ),
        imported_indicator_exit_column=str(
            st.session_state.get("imported_exit_column_1", "")
        ),
        imported_indicator_exit_operator=str(
            st.session_state.get(
                "imported_exit_operator_1",
                factor_control_default("imported_indicator_exit_operator"),
            )
        ),
        imported_indicator_exit_threshold=float(
            st.session_state.get(
                "imported_exit_threshold_1",
                factor_control_default("imported_indicator_exit_threshold"),
            )
        ),
        imported_indicator_exits=imported_exit_rules,
        enable_ma_exit=bool(enable_ma_exit),
        exit_ma_period=int(exit_ma_period),
        enable_atr_trailing_exit=bool(enable_atr_trailing_exit),
        atr_trailing_period=int(atr_trailing_period),
        atr_trailing_multiplier=float(atr_trailing_multiplier),
        min_profit_to_activate_atr_trailing_pct=float(
            min_profit_to_activate_atr_trailing_pct
        ),
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

    is_batch_per_stock_mode = batch_backtest_mode_label == "逐股独立回测（批量）"
    if is_batch_per_stock_mode:
        if params.entry_factor not in {"gap", "trend_breakout"}:
            errors.append("逐股独立回测当前仅支持 gap 与 trend_breakout。")
        if len(params.stock_codes) < 2:
            errors.append("逐股独立回测请在股票池至少输入 2 只股票代码。")

    if errors:
        st.error("参数校验失败")
        for error in errors:
            st.error(error)
    else:
        try:
            with st.spinner("正在运行回测，请稍候..."):
                indicator_keys: tuple[str, ...] = tuple(
                    dict.fromkeys(
                        key
                        for key in (
                            "board_ma"
                            if params.enable_board_ma_filter
                            or params.enable_board_ma_exit
                            else "",
                            params.imported_indicator_filter_key if False else "",
                        )
                        if str(key).strip()
                    )
                )
                extra_indicator_keys = tuple(
                    dict.fromkeys(
                        [
                            rule.indicator_key
                            for rule in (
                                *params.effective_imported_indicator_filters,
                                *params.effective_imported_indicator_exits,
                            )
                            if rule.indicator_key.strip()
                        ]
                    )
                )
                partial_indicator_keys = params.partial_exit_indicator_keys
                indicator_keys = tuple(
                    dict.fromkeys(
                        [
                            *indicator_keys,
                            *extra_indicator_keys,
                            *partial_indicator_keys,
                        ]
                    )
                )
                all_data = load_market_data_cached_v2(
                    source_type=params.data_source_type,
                    start_date=params.start_date,
                    end_date=params.end_date,
                    stock_codes=tuple(params.stock_codes),
                    table_name=params.table_name,
                    column_override_items=tuple(
                        sorted(params.column_overrides.items())
                    ),
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
                    indicator_keys=indicator_keys,
                )
                result_bundle = run_backtest(
                    all_data,
                    params,
                    batch_mode="per_stock" if is_batch_per_stock_mode else "combined",
                )
                detail_df = result_bundle.detail_df
                signal_trace_df = result_bundle.signal_trace_df
                rejected_signal_df = result_bundle.rejected_signal_df
                daily_df = result_bundle.daily_df
                equity_df = result_bundle.equity_df
                stats = result_bundle.stats
                scan_df = result_bundle.scan_df
                best_scan_overrides = result_bundle.best_scan_overrides
                per_stock_stats_df = result_bundle.per_stock_stats_df
                excel_bytes = export_to_excel_bytes(
                    detail_df, daily_df, equity_df, scan_df=scan_df
                )
                trade_behavior_df = result_bundle.trade_behavior_df
                drawdown_episodes_df = result_bundle.drawdown_episodes_df
                drawdown_contributors_df = result_bundle.drawdown_contributors_df
                anomaly_queue_df = result_bundle.anomaly_queue_df
            st.success("回测完成")
            st.session_state["signal_trace_df"] = signal_trace_df
            st.session_state["rejected_signal_df"] = rejected_signal_df
            store_backtest_result_state(
                detail_df=detail_df,
                daily_df=daily_df,
                equity_df=equity_df,
                trade_behavior_df=trade_behavior_df,
                drawdown_episodes_df=drawdown_episodes_df,
                drawdown_contributors_df=drawdown_contributors_df,
                anomaly_queue_df=anomaly_queue_df,
                stats=stats,
                scan_df=scan_df,
                scan_metric=params.scan_config.metric,
                scan_axis_fields=[axis.field_name for axis in params.scan_config.axes],
                best_scan_overrides=best_scan_overrides,
                excel_bytes=excel_bytes,
                download_name=build_download_name(params.start_date, params.end_date),
                per_stock_stats_df=per_stock_stats_df,
                batch_backtest_mode=result_bundle.batch_backtest_mode,
                result_params_snapshot=build_result_params_snapshot(params),
            )
        except Exception as exc:
            st.error(f"回测失败：{exc}")

result_state = load_backtest_result_state()
detail_df = cast(pd.DataFrame, result_state["detail_df"])
signal_trace_df = cast(pd.DataFrame, result_state["signal_trace_df"])
rejected_signal_df = cast(pd.DataFrame, result_state["rejected_signal_df"])
daily_df = cast(pd.DataFrame, result_state["daily_df"])
equity_df = cast(pd.DataFrame, result_state["equity_df"])
trade_behavior_df = cast(pd.DataFrame, result_state["trade_behavior_df"])
drawdown_episodes_df = cast(pd.DataFrame, result_state["drawdown_episodes_df"])
drawdown_contributors_df = cast(pd.DataFrame, result_state["drawdown_contributors_df"])
anomaly_queue_df = cast(pd.DataFrame, result_state["anomaly_queue_df"])
stats = cast(dict[str, Any], result_state["stats"])
scan_df = cast(pd.DataFrame, result_state["scan_df"])
scan_metric = str(result_state["scan_metric"])
scan_axis_fields = cast(list[str], result_state["scan_axis_fields"])
best_scan_overrides = cast(dict[str, Any], result_state["best_scan_overrides"])
per_stock_stats_df = cast(pd.DataFrame, result_state["per_stock_stats_df"])
batch_backtest_mode = str(result_state["batch_backtest_mode"])

if isinstance(detail_df, pd.DataFrame) and "excel_bytes" in st.session_state:
    tab_names = build_result_tab_names(
        isinstance(scan_df, pd.DataFrame) and not scan_df.empty
    )
    tabs = st.tabs(tab_names)
    tab_summary, tab_curve, tab_diagnostics, tab_details = tabs[:4]
    with tab_summary:
        section_header("绩效总览", "先看关键结果，再决定是否继续展开明细或参数扫描。")
        if (
            batch_backtest_mode == "per_stock"
            and isinstance(per_stock_stats_df, pd.DataFrame)
            and not per_stock_stats_df.empty
        ):
            st.markdown("**逐股独立回测汇总**")
            dataframe_stretch(
                format_per_stock_stats_for_display(
                    per_stock_stats_df.sort_values("total_return_pct", ascending=False)
                ),
                hide_index=True,
                height=260,
            )
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("总收益率", format_percent(float(stats.get("total_return_pct", 0.0))))
        c2.metric(
            "胜率", format_percent(float(stats.get("strategy_win_rate_pct", 0.0)))
        )
        c3.metric("最大回撤", format_percent(float(stats.get("max_drawdown_pct", 0.0))))
        c4.metric("交易笔数", f"{int(stats.get('executed_trades', len(detail_df))):,}")
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
            symbol_name_map = load_symbol_name_map()
            batch_stock_code_series = chart_df.get("batch_stock_code")
            has_batch_stock_code = bool(
                isinstance(batch_stock_code_series, pd.Series)
                and batch_stock_code_series.notna().to_numpy().any()
            )
            use_grouped_equity_chart = bool(
                batch_backtest_mode == "per_stock"
                and "batch_stock_code" in chart_df.columns
                and has_batch_stock_code
            )
            if use_grouped_equity_chart:
                chart_df["batch_stock_display"] = chart_df["batch_stock_code"].map(
                    lambda value: build_display_symbol_label(value, symbol_name_map)
                )
                chart_df = chart_df.sort_values(["batch_stock_display", "date"])
                fig = px.line(
                    chart_df,
                    x="date",
                    y="net_value",
                    color="batch_stock_display",
                    line_group="batch_stock_display",
                    labels={
                        "date": "日期",
                        "net_value": "净值",
                        "batch_stock_display": "标的",
                    },
                )
                fig.update_traces(mode="lines", line=dict(width=1.8))
                fig.update_layout(
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="left",
                        x=0,
                        title_text="标的",
                    )
                )
            else:
                fig = px.line(chart_df, x="date", y="net_value")
            fig.update_layout(margin=dict(l=0, r=0, t=16, b=0), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                "逐股独立回测时，图例会按标的区分各条净值曲线；下表保留原始净值序列，适合与图形交叉核对。"
                if use_grouped_equity_chart
                else "下表保留原始净值序列，适合与图形交叉核对。"
            )
            dataframe_stretch(
                format_equity_for_display(equity_df),
                hide_index=True,
                column_config=build_equity_column_config(),
                height=260,
            )
        else:
            st.info("暂无资金曲线数据")

    with tab_diagnostics:
        section_header(
            "交易诊断", "将交易行为、回撤诊断与异常队列集中展示，便于排查与复盘。"
        )
        symbol_name_map = load_symbol_name_map()
        if isinstance(trade_behavior_df, pd.DataFrame) and not trade_behavior_df.empty:
            st.markdown("**交易行为总览**")
            behavior_metrics = trade_behavior_df.iloc[0]
            behavior_cols = st.columns(6)
            behavior_cols[0].metric(
                "平均最大有利波动",
                f"{float(behavior_metrics.get('avg_mfe_pct', 0.0)):.2f}%",
            )
            behavior_cols[1].metric(
                "平均最大不利波动",
                f"{float(behavior_metrics.get('avg_mae_pct', 0.0)):.2f}%",
            )
            behavior_cols[2].metric(
                "平均利润回吐",
                f"{float(behavior_metrics.get('avg_give_back_pct', 0.0)):.2f}%",
            )
            behavior_cols[3].metric(
                "平均 MFE 兑现率",
                f"{float(behavior_metrics.get('avg_mfe_capture_pct', 0.0)):.2f}%",
            )
            behavior_cols[4].metric(
                "触发成交占比",
                f"{float(behavior_metrics.get('trigger_fill_share_pct', 0.0)):.2f}%",
            )
            behavior_cols[5].metric(
                "多批成交占比",
                f"{float(behavior_metrics.get('multi_fill_trade_share_pct', 0.0)):.2f}%",
            )
            dataframe_stretch(
                format_trade_behavior_for_display(trade_behavior_df),
                hide_index=True,
                column_config=build_trade_behavior_column_config(),
                height=120,
            )
        if (
            isinstance(drawdown_episodes_df, pd.DataFrame)
            and not drawdown_episodes_df.empty
        ):
            st.markdown("**回撤诊断**")
            dd_metric_cols = st.columns(4)
            dd_metric_cols[0].metric("回撤段数", f"{len(drawdown_episodes_df)}")
            dd_metric_cols[1].metric(
                "最深回撤",
                f"{float(drawdown_episodes_df['peak_to_trough_pct'].max()):.2f}%",
            )
            dd_metric_cols[2].metric(
                "最长水下周期",
                f"{int(drawdown_episodes_df['underwater_bars'].max())}",
            )
            deepest_drawdown_row = drawdown_episodes_df.sort_values(
                ["peak_to_trough_pct", "episode_no"], ascending=[False, True]
            ).iloc[0]
            dd_metric_cols[3].metric(
                "最大回撤主导原因",
                str(deepest_drawdown_row.get("dominant_entry_reason", "") or "-"),
            )
            dd_cols = st.columns(2)
            with dd_cols[0]:
                dataframe_stretch(
                    format_drawdown_episodes_for_display(drawdown_episodes_df),
                    hide_index=True,
                    column_config=build_drawdown_episode_column_config(),
                    height=240,
                )
            if (
                isinstance(drawdown_contributors_df, pd.DataFrame)
                and not drawdown_contributors_df.empty
            ):
                with dd_cols[1]:
                    st.caption("最大回撤段内的开仓原因贡献分布")
                    dataframe_stretch(
                        format_drawdown_contributors_for_display(
                            drawdown_contributors_df
                        ),
                        hide_index=True,
                        column_config=build_drawdown_contributor_column_config(),
                        height=240,
                    )
        if isinstance(anomaly_queue_df, pd.DataFrame) and not anomaly_queue_df.empty:
            st.markdown("**异常交易队列**")
            anomaly_counts = anomaly_queue_df["anomaly_type"].value_counts()
            anomaly_metric_cols = st.columns(4)
            anomaly_metric_cols[0].metric("异常交易数", f"{len(anomaly_queue_df)}")
            anomaly_metric_cols[1].metric(
                "固定止盈复审",
                f"{int(anomaly_counts.get('fixed_tp_review', 0) or 0)}",
            )
            anomaly_metric_cols[2].metric(
                "利润回撤复审",
                f"{int(anomaly_counts.get('profit_drawdown_review', 0) or 0)}",
            )
            anomaly_metric_cols[3].metric(
                "ATR 回撤复审",
                f"{int(anomaly_counts.get('atr_trailing_review', 0) or 0)}",
            )
            st.caption(
                "异常严重度按持有周期锚定：优先看日均最大浮盈/日均最大不利波动及是否越过对应激发阈值。"
            )
            dataframe_stretch(
                format_anomaly_queue_for_display(
                    anomaly_queue_df,
                    symbol_name_map=symbol_name_map,
                ),
                hide_index=True,
                column_config=build_anomaly_queue_column_config(),
                height=280,
            )
        if (
            (not isinstance(trade_behavior_df, pd.DataFrame) or trade_behavior_df.empty)
            and (
                not isinstance(drawdown_episodes_df, pd.DataFrame)
                or drawdown_episodes_df.empty
            )
            and (
                not isinstance(anomaly_queue_df, pd.DataFrame) or anomaly_queue_df.empty
            )
        ):
            st.info("暂无诊断数据")

    with tab_details:
        result_params_snapshot = cast(
            dict[str, object], result_state["result_params_snapshot"]
        )
        imported_filter_count = int(
            result_params_snapshot.get(
                "imported_filter_count",
                sum(
                    1
                    for rule_index in range(1, 4)
                    if bool(
                        st.session_state.get(
                            f"imported_filter_rule_enabled_{rule_index}", False
                        )
                    )
                ),
            )
        )
        render_trade_explanations(
            signal_trace_df=signal_trace_df,
            rejected_signal_df=rejected_signal_df,
            detail_df=detail_df,
            stats=stats,
            section_header=section_header,
            summarize_signal_funnel=summarize_signal_funnel,
            summarize_filter_stack=summarize_filter_stack,
            summarize_trade_decision_chain=summarize_trade_decision_chain,
            dataframe_stretch=dataframe_stretch,
            format_timestamp=format_timestamp_for_display,
            format_number=format_number,
            format_percent=format_percent,
            entry_factor=str(result_params_snapshot.get("entry_factor", entry_factor)),
            use_ma_filter=bool(
                result_params_snapshot.get("use_ma_filter", use_ma_filter)
            ),
            fast_ma_period=int(
                result_params_snapshot.get("fast_ma_period", fast_ma_period)
            ),
            slow_ma_period=int(
                result_params_snapshot.get("slow_ma_period", slow_ma_period)
            ),
            enable_atr_filter=bool(
                result_params_snapshot.get("enable_atr_filter", enable_atr_filter)
            ),
            min_atr_filter_pct=float(
                result_params_snapshot.get("min_atr_filter_pct", min_atr_filter_pct)
            ),
            max_atr_filter_pct=float(
                result_params_snapshot.get("max_atr_filter_pct", max_atr_filter_pct)
            ),
            enable_board_ma_filter=bool(
                result_params_snapshot.get(
                    "enable_board_ma_filter", enable_board_ma_filter
                )
            ),
            board_ma_filter_line=str(
                result_params_snapshot.get("board_ma_filter_line", board_ma_filter_line)
            ),
            board_ma_filter_operator=str(
                result_params_snapshot.get(
                    "board_ma_filter_operator", board_ma_filter_operator
                )
            ),
            board_ma_filter_threshold=float(
                result_params_snapshot.get(
                    "board_ma_filter_threshold", board_ma_filter_threshold
                )
            ),
            imported_filter_count=imported_filter_count,
        )

        if isinstance(detail_df, pd.DataFrame) and not detail_df.empty:
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

    if len(tabs) == 5:
        with tabs[4]:
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
    render_results_empty_state(section_header)
