from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app_module():
    spec = importlib.util.spec_from_file_location("app_display_module", ROOT / "app.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_format_detail_for_display_compacts_numeric_values() -> None:
    app = _load_app_module()

    detail = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "sell_date": ["2024-01-02"],
            "prev_close": [10.0],
            "open": [10.1234],
            "close": [10.5],
            "buy_price": [10.5],
            "sell_price": [10.6789],
            "gap_pct_vs_prev_close": [2.0],
            "net_return_pct": [1.23],
            "nav_before_trade": [1.0],
            "nav_after_trade": [1.0234],
            "volume": [1234567.0],
            "holding_days": [5.0],
            "fill_count": [2.0],
            "win_flag": [1.0],
            "entry_reason": ["gap.strict_break.up"],
            "exit_reason": ["atr_trailing: ATR 跟踪止盈触发"],
        }
    )

    record = app.format_detail_for_display(detail).to_dict("records")[0]

    assert record["前收盘价"] == "10"
    assert record["开盘价"] == "10.12"
    assert record["买入价"] == "10.5"
    assert record["卖出均价"] == "10.68"
    assert record["相对昨收跳空幅度"] == "2%"
    assert record["净收益率"] == "1.23%"
    assert record["交易前净值"] == "1"
    assert record["交易后净值"] == "1.0234"
    assert record["成交量"] == "1,234,567"
    assert record["持有天数"] == "5"
    assert record["成交批次数"] == "2"
    assert record["是否盈利"] == "是"
    assert record["开仓原因"] == "gap.strict_break.up"
    assert record["离场原因"] == "atr_trailing: ATR 跟踪止盈触发"


def test_other_display_formatters_compact_numeric_values() -> None:
    app = _load_app_module()

    summary = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "signal_count": [1234.0],
            "executed_trades": [56.0],
            "win_rate_pct": [12.34],
            "avg_net_return_pct": [1.2],
            "median_net_return_pct": [1.0],
            "avg_holding_days": [3.0],
        }
    )
    equity = pd.DataFrame(
        {"date": ["2024-01-01"], "net_value": [1.0234], "drawdown_pct": [-2.5]}
    )
    scan = pd.DataFrame(
        {
            "scan_id": [1001.0],
            "rank": [1.0],
            "signal_count": [1234.0],
            "closed_trade_candidates": [89.0],
            "executed_trades": [56.0],
            "trend_breakout_lookback": [20.0],
            "profit_risk_ratio": [1.5],
            "trade_return_volatility_pct": [3.2],
            "final_net_value": [1.0234],
            "total_return_pct": [2.3],
        }
    )
    update_log = pd.DataFrame(
        {
            "start_date": ["2024-01-01"],
            "end_date": ["2024-01-31"],
            "updated_at": ["2024-02-01 09:30:00"],
            "rows": [123456.0],
        }
    )

    summary_record = app.format_summary_for_display(summary).to_dict("records")[0]
    equity_record = app.format_equity_for_display(equity).to_dict("records")[0]
    scan_record = app.format_scan_for_display(scan).to_dict("records")[0]
    log_record = app.format_update_log_for_display(update_log).to_dict("records")[0]

    assert summary_record["信号数"] == "1,234"
    assert summary_record["实际执行交易数"] == "56"
    assert summary_record["胜率"] == "12.34%"
    assert summary_record["平均净收益率"] == "1.2%"
    assert summary_record["净收益率中位数"] == "1%"
    assert summary_record["平均持有天数"] == "3"

    assert equity_record["净值"] == "1.0234"
    assert equity_record["回撤"] == "-2.5%"

    assert scan_record["扫描编号"] == "1,001"
    assert scan_record["信号数"] == "1,234"
    assert scan_record["候选平仓交易数"] == "89"
    assert scan_record["实际执行交易数"] == "56"
    assert scan_record["趋势突破回看天数"] == "20"
    assert scan_record["收益风险比"] == "1.5"
    assert scan_record["单笔收益波动率"] == "3.2%"
    assert scan_record["最终净值"] == "1.0234"
    assert scan_record["总收益率"] == "2.3%"

    assert log_record["开始日期"] == "2024-01-01"
    assert log_record["结束日期"] == "2024-01-31"
    assert log_record["更新时间"] == "2024-02-01 09:30"
    assert log_record["更新行数"] == "123,456"


def test_format_detail_for_display_keeps_intraday_time_text() -> None:
    app = _load_app_module()

    detail = pd.DataFrame(
        {
            "date": ["2024-01-01 10:05:00"],
            "sell_date": ["2024-01-01 10:55:00"],
            "stock_code": ["000001.SZ"],
            "prev_close": [10.0],
            "prev_high": [10.2],
            "prev_low": [9.8],
            "open": [10.1],
            "close": [10.2],
            "buy_price": [10.1],
            "sell_price": [10.3],
            "gap_pct_vs_prev_close": [1.0],
            "net_return_pct": [1.0],
            "volume": [1000.0],
            "holding_days": [1],
            "fill_count": [1],
            "win_flag": [1],
            "entry_reason": ["early_surge_high_base.up"],
            "exit_reason": ["time_exit: 时间退出触发"],
        }
    )

    record = app.format_detail_for_display(detail).to_dict("records")[0]
    assert record["信号日期"] == "2024-01-01 10:05"
    assert record["卖出日期"] == "2024-01-01 10:55"


def test_review_display_formatters_compact_behavior_drawdown_and_anomaly_values() -> (
    None
):
    app = _load_app_module()

    behavior_df = pd.DataFrame(
        {
            "executed_trades": [12.0],
            "win_rate_pct": [58.33],
            "avg_net_return_pct": [1.25],
            "median_net_return_pct": [0.8],
            "avg_mfe_pct": [4.5],
            "avg_mae_pct": [-2.1],
            "avg_give_back_pct": [2.6],
            "avg_mfe_capture_pct": [43.21],
            "trigger_fill_share_pct": [25.0],
            "multi_fill_trade_share_pct": [33.33],
            "avg_profit_drawdown_ratio": [0.56],
        }
    )
    episodes_df = pd.DataFrame(
        {
            "episode_no": [1.0],
            "drawdown_start_date": ["2024-01-02"],
            "trough_date": ["2024-01-05"],
            "recovery_date": ["2024-01-10"],
            "peak_to_trough_pct": [12.34],
            "underwater_bars": [5.0],
            "trade_count": [3.0],
            "worst_trade_return_pct": [-6.2],
            "dominant_entry_reason": ["trend_breakout.up"],
            "recovered_flag": [True],
        }
    )
    contributors_df = pd.DataFrame(
        {
            "entry_reason": ["trend_breakout.up"],
            "trade_count": [3.0],
            "avg_net_return_pct": [-2.1],
            "total_net_return_pct": [-6.3],
            "avg_mae_pct": [-4.0],
            "avg_mfe_pct": [1.2],
        }
    )
    anomaly_df = pd.DataFrame(
        {
            "anomaly_type": ["missed_fixed_tp"],
            "severity_score": [8.2],
            "trade_no": [7.0],
            "date": ["2024-01-03 10:30:00"],
            "stock_code": ["000001.SZ"],
            "holding_days": [4.0],
            "activation_threshold_pct": [5.0],
            "threshold_excess_pct": [4.7],
            "holding_anchor_mfe_pct": [2.43],
            "holding_anchor_mae_pct": [0.3],
            "give_back_pct": [8.2],
            "net_return_pct": [1.5],
            "entry_reason": ["trend_breakout.up"],
            "exit_reason": ["profit_drawdown: 利润回撤触发"],
            "anomaly_note": ["最大浮盈 9.70%，最终仅实现 1.50%"],
        }
    )

    behavior_record = app.format_trade_behavior_for_display(behavior_df).to_dict(
        "records"
    )[0]
    episode_record = app.format_drawdown_episodes_for_display(episodes_df).to_dict(
        "records"
    )[0]
    contributor_record = app.format_drawdown_contributors_for_display(
        contributors_df
    ).to_dict("records")[0]
    anomaly_record = app.format_anomaly_queue_for_display(anomaly_df).to_dict(
        "records"
    )[0]

    assert behavior_record["交易笔数"] == "12"
    assert behavior_record["平均利润回吐"] == "2.6%"
    assert behavior_record["平均利润回撤比"] == "0.56"
    assert episode_record["峰谷回撤"] == "12.34%"
    assert episode_record["是否恢复"] == "是"
    assert contributor_record["累计净收益率"] == "-6.3%"
    assert anomaly_record["严重度"] == "8.2"
    assert anomaly_record["激发阈值"] == "5%"
    assert anomaly_record["日均最大浮盈"] == "2.43%"
    assert anomaly_record["信号日期"] == "2024-01-03 10:30"


def test_symbol_display_label_prefers_name_and_falls_back_to_code() -> None:
    app = _load_app_module()

    assert (
        app.build_display_symbol_label(
            "000300.SH",
            {"000300.SH": "沪深300"},
        )
        == "沪深300（000300.SH）"
    )
    assert app.build_display_symbol_label("399001.SZ", {"000300.SH": "沪深300"}) == "399001.SZ"


def test_format_anomaly_queue_for_display_prefers_symbol_name_when_mapping_provided() -> None:
    app = _load_app_module()

    anomaly_df = pd.DataFrame(
        {
            "date": ["2024-01-03 10:30:00", "2024-01-04 10:30:00"],
            "stock_code": ["000300.SH", "399001.SZ"],
            "anomaly_type": ["missed_fixed_tp", "missed_fixed_tp"],
            "severity_score": [8.2, 7.1],
            "trade_no": [7, 8],
            "holding_days": [4, 3],
        }
    )

    records = app.format_anomaly_queue_for_display(
        anomaly_df,
        symbol_name_map={"000300.SH": "沪深300"},
    ).to_dict("records")

    assert records[0]["股票代码"] == "沪深300（000300.SH）"
    assert records[1]["股票代码"] == "399001.SZ"
