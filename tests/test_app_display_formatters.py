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
