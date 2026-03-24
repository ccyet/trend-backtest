from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest


def test_app_exposes_new_strategy_modes_and_timeframe_socket() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    entry_factor = app.selectbox(key="entry_factor")
    assert "连续K线追势" in entry_factor.options
    assert "连续K线加速追势" in entry_factor.options

    timeframe = app.selectbox(key="timeframe")
    assert timeframe.label == "周期（预留 30m/15m 插座）"
    assert timeframe.options == ["1d", "30m", "15m"]

    captions = [caption.value for caption in app.caption]
    assert "当前回测执行按 1d 生效；30m / 15m 为后续接入预留。" in captions


def test_app_updates_direction_options_for_acceleration_mode() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.selectbox(key="entry_factor").set_value("candle_run_acceleration")
    app.run()

    direction = app.selectbox(key="direction_label")
    assert direction.options == ["连续阳线加速追涨", "连续阴线加速追空"]

    captions = [caption.value for caption in app.caption]
    assert (
        "连续K线追势基于前序连续阳线/阴线组合；加速模式额外要求实体强度逐步增强。"
        in captions
    )


def test_app_exposes_candle_run_specific_controls_and_scan_fields() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.selectbox(key="entry_factor").set_value("candle_run")
    app.run()

    number_input_labels = [widget.label for widget in app.number_input]
    assert "连续K线根数" in number_input_labels
    assert "单根最小实体幅度（%）" in number_input_labels
    assert "组合最小累计涨跌幅（%）" in number_input_labels

    direction = app.selectbox(key="direction_label")
    assert direction.options == ["连续阳线追涨", "连续阴线追空"]

    captions = [caption.value for caption in app.caption]
    assert "连续K线追势基于前序连续阳线/阴线组合，在下一根K线开盘追势。" in captions

    scan_axis_1 = app.selectbox(key="scan_axis_1_field")
    scan_axis_2 = app.selectbox(key="scan_axis_2_field")
    assert "连续K线根数" in scan_axis_1.options
    assert "单根最小实体幅度" in scan_axis_1.options
    assert "组合最小累计涨跌幅" in scan_axis_1.options
    assert "连续K线根数" in scan_axis_2.options
    assert "单根最小实体幅度" in scan_axis_2.options
    assert "组合最小累计涨跌幅" in scan_axis_2.options


def test_app_exposes_atr_filter_and_trailing_controls() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    checkbox_labels = [widget.label for widget in app.checkbox]
    number_input_labels = [widget.label for widget in app.number_input]

    assert "启用 ATR 波动率过滤" in checkbox_labels
    assert "启用 ATR 跟踪止盈（整笔）" in checkbox_labels
    assert "ATR 过滤周期" in number_input_labels
    assert "ATR 跟踪周期" in number_input_labels
    assert "ATR 倍数" in number_input_labels

    scan_axis_1 = app.selectbox(key="scan_axis_1_field")
    scan_axis_2 = app.selectbox(key="scan_axis_2_field")
    for option in ["ATR过滤周期", "最小ATR波动过滤", "最大ATR波动过滤", "ATR跟踪周期", "ATR跟踪倍数"]:
        assert option in scan_axis_1.options
        assert option in scan_axis_2.options


def test_app_preserves_candle_run_length_above_two() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.selectbox(key="entry_factor").set_value("candle_run")
    app.run()
    app.number_input(key="candle_run_length").set_value(4)
    app.run()

    assert app.number_input(key="candle_run_length").value == 4


def test_app_exposes_candle_run_acceleration_controls_and_scan_fields() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.selectbox(key="entry_factor").set_value("candle_run_acceleration")
    app.run()

    number_input_labels = [widget.label for widget in app.number_input]
    assert "连续K线根数" in number_input_labels
    assert "单根最小实体幅度（%）" in number_input_labels
    assert "组合最小累计涨跌幅（%）" in number_input_labels

    direction = app.selectbox(key="direction_label")
    assert direction.options == ["连续阳线加速追涨", "连续阴线加速追空"]

    scan_axis_1 = app.selectbox(key="scan_axis_1_field")
    scan_axis_2 = app.selectbox(key="scan_axis_2_field")
    assert "连续K线根数" in scan_axis_1.options
    assert "单根最小实体幅度" in scan_axis_1.options
    assert "组合最小累计涨跌幅" in scan_axis_1.options
    assert "连续K线根数" in scan_axis_2.options
    assert "单根最小实体幅度" in scan_axis_2.options
    assert "组合最小累计涨跌幅" in scan_axis_2.options


def test_app_preserves_candle_run_acceleration_length_above_two() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.selectbox(key="entry_factor").set_value("candle_run_acceleration")
    app.run()
    app.number_input(key="candle_run_length").set_value(5)
    app.run()

    assert app.number_input(key="candle_run_length").value == 5


def test_app_exposes_backtest_date_presets_and_applies_shortcut() -> None:
    today = pd.Timestamp.today().date()
    try:
        expected_start = today.replace(year=today.year - 10)
    except ValueError:
        expected_start = today.replace(year=today.year - 10, month=2, day=28)

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    button_labels = [widget.label for widget in app.button]
    assert "10年至今" in button_labels
    assert "7年至今" in button_labels
    assert "5年至今" in button_labels
    assert "3年至今" in button_labels

    app.button(key="backtest_preset_10y").click()
    app.run()

    assert app.date_input(key="backtest_start_date").value == expected_start
    assert app.date_input(key="backtest_end_date").value == today


def test_app_can_run_candle_run_acceleration_backtest_with_local_parquet(
    tmp_path: Path,
) -> None:
    root = tmp_path / "market" / "daily" / "qfq"
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                ]
            ),
            "stock_code": ["000001.SZ"] * 8,
            "open": [100.0, 101.0, 103.2, 104.5, 105.8, 107.0, 108.0, 109.0],
            "high": [101.2, 103.4, 104.8, 106.0, 107.4, 108.8, 109.8, 111.0],
            "low": [99.8, 100.9, 103.0, 104.3, 105.6, 106.8, 107.8, 108.8],
            "close": [101.0, 103.2, 104.5, 105.8, 107.0, 108.0, 109.0, 110.0],
            "volume": [1000.0, 1100.0, 1200.0, 1300.0, 1250.0, 1280.0, 1320.0, 1350.0],
        }
    ).to_parquet(root / "000001.SZ.parquet", index=False)

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.text_area(key="stock_scope_text").set_value("000001.SZ")
    app.date_input(key="backtest_start_date").set_value(date(2024, 1, 1))
    app.date_input(key="backtest_end_date").set_value(date(2024, 1, 8))
    app.text_input(key="local_data_root").set_value(str(tmp_path / "market" / "daily"))
    app.selectbox(key="entry_factor").set_value("candle_run_acceleration")
    app.button(key="run_backtest").click()
    app.run(timeout=10)

    assert app.success[0].value == "回测完成"
    assert any(
        metric.label == "交易笔数" and str(metric.value) == "1" for metric in app.metric
    )
