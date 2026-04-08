from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import cast

import pandas as pd
from streamlit.testing.v1 import AppTest


def test_app_exposes_new_strategy_modes_and_intraday_timeframe_support() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    entry_factor = app.selectbox(key="entry_factor")
    assert "连续K线追势" in entry_factor.options
    assert "连续K线加速追势" in entry_factor.options
    assert "早盘冲高高位横盘突破" in entry_factor.options

    timeframe = app.selectbox(key="timeframe")
    assert timeframe.label == "周期"
    assert timeframe.options == ["1d", "30m", "15m", "5m"]

    update_timeframe = app.multiselect(key="offline_update_timeframe")
    assert update_timeframe.options == ["1d", "30m", "15m", "5m"]
    assert update_timeframe.value == ["1d"]

    update_provider_1d = app.selectbox(key="offline_update_provider_1d")
    assert update_provider_1d.options == ["AKShare 在线", "通达信 TDX"]
    assert update_provider_1d.value == "akshare"

    app.multiselect(key="offline_update_timeframe").set_value(["1d", "30m", "15m"])
    app.run()
    update_provider_30m = app.selectbox(key="offline_update_provider_30m")
    update_provider_15m = app.selectbox(key="offline_update_provider_15m")
    assert update_provider_30m.options == ["AKShare 在线", "通达信 TDX"]
    assert update_provider_15m.options == ["AKShare 在线", "通达信 TDX"]

    captions = [caption.value for caption in app.caption]
    assert (
        "1d 为常规日线；early_surge_high_base 使用 30m 形态并自动切换到 5m 执行。"
        in captions
    )
    assert (
        "当前支持按周期分别选择 AKShare / TDX 更新源；当前更新链路已覆盖 1d / 30m / 15m / 5m。"
        in captions
    )


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
    for option in [
        "ATR过滤周期",
        "最小ATR波动过滤",
        "最大ATR波动过滤",
        "ATR跟踪周期",
        "ATR跟踪倍数",
    ]:
        assert option in scan_axis_1.options
        assert option in scan_axis_2.options


def test_app_exposes_board_ma_filter_and_exit_controls() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    checkbox_labels = [widget.label for widget in app.checkbox]
    number_input_labels = [widget.label for widget in app.number_input]
    selectbox_labels = [widget.label for widget in app.selectbox]

    assert "启用板块均线占比过滤" in checkbox_labels
    assert "启用板块均线离场（整笔）" in checkbox_labels
    assert "板块均线过滤阈值（%）" in number_input_labels
    assert "板块均线离场阈值（%）" in number_input_labels
    assert "占比线" in selectbox_labels
    assert "离场占比线" in selectbox_labels
    assert "比较方向" in selectbox_labels
    assert "离场比较方向" in selectbox_labels


def test_app_exposes_generic_imported_indicator_filter_controls() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    checkbox_labels = [widget.label for widget in app.checkbox]
    selectbox_labels = [widget.label for widget in app.selectbox]
    number_input_labels = [widget.label for widget in app.number_input]

    assert "启用导入指标过滤" in checkbox_labels
    assert any(label.startswith("导入指标 ") for label in selectbox_labels)
    assert any(label.startswith("输出列 ") for label in selectbox_labels)
    assert any(label.startswith("比较方向 ") for label in selectbox_labels)
    assert any(label.startswith("阈值 ") for label in number_input_labels)


def test_app_exposes_generic_imported_indicator_exit_controls() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    checkbox_labels = [widget.label for widget in app.checkbox]
    selectbox_labels = [widget.label for widget in app.selectbox]
    number_input_labels = [widget.label for widget in app.number_input]

    assert "启用导入指标离场" in checkbox_labels
    assert any(label.startswith("离场导入指标 ") for label in selectbox_labels)
    assert any(label.startswith("离场输出列 ") for label in selectbox_labels)
    assert any(label.startswith("离场比较方向 ") for label in selectbox_labels)
    assert any(label.startswith("离场阈值 ") for label in number_input_labels)


def test_app_exposes_multiple_imported_indicator_rule_rows() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    checkbox_labels = [widget.label for widget in app.checkbox]
    assert "规则 1" in checkbox_labels
    assert "规则 2" in checkbox_labels
    assert "规则 3" in checkbox_labels
    assert "离场规则 1" in checkbox_labels
    assert "启用板块均线离场（整笔）" in checkbox_labels


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


def test_app_exposes_eshb_controls_and_scan_fields() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.selectbox(key="entry_factor").set_value("early_surge_high_base")
    app.run()

    direction = app.selectbox(key="direction_label")
    assert direction.options == ["早盘冲高高位横盘突破"]

    number_input_labels = [widget.label for widget in app.number_input]
    assert "早盘观察窗口K数" in number_input_labels
    assert "高位横盘最少K数" in number_input_labels
    assert "突破量能倍数下限" in number_input_labels
    assert "突破触发缓冲（%）" in number_input_labels

    scan_axis_1 = app.selectbox(key="scan_axis_1_field")
    assert "早盘观察窗口K数" in scan_axis_1.options
    assert "突破触发缓冲" in scan_axis_1.options


def test_app_exposes_backtest_date_presets_and_applies_shortcut() -> None:
    today = pd.Timestamp.today().date()
    ytd_start = today.replace(month=1, day=1)
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
    assert "YTD" in button_labels

    app.button(key="backtest_preset_10y").click()
    app.run()

    assert app.date_input(key="backtest_start_date").value == expected_start
    assert app.date_input(key="backtest_end_date").value == today

    app.button(key="backtest_preset_ytd").click()
    app.run()

    assert app.date_input(key="backtest_start_date").value == ytd_start
    assert app.date_input(key="backtest_end_date").value == today


def test_app_exposes_major_index_presets_and_can_fill_stock_scope() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    button_labels = [widget.label for widget in app.button]
    assert "沪深300" in button_labels
    assert "创业板指" in button_labels
    assert "中证1000" in button_labels
    assert "中证500" in button_labels
    assert "上证50" in button_labels

    app.button(key="stock_scope_preset_hs300").click()
    app.run()
    assert app.text_area(key="stock_scope_text").value == "000300.SH"

    app.button(key="stock_scope_preset_cyb").click()
    app.run()
    assert app.text_area(key="stock_scope_text").value == "000300.SH,399006.SZ"

    app.button(key="stock_scope_preset_hs300").click()
    app.run()
    assert app.text_area(key="stock_scope_text").value == "000300.SH,399006.SZ"


def test_app_exposes_data_prep_shortcuts_and_applies_to_update_inputs() -> None:
    today = pd.Timestamp.today().date()
    ytd_start = today.replace(month=1, day=1)
    try:
        expected_start = today.replace(year=today.year - 10)
    except ValueError:
        expected_start = today.replace(year=today.year - 10, month=2, day=28)

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    app.button(key="offline_update_symbol_preset_hs300").click()
    app.run()
    assert app.text_area(key="offline_update_symbols").value == "000300.SH"

    app.button(key="offline_update_symbol_preset_cyb").click()
    app.run()
    assert app.text_area(key="offline_update_symbols").value == "000300.SH,399006.SZ"

    app.button(key="offline_update_symbol_preset_hs300").click()
    app.run()
    assert app.text_area(key="offline_update_symbols").value == "000300.SH,399006.SZ"

    app.button(key="offline_update_preset_10y").click()
    app.run()
    assert app.date_input(key="offline_update_start").value == expected_start
    assert app.date_input(key="offline_update_end").value == today

    app.button(key="offline_update_preset_ytd").click()
    app.run()
    assert app.date_input(key="offline_update_start").value == ytd_start
    assert app.date_input(key="offline_update_end").value == today


def test_app_passes_tdx_tqcenter_path_to_update_subprocess(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    class _FakeResult:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def _fake_run(cmd, capture_output, text, env=None):
        captured["cmd"] = cmd
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["env"] = env
        return _FakeResult()

    monkeypatch.setattr("subprocess.run", _fake_run)

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    install_root = str(tmp_path / "TdxInstall")
    app.session_state["tdx_tqcenter_path"] = install_root
    app.session_state["tdx_tqcenter_path_display"] = install_root
    app.multiselect(key="offline_update_timeframe").set_value(["1d", "30m"])
    app.run()
    app.selectbox(key="offline_update_provider_1d").set_value("tdx")

    app.button(key="offline_update_submit").click()
    app.run()

    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert isinstance(captured["env"], dict)
    assert cast(dict[str, str], captured["env"])["TDX_TQCENTER_PATH"] == str(
        (tmp_path / "TdxInstall" / "PYPlugins" / "user")
    )

    cmd = cast(list[str], captured["cmd"])
    timeframe_values = [
        cmd[index + 1]
        for index, token in enumerate(cmd)
        if token == "--timeframe" and index + 1 < len(cmd)
    ]
    assert timeframe_values == ["1d", "30m"]
    provider_values = [
        cmd[index + 1]
        for index, token in enumerate(cmd)
        if token == "--provider" and index + 1 < len(cmd)
    ]
    assert provider_values == ["1d=tdx", "30m=akshare"]


def test_app_passes_tdx_tqcenter_path_to_indicator_import_subprocess(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    class _FakeResult:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def _fake_run(cmd, capture_output, text, env=None):
        captured["cmd"] = cmd
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["env"] = env
        return _FakeResult()

    monkeypatch.setattr("subprocess.run", _fake_run)

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    install_root = str(tmp_path / "TdxInstall")
    app.session_state["tdx_tqcenter_path"] = install_root
    app.session_state["tdx_tqcenter_path_display"] = install_root
    app.text_area(key="offline_update_symbols").set_value("000001.SZ")

    app.button(key="offline_indicator_submit").click()
    app.run()

    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert isinstance(captured["env"], dict)
    assert cast(dict[str, str], captured["env"])["TDX_TQCENTER_PATH"] == str(
        (tmp_path / "TdxInstall" / "PYPlugins" / "user")
    )

    cmd = cast(list[str], captured["cmd"])
    assert cmd[1].endswith("scripts/import_tdx_local_indicators.py")
    assert "--indicator" in cmd and "board_ma" in cmd


def test_app_indicator_probe_shows_manual_fallback_message(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "app.probe_local_indicator_candidates",
        lambda path: (
            [("board_ma", "板块均线")],
            "当前未发现通达信提供稳定的本地公式枚举接口，已展示系统内置支持项；若未命中，请手动输入公式名称和输出标识符。",
        ),
    )

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()
    app.session_state["tdx_tqcenter_path"] = str(tmp_path / "TdxInstall")
    app.button(key="offline_indicator_probe").click()
    app.run()

    infos = [info.value for info in app.info]
    assert any("未发现通达信提供稳定的本地公式枚举接口" in item for item in infos)


def test_app_passes_manual_formula_mapping_to_indicator_import(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    class _FakeResult:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def _fake_run(cmd, capture_output, text, env=None):
        captured["cmd"] = cmd
        captured["env"] = env
        return _FakeResult()

    monkeypatch.setattr("subprocess.run", _fake_run)

    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()
    app.session_state["tdx_tqcenter_path"] = str(tmp_path / "TdxInstall")
    app.session_state["tdx_tqcenter_path_display"] = str(tmp_path / "TdxInstall")
    app.text_area(key="offline_update_symbols").set_value("000001.SZ")
    app.radio(key="offline_indicator_mode").set_value("手动指定")
    app.run()
    app.text_input(key="offline_indicator_manual_key").set_value("board_ma")
    app.text_input(key="offline_indicator_formula_name").set_value("板块均线")
    app.text_area(key="offline_indicator_output_map_text").set_value(
        "board_ma_ratio_20=NOTEXT1\nboard_ma_ratio_50=NOTEXT2"
    )
    app.button(key="offline_indicator_submit").click()
    app.run()

    cmd = cast(list[str], captured["cmd"])
    assert "--formula-name" in cmd and "板块均线" in cmd
    assert "--output-map" in cmd
    output_map_arg = cmd[cmd.index("--output-map") + 1]
    assert "board_ma_ratio_20=NOTEXT1" in output_map_arg
    assert "board_ma_ratio_50=NOTEXT2" in output_map_arg


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

    assert not app.error, app.error[0].value if app.error else "unknown error"
    assert app.success[0].value == "回测完成"
    assert any(
        metric.label == "交易笔数" and str(metric.value) == "1" for metric in app.metric
    )


def _seed_result_state_for_curve_caption(
    app: AppTest,
    *,
    batch_backtest_mode: str,
    equity_df: pd.DataFrame,
) -> None:
    app.session_state["detail_df"] = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-02")],
            "sell_date": [pd.Timestamp("2024-01-03")],
            "stock_code": ["000001.SZ"],
        }
    )
    app.session_state["daily_df"] = pd.DataFrame()
    app.session_state["equity_df"] = equity_df
    app.session_state["trade_behavior_df"] = pd.DataFrame()
    app.session_state["drawdown_episodes_df"] = pd.DataFrame()
    app.session_state["drawdown_contributors_df"] = pd.DataFrame()
    app.session_state["anomaly_queue_df"] = pd.DataFrame()
    app.session_state["stats"] = {}
    app.session_state["scan_df"] = pd.DataFrame()
    app.session_state["scan_metric"] = "total_return_pct"
    app.session_state["scan_axis_fields"] = []
    app.session_state["best_scan_overrides"] = {}
    app.session_state["per_stock_stats_df"] = pd.DataFrame()
    app.session_state["batch_backtest_mode"] = batch_backtest_mode
    app.session_state["excel_bytes"] = b"dummy"
    app.session_state["download_name"] = "test.xlsx"


def test_app_curve_caption_mentions_legend_when_grouped_traces_are_available() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    _seed_result_state_for_curve_caption(
        app,
        batch_backtest_mode="per_stock",
        equity_df=pd.DataFrame(
            {
                "date": [
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-01"),
                    pd.Timestamp("2024-01-02"),
                ],
                "net_value": [1.0, 1.1, 1.0, 0.95],
                "batch_stock_code": [
                    "000001.SZ",
                    "000001.SZ",
                    "000002.SZ",
                    "000002.SZ",
                ],
            }
        ),
    )
    app.run()

    captions = [caption.value for caption in app.caption]
    assert (
        "逐股独立回测时，图例会按标的区分各条净值曲线；下表保留原始净值序列，适合与图形交叉核对。"
        in captions
    )

    plotly_proto = str(app.get("plotly_chart")[0].proto)
    assert "000001.SZ" in plotly_proto
    assert "000002.SZ" in plotly_proto
    assert 'hovermode\\":\\"x unified\\"' in plotly_proto


def test_app_curve_caption_falls_back_when_per_stock_mode_has_no_batch_labels() -> None:
    app = AppTest.from_file("app.py", default_timeout=10)
    app.run()

    _seed_result_state_for_curve_caption(
        app,
        batch_backtest_mode="per_stock",
        equity_df=pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
                "net_value": [1.0, 1.1],
            }
        ),
    )
    app.run()

    captions = [caption.value for caption in app.caption]
    assert "下表保留原始净值序列，适合与图形交叉核对。" in captions
    assert (
        "逐股独立回测时，图例会按标的区分各条净值曲线；下表保留原始净值序列，适合与图形交叉核对。"
        not in captions
    )
