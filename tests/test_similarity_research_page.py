from __future__ import annotations

import pandas as pd

from pages.similarity_research import (
    build_top_similarity_candlestick_grid,
    format_similarity_cases_for_display,
    format_outcome_summary_for_display,
    similarity_algorithm_steps_for_display,
    similarity_feature_guide_for_display,
    resolve_similarity_symbol_value,
    resolve_similarity_config_from_settings,
)


def test_format_similarity_cases_for_display_uses_chinese_headers() -> None:
    cases = pd.DataFrame(
        {
            "window_start": [pd.Timestamp("2024-01-01")],
            "window_end": [pd.Timestamp("2024-01-10")],
            "total_similarity": [0.91],
            "main_similarity": [0.88],
            "external_similarity": [0.66],
            "similar_reason": ["方向相近"],
            "external_difference": ["成交环境接近"],
            "window_start_index": [1],
            "window_end_index": [10],
            "path_distance": [0.123456789],
            "feature_distance": [0.223456789],
            "external_distance": [0.323456789],
            "dtw_distance": [0.423456789],
            "t_plus_5_return": [0.03],
            "t_plus_5_max_drawdown": [-0.01],
        }
    )

    display = format_similarity_cases_for_display(cases)

    assert "窗口开始" in display.columns
    assert "窗口结束" in display.columns
    assert "总相似度" in display.columns
    assert "主走势相似度" in display.columns
    assert "独立变量相似度" in display.columns
    assert "相似原因" in display.columns
    assert "主要差异" in display.columns
    assert "后验收益 T+5" in display.columns
    assert "最大回撤 T+5" in display.columns
    assert "window_start_index" not in display.columns
    assert "window_end_index" not in display.columns
    assert "path_distance" not in display.columns
    assert "feature_distance" not in display.columns
    assert "external_distance" not in display.columns
    assert "dtw_distance" not in display.columns
    assert display.loc[0, "总相似度"] == "91.00%"
    assert display.loc[0, "主走势相似度"] == "88.00%"
    assert display.loc[0, "独立变量相似度"] == "66.00%"
    assert display.loc[0, "后验收益 T+5"] == "3.00%"
    assert display.loc[0, "最大回撤 T+5"] == "-1.00%"


def test_resolve_similarity_symbol_value_refreshes_stale_session_symbol() -> None:
    symbol, last_preset = resolve_similarity_symbol_value(
        preset="创业板指",
        current_symbol="000300.SH",
        last_preset="沪深300",
    )

    assert symbol == "399006.SZ"
    assert last_preset == "创业板指"


def test_format_outcome_summary_for_display_uses_chinese_headers_and_percentages() -> None:
    summary = pd.DataFrame(
        {
            "horizon": [5],
            "sample_count": [10],
            "win_rate": [0.7],
            "median_return": [0.025],
            "median_max_drawdown": [-0.012],
        }
    )

    display = format_outcome_summary_for_display(summary)

    assert list(display.columns) == ["观察窗口", "样本数", "胜率", "中位收益", "中位最大回撤"]
    assert display.loc[0, "胜率"] == "70.00%"
    assert display.loc[0, "中位收益"] == "2.50%"
    assert display.loc[0, "中位最大回撤"] == "-1.20%"


def test_similarity_feature_guide_explains_usage_and_tuning() -> None:
    guide = similarity_feature_guide_for_display()

    assert list(guide.columns) == ["模块", "特征", "怎么用", "相关参数", "调参方向"]
    assert "总相似度" in guide["特征"].to_list()
    assert "归一化收盘路径" in guide["特征"].to_list()
    assert guide["怎么用"].str.contains("65%").any()
    assert guide["怎么用"].str.contains("主走势").any()
    assert guide["相关参数"].str.contains("主走势窗口").any()
    assert guide["调参方向"].str.contains("放宽").any()


def test_similarity_algorithm_steps_explain_pipeline_in_plain_language() -> None:
    steps = similarity_algorithm_steps_for_display()

    assert list(steps.columns) == ["步骤", "算法在做什么", "用户怎么理解", "受哪些参数影响"]
    assert steps["算法在做什么"].str.contains("当前窗口").any()
    assert steps["算法在做什么"].str.contains("遍历历史").any()
    assert steps["算法在做什么"].str.contains("DTW").any()
    assert steps["用户怎么理解"].str.contains("不偷看未来").any()
    assert steps["受哪些参数影响"].str.contains("候选样本数").any()


def test_build_top_similarity_candlestick_grid_uses_compact_2_by_2_layout() -> None:
    result = type(
        "Result",
        (),
        {
            "cases": pd.DataFrame(
                {
                    "window_start": pd.to_datetime(
                        ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
                    ),
                    "window_end": pd.to_datetime(
                        ["2024-01-03", "2024-02-03", "2024-03-03", "2024-04-03"]
                    ),
                    "total_similarity": [0.92, 0.88, 0.84, 0.81],
                }
            ),
            "historical_windows_with_forward": [
                pd.DataFrame(
                    {
                        "date": pd.date_range("2024-01-01", periods=5),
                        "open": [10, 11, 12, 13, 14],
                        "high": [11, 12, 13, 14, 15],
                        "low": [9, 10, 11, 12, 13],
                        "close": [10.5, 11.5, 12.5, 13.5, 14.5],
                        "similarity_phase": ["相似窗口", "相似窗口", "相似窗口", "后10交易日", "后10交易日"],
                    }
                ),
                pd.DataFrame(
                    {
                        "date": pd.date_range("2024-02-01", periods=5),
                        "open": [20, 21, 22, 23, 24],
                        "high": [21, 22, 23, 24, 25],
                        "low": [19, 20, 21, 22, 23],
                        "close": [20.5, 21.5, 22.5, 23.5, 24.5],
                        "similarity_phase": ["相似窗口", "相似窗口", "相似窗口", "后10交易日", "后10交易日"],
                    }
                ),
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(
                            ["2024-03-01", "2024-03-04", "2024-03-05", "2024-03-06", "2024-03-07"]
                        ),
                        "open": [30, 31, 32, 33, 34],
                        "high": [31, 32, 33, 34, 35],
                        "low": [29, 30, 31, 32, 33],
                        "close": [30.5, 31.5, 32.5, 33.5, 34.5],
                        "similarity_phase": ["相似窗口", "相似窗口", "相似窗口", "后10交易日", "后10交易日"],
                    }
                ),
                pd.DataFrame(
                    {
                        "date": pd.date_range("2024-04-01", periods=5),
                        "open": [40, 41, 42, 43, 44],
                        "high": [41, 42, 43, 44, 45],
                        "low": [39, 40, 41, 42, 43],
                        "close": [40.5, 41.5, 42.5, 43.5, 44.5],
                        "similarity_phase": ["相似窗口", "相似窗口", "相似窗口", "后10交易日", "后10交易日"],
                    }
                ),
            ],
        },
    )()

    figure = build_top_similarity_candlestick_grid(result, top_k=4)

    assert figure is not None
    assert len(figure.data) == 4
    assert figure.data[0].type == "candlestick"
    assert list(figure.data[0].x) == [1, 2, 3, 4, 5]
    assert list(figure.data[2].x) == [1, 2, 3, 4, 5]
    assert "后10交易日" in figure.data[0].hovertext[-1]
    assert len(figure.layout.shapes) >= 8
    assert len(figure.layout.annotations) == 4
    assert "Top 1" in figure.layout.annotations[0].text
    assert "92.00%" in figure.layout.annotations[0].text
    assert figure.layout.grid.rows == 2
    assert figure.layout.grid.columns == 2


def test_resolve_similarity_config_from_custom_interval_uses_selected_trading_range() -> None:
    bars = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "stock_code": ["000300.SH"] * 10,
            "open": range(10),
            "high": range(1, 11),
            "low": range(10),
            "close": range(1, 11),
            "volume": [100] * 10,
            "amount": [1000] * 10,
        }
    )
    settings = {
        "symbol": "000300.SH",
        "timeframe": "1d",
        "window_mode": "自定义起止",
        "interval_start": pd.Timestamp("2024-01-03"),
        "interval_end": pd.Timestamp("2024-01-06"),
        "forward_windows": (1, 3),
        "candidate_n": 100,
        "top_n": 10,
        "min_valid_cases": 8,
    }

    config, message = resolve_similarity_config_from_settings(bars, settings)

    assert message == ""
    assert config is not None
    assert config.window_size == 4
    assert config.as_of == pd.Timestamp("2024-01-06")


def test_resolve_similarity_config_rejects_empty_custom_interval() -> None:
    bars = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "stock_code": ["000300.SH"] * 3,
            "open": [1, 2, 3],
            "high": [1, 2, 3],
            "low": [1, 2, 3],
            "close": [1, 2, 3],
        }
    )
    settings = {
        "symbol": "000300.SH",
        "timeframe": "1d",
        "window_mode": "自定义起止",
        "interval_start": pd.Timestamp("2024-02-01"),
        "interval_end": pd.Timestamp("2024-02-05"),
        "forward_windows": (1, 3),
        "candidate_n": 100,
        "top_n": 10,
        "min_valid_cases": 8,
    }

    config, message = resolve_similarity_config_from_settings(bars, settings)

    assert config is None
    assert "自定义区间内没有可用交易数据" in message


def test_resolve_similarity_config_custom_interval_includes_full_intraday_end_date() -> None:
    bars = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02 10:00",
                    "2024-01-02 10:30",
                    "2024-01-03 10:00",
                ]
            ),
            "stock_code": ["399006.SZ"] * 3,
            "open": [1, 2, 3],
            "high": [1, 2, 3],
            "low": [1, 2, 3],
            "close": [1, 2, 3],
        }
    )
    settings = {
        "symbol": "399006.SZ",
        "timeframe": "30m",
        "window_mode": "自定义起止",
        "interval_start": pd.Timestamp("2024-01-02"),
        "interval_end": pd.Timestamp("2024-01-02"),
        "forward_windows": (8, 24),
        "candidate_n": 100,
        "top_n": 10,
        "min_valid_cases": 8,
    }

    config, message = resolve_similarity_config_from_settings(bars, settings)

    assert message == ""
    assert config is not None
    assert config.window_size == 2
    assert config.as_of == pd.Timestamp("2024-01-02 10:30")
