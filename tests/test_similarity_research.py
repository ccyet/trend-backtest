from __future__ import annotations

import pandas as pd

from research.similarity import SimilarityConfig, run_similarity_research


def _make_bars(closes: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=len(closes), freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "stock_code": ["000300.SH"] * len(closes),
            "open": closes,
            "high": [value + 0.5 for value in closes],
            "low": [value - 0.5 for value in closes],
            "close": closes,
            "volume": [100.0 + index for index in range(len(closes))],
            "amount": [1000.0 + index * 10 for index in range(len(closes))],
        }
    )


def test_similarity_research_excludes_current_neighborhood_and_future_data() -> None:
    closes = [
        10,
        11,
        12,
        13,
        14,
        11,
        12,
        13,
        14,
        15,
        20,
        19,
        18,
        17,
        16,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
    ]
    bars = _make_bars(closes)

    result = run_similarity_research(
        bars,
        SimilarityConfig(
            symbol="000300.SH",
            timeframe="1d",
            as_of=pd.Timestamp("2024-01-20"),
            window_size=5,
            forward_windows=(1, 3),
            candidate_n=10,
            top_n=3,
            min_valid_cases=1,
            exclusion_bars=5,
        ),
    )

    assert not result.cases.empty
    assert result.cases["window_end"].max() < pd.Timestamp("2024-01-15")
    assert result.cases.iloc[0]["window_start"] == pd.Timestamp("2024-01-06")
    assert len(result.historical_windows) == len(result.cases)
    assert result.historical_windows[0]["date"].iloc[0] == pd.Timestamp("2024-01-06")
    assert result.historical_windows[0]["date"].iloc[-1] == pd.Timestamp("2024-01-10")
    assert len(result.historical_windows_with_forward) == len(result.cases)
    assert len(result.historical_windows_with_forward[0]) == 15
    assert result.historical_windows_with_forward[0]["similarity_phase"].iloc[0] == "相似窗口"
    assert result.historical_windows_with_forward[0]["similarity_phase"].iloc[4] == "相似窗口"
    assert result.historical_windows_with_forward[0]["similarity_phase"].iloc[5] == "后10交易日"
    assert result.historical_windows_with_forward[0]["date"].iloc[-1] == pd.Timestamp("2024-01-20")
    assert result.status in {"偏多", "震荡", "偏防守"}


def test_similarity_research_marks_insufficient_cases_untradable() -> None:
    bars = _make_bars([10, 11, 12, 13, 14, 15, 16])

    result = run_similarity_research(
        bars,
        SimilarityConfig(
            symbol="000300.SH",
            timeframe="1d",
            as_of=pd.Timestamp("2024-01-07"),
            window_size=5,
            forward_windows=(1, 3),
            candidate_n=10,
            top_n=10,
            min_valid_cases=8,
        ),
    )

    assert result.status == "不可交易"
    assert "有效样本不足" in result.message
