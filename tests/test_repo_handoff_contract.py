from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_readme_records_selected_strategy_variants() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "## 1.1 本次新增方案的落地选择" in readme
    assert "candle_run" in readme
    assert "candle_run_acceleration" in readme
    assert "Brooks 趋势回撤 H2/L2" in readme
    assert "Brooks 交易区间失败突破" in readme
    assert "Brooks 主要趋势反转" in readme
    assert "已落地 5 个扩展方案" in readme


def test_al_brooks_mapping_doc_records_sources_and_chinese_names() -> None:
    doc = (ROOT / "docs" / "al_brooks_entry_mapping.md").read_text(encoding="utf-8")

    assert "Brooks 趋势回撤 H2/L2" in doc
    assert "Brooks 交易区间失败突破" in doc
    assert "Brooks 主要趋势反转" in doc
    assert "brookstradingcourse.com/brooks-price-action-abbreviations" in doc
    assert "close_pos_t = (close_t - low_t) / (high_t - low_t)" in doc
    assert "attempt_count_t = sum(long_attempt_i, i=t-L..t-1)" in doc
    assert "range_width_pct_t = (range_high_t / range_low_t - 1) * 100" in doc
    assert "trendline_break_t = any(close_i > ma_i, i=t-L-1..t-2)" in doc


def test_requirements_match_delivered_handoff_runtime_floor() -> None:
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")

    assert "streamlit>=1.55" in requirements
    assert "pytest>=9.0" in requirements
