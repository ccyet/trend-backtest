from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_readme_records_selected_strategy_variants() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "## 1.1 本次新增方案的落地选择" in readme
    assert "candle_run" in readme
    assert "candle_run_acceleration" in readme
    assert "最终**落地 2 个方案**" in readme


def test_requirements_match_delivered_handoff_runtime_floor() -> None:
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")

    assert "streamlit>=1.55" in requirements
    assert "pytest>=9.0" in requirements
