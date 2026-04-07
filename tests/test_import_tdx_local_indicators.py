from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

import scripts.import_tdx_local_indicators as importer


def test_import_script_writes_indicator_parquet(tmp_path: Path, monkeypatch) -> None:
    import pytest

    pytest.importorskip("pyarrow")
    monkeypatch.setattr(importer, "ROOT", tmp_path)
    monkeypatch.setattr(importer, "INDICATOR_ROOT", tmp_path / "data" / "indicators")
    monkeypatch.setattr(importer, "sync_registry_manifest", lambda: pd.DataFrame())
    inventory_rows: list[dict[str, object]] = []
    monkeypatch.setattr(
        importer,
        "upsert_indicator_inventory_row",
        lambda row: inventory_rows.append(row) or pd.DataFrame([row]),
    )

    monkeypatch.setattr(
        importer.TdxLocalIndicatorProvider,
        "import_indicator",
        staticmethod(
            lambda **kwargs: pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-03", "2024-01-04"]),
                    "symbol": [kwargs["symbol"], kwargs["symbol"]],
                    "board_ma_ratio_20": [55.0, 60.0],
                    "board_ma_ratio_50": [45.0, 50.0],
                }
            )
        ),
    )

    ok, message = importer.import_one_symbol(
        symbol="000001.SZ",
        indicator_key="board_ma",
        start_date="2024-01-01",
        end_date="2024-01-04",
        adjust="qfq",
    )

    assert ok is True
    assert "rows=2" in message
    saved = pd.read_parquet(tmp_path / "data" / "indicators" / "board_ma" / "000001.SZ.parquet")
    assert saved["board_ma_ratio_20"].tolist() == [55.0, 60.0]
    assert inventory_rows and inventory_rows[0]["indicator_key"] == "board_ma"


def test_parse_args_accepts_indicator(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "import_tdx_local_indicators.py",
            "--indicator",
            "board_ma",
            "--symbols",
            "000001.SZ",
        ],
    )

    args = importer.parse_args()

    assert args.indicator == "board_ma"
    assert args.symbols == "000001.SZ"


def test_parse_args_accepts_manual_formula_options(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "import_tdx_local_indicators.py",
            "--indicator",
            "board_ma",
            "--symbols",
            "000001.SZ",
            "--formula-name",
            "板块均线",
            "--output-map",
            "board_ma_ratio_20=NOTEXT1,board_ma_ratio_50=NOTEXT2",
        ],
    )

    args = importer.parse_args()

    assert args.formula_name == "板块均线"
    assert args.output_map == "board_ma_ratio_20=NOTEXT1,board_ma_ratio_50=NOTEXT2"


def test_parse_symbols_supports_whitespace_and_chinese_commas() -> None:
    symbols = importer._parse_symbols("000001.SZ，600519.SH  300750.SZ")

    assert symbols == ["000001.SZ", "600519.SH", "300750.SZ"]


def test_parse_output_map_supports_target_source_pairs() -> None:
    mapping = importer._parse_output_map("board_ma_ratio_20=NOTEXT1,board_ma_ratio_50=NOTEXT2")

    assert mapping == {"board_ma_ratio_20": "NOTEXT1", "board_ma_ratio_50": "NOTEXT2"}
