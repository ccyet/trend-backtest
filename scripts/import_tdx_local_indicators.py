from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.providers.akshare_provider import AkshareProvider
from data.providers.tdx_local_indicator_provider import TdxLocalIndicatorProvider


INDICATOR_ROOT = ROOT / "data" / "indicators"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导入通达信本地指标到本地 parquet")
    parser.add_argument("--indicator", type=str, default="board_ma", help="指标标识，例如 board_ma")
    parser.add_argument("--symbols", type=str, required=True, help="逗号分隔标准代码，如 000001.SZ,600519.SH")
    parser.add_argument("--start-date", type=str, default="20100101", help="起始日期，支持 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=datetime.now().strftime("%Y%m%d"), help="结束日期")
    parser.add_argument("--adjust", type=str, default="qfq", choices=["", "qfq", "hfq"], help="复权类型")
    parser.add_argument("--formula-name", type=str, default="", help="手动指定通达信公式名称")
    parser.add_argument(
        "--output-map",
        type=str,
        default="",
        help="手动指定输出映射，格式如 board_ma_ratio_20=NOTEXT1,board_ma_ratio_50=NOTEXT2",
    )
    return parser.parse_args()


def _normalize_date(date_text: str) -> str:
    return pd.to_datetime(date_text).strftime("%Y-%m-%d")


def _indicator_dir(indicator_key: str) -> Path:
    path = INDICATOR_ROOT / indicator_key
    path.mkdir(parents=True, exist_ok=True)
    return path


def _parse_symbols(symbols_text: str) -> list[str]:
    pieces = [piece.strip() for piece in re.split(r"[，,\s]+", str(symbols_text)) if piece.strip()]
    return [AkshareProvider.to_standard_symbol(piece) for piece in pieces]


def _parse_output_map(output_map_text: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for piece in [item.strip() for item in str(output_map_text).split(",") if item.strip()]:
        if "=" not in piece:
            raise ValueError(f"输出映射格式错误: {piece}")
        target_column, source_key = [item.strip() for item in piece.split("=", 1)]
        if not target_column or not source_key:
            raise ValueError(f"输出映射格式错误: {piece}")
        mapping[target_column] = source_key
    return mapping


def import_one_symbol(
    *,
    symbol: str,
    indicator_key: str,
    start_date: str,
    end_date: str,
    adjust: str,
    formula_name: str = "",
    output_map: dict[str, str] | None = None,
) -> tuple[bool, str]:
    standardized_symbol = AkshareProvider.to_standard_symbol(symbol)
    target_path = _indicator_dir(indicator_key) / f"{standardized_symbol}.parquet"
    try:
        df = TdxLocalIndicatorProvider.import_indicator(
            symbol=standardized_symbol,
            indicator_key=indicator_key,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            formula_name=formula_name or None,
            output_map=output_map,
        )
        df.to_parquet(target_path, index=False)
        return True, f"[{indicator_key}] {standardized_symbol}: rows={len(df)}"
    except Exception as exc:  # noqa: BLE001
        return False, f"[{indicator_key}] {standardized_symbol}: failed -> {exc}"


def main() -> None:
    args = parse_args()
    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)
    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("请至少提供一个有效的股票代码（--symbols）。")
    output_map = _parse_output_map(args.output_map) if str(args.output_map).strip() else None
    success_count = 0
    failed_count = 0
    for symbol in symbols:
        ok, message = import_one_symbol(
            symbol=symbol,
            indicator_key=str(args.indicator).strip(),
            start_date=start_date,
            end_date=end_date,
            adjust=str(args.adjust),
            formula_name=str(args.formula_name).strip(),
            output_map=output_map,
        )
        print(message)
        if ok:
            success_count += 1
        else:
            failed_count += 1
    print(f"[indicator={args.indicator}] 导入完成：success={success_count}, failed={failed_count}, total={len(symbols)}")
    if failed_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
