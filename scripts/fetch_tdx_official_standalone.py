from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import traceback
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.providers.tdx_official_standalone import (  # noqa: E402
    TdxOfficialStandaloneError,
    TdxOfficialStandaloneProvider,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone TDX 官方分钟 K 线抓取调试脚本")
    parser.add_argument("--symbol", required=True, type=str, help="标准代码，如 000001.SZ")
    parser.add_argument("--timeframe", required=True, choices=["1m", "5m"], help="周期，仅支持 1m/5m")
    parser.add_argument("--start-date", required=True, type=str, help="开始时间，支持 YYYY-MM-DD 或带时分秒")
    parser.add_argument("--end-date", required=True, type=str, help="结束时间，支持 YYYY-MM-DD 或带时分秒")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"], help="复权类型")
    parser.add_argument("--tdx-tqcenter-path", default="", type=str, help="可选：通达信安装根目录或 PYPlugins/user 目录")
    parser.add_argument("--output", default="", type=str, help="可选输出文件（.csv/.parquet/.json）")
    parser.add_argument("--print-diagnostics", action="store_true", help="打印请求和原始载荷诊断信息")
    return parser.parse_args(argv)


def _write_output(df, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(output_path, index=False)
        return
    if suffix in {".parquet", ".pq"}:
        df.to_parquet(output_path, index=False)
        return
    if suffix == ".json":
        serializable = df.copy()
        if "date" in serializable.columns:
            serializable["date"] = serializable["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        output_path.write_text(serializable.to_json(orient="records", force_ascii=False), encoding="utf-8")
        return
    raise ValueError("--output 仅支持 .csv/.parquet/.json")


def _print_diagnostics(diag: dict[str, Any], *, stream: Any | None = None) -> None:
    if stream is None:
        stream = sys.stdout
    print("Diagnostics:", file=stream)
    print(json.dumps(diag, ensure_ascii=False, indent=2, default=str), file=stream)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol=args.symbol,
            timeframe=args.timeframe,
            start_date=args.start_date,
            end_date=args.end_date,
            adjust=args.adjust,
            tqcenter_path=args.tdx_tqcenter_path or None,
        )
    except TdxOfficialStandaloneError as exc:
        print(str(exc), file=sys.stderr)
        _print_diagnostics(exc.diagnostics.to_dict(), stream=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Unexpected error: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return 1

    if args.print_diagnostics:
        _print_diagnostics(result.diagnostics.to_dict())

    bars = result.bars
    print(f"Fetched rows: {len(bars)}")
    if not bars.empty:
        print(bars.head(5).to_string(index=False))

    if args.output:
        output_path = Path(args.output)
        try:
            _write_output(bars, output_path)
        except Exception as exc:  # noqa: BLE001
            print(f"Output error: {exc.__class__.__name__}: {exc}", file=sys.stderr)
            return 1
        print(f"Saved normalized bars to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
