from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.providers.akshare_provider import AkshareProvider
from data.providers.tdx_quant_provider import TdxQuantProvider
from data.services.local_inventory_service import upsert_inventory_row
from data_loader import resolve_local_data_root
CONFIG_PATH = ROOT / "config" / "data_source.yaml"
METADATA_DIR = ROOT / "data" / "market" / "metadata"
UPDATE_LOG_PATH = METADATA_DIR / "update_log.parquet"
SYMBOLS_PATH = METADATA_DIR / "symbols.parquet"
TIMEFRAME_EXPORT_DIR = {"1d": "daily", "30m": "30m", "15m": "15m", "5m": "5m", "1m": "1m"}
TIMEFRAME_BACKFILL_DAYS = {"1d": 30, "30m": 10, "15m": 7, "5m": 5, "1m": 3}


def read_config() -> dict[str, str]:
    cfg: dict[str, str] = {}
    for line in CONFIG_PATH.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or ":" not in text:
            continue
        key, value = text.split(":", 1)
        cfg[key.strip()] = value.strip().strip("\"'" )
    return {
        "data_source": str(cfg.get("data_source", "local_parquet")),
        "local_data_root": str(cfg.get("local_data_root", "data/market/daily")),
        "default_adjust": str(cfg.get("default_adjust", "qfq")),
    }


def _append_update_log(row: dict[str, object]) -> None:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    if UPDATE_LOG_PATH.exists():
        log_df = pd.read_parquet(UPDATE_LOG_PATH)
    else:
        log_df = pd.DataFrame(
            columns=pd.Index(["symbol", "timeframe", "adjust", "start_date", "end_date", "rows", "updated_at", "status", "error_message"])
        )

    if "timeframe" not in log_df.columns:
        log_df["timeframe"] = "1d"

    new_row_df = pd.DataFrame([row])
    updated = new_row_df if log_df.empty else pd.concat([log_df, new_row_df], ignore_index=True)
    updated["updated_at"] = pd.to_datetime(updated["updated_at"], errors="coerce")
    updated.to_parquet(UPDATE_LOG_PATH, index=False)


def _clean_bars(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    out = df.copy()
    out["symbol"] = AkshareProvider.to_standard_symbol(symbol)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
    for column in numeric_columns:
        if column not in out.columns:
            out[column] = pd.NA
        numeric_series = cast(pd.Series, pd.to_numeric(out[column], errors="coerce"))
        out[column] = numeric_series.astype(float)

    out = out.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    out = out[["date", "symbol", "open", "high", "low", "close", "volume", "amount"]]
    return cast(pd.DataFrame, out)


def _merge_incremental(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged




def _export_symbol_to_excel(symbol: str, adjust: str, local_root: Path, timeframe: str) -> str | None:
    parquet_path = local_root / adjust / f"{symbol}.parquet"
    if not parquet_path.exists():
        return None

    export_dir = ROOT / "data" / "market" / "exports"
    if timeframe != "1d":
        export_dir = export_dir / TIMEFRAME_EXPORT_DIR[timeframe]
    export_dir = export_dir / adjust
    export_dir.mkdir(parents=True, exist_ok=True)
    excel_path = export_dir / f"{symbol}.xlsx"

    df = pd.read_parquet(parquet_path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.to_excel(excel_path, index=False)
    return str(excel_path)


def _resolve_incremental_start(
    requested_start: str,
    existing_df: pd.DataFrame | None,
    timeframe: str,
) -> str:
    requested_start_ts = pd.to_datetime(requested_start)
    if existing_df is None or existing_df.empty or "date" not in existing_df.columns:
        return requested_start_ts.strftime("%Y-%m-%d")

    existing_dates = pd.to_datetime(existing_df["date"], errors="coerce").dropna()
    if existing_dates.empty:
        return requested_start_ts.strftime("%Y-%m-%d")

    max_existing = existing_dates.max()
    backfill_days = TIMEFRAME_BACKFILL_DAYS.get(timeframe, 5)
    incremental_start = max_existing - pd.Timedelta(days=backfill_days)
    effective_start = max(requested_start_ts, incremental_start)
    return effective_start.strftime("%Y-%m-%d %H:%M:%S" if timeframe != "1d" else "%Y-%m-%d")


def _build_inventory_row(
    *,
    symbol: str,
    timeframe: str,
    adjust: str,
    file_path: Path,
    final_df: pd.DataFrame,
    status: str,
    error_message: str,
    updated_at: pd.Timestamp,
) -> dict[str, object]:
    date_series = cast(pd.Series, final_df["date"] if "date" in final_df.columns else pd.Series(dtype="datetime64[ns]"))
    normalized_dates = pd.to_datetime(date_series, errors="coerce").dropna()
    file_size_bytes = file_path.stat().st_size if file_path.exists() else 0
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "adjust": adjust,
        "file_path": str(file_path),
        "row_count": int(len(final_df)),
        "min_date": normalized_dates.min() if not normalized_dates.empty else pd.NaT,
        "max_date": normalized_dates.max() if not normalized_dates.empty else pd.NaT,
        "file_size_bytes": int(file_size_bytes),
        "last_success_at": updated_at if status == "success" else pd.NaT,
        "last_update_status": status,
        "last_error_message": error_message,
        "updated_at": updated_at,
    }


def _dataframes_equivalent(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    if left.empty and right.empty:
        return True
    if list(left.columns) != list(right.columns):
        return False
    comparable_left = left.reset_index(drop=True).copy()
    comparable_right = right.reset_index(drop=True).copy()
    for column in comparable_left.columns:
        if column == "date":
            comparable_left[column] = pd.to_datetime(comparable_left[column], errors="coerce")
            comparable_right[column] = pd.to_datetime(comparable_right[column], errors="coerce")
    return comparable_left.equals(comparable_right)


def _format_exception_message(exc: BaseException) -> str:
    parts: list[str] = []
    seen_ids: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen_ids:
        seen_ids.add(id(current))
        detail = str(current).strip()
        parts.append(f"{current.__class__.__name__}: {detail}" if detail else current.__class__.__name__)
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return " <- ".join(parts)


def _fetch_bars_for_timeframe(
    *,
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> pd.DataFrame:
    if timeframe in {"1m", "5m"}:
        return TdxQuantProvider.fetch_bars(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

    return AkshareProvider.fetch_bars(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )

def update_one_symbol(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str,
    local_root: Path,
    export_excel: bool,
    timeframe: str = "1d",
) -> bool:
    standardized_symbol = AkshareProvider.to_standard_symbol(symbol)
    symbol_path = local_root / adjust / f"{standardized_symbol}.parquet"
    symbol_path.parent.mkdir(parents=True, exist_ok=True)

    status = "success"
    error_message = ""
    rows = 0
    updated_at = pd.Timestamp.utcnow()
    final_df = pd.DataFrame(columns=pd.Index(["date", "symbol", "open", "high", "low", "close", "volume", "amount"]))

    try:
        existing: pd.DataFrame | None = None
        fetch_start = start_date
        if symbol_path.exists():
            existing = pd.read_parquet(symbol_path)
            existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
            fetch_start = _resolve_incremental_start(start_date, existing, timeframe)

        raw = _fetch_bars_for_timeframe(
            symbol=standardized_symbol,
            timeframe=timeframe,
            start_date=fetch_start,
            end_date=end_date,
            adjust=adjust,
        )
        cleaned = _clean_bars(raw, standardized_symbol)

        if existing is not None:
            merged = _merge_incremental(existing, cleaned)
        else:
            merged = cleaned.sort_values("date").reset_index(drop=True)

        if cleaned.empty and existing is not None:
            final_df = existing.sort_values("date").reset_index(drop=True)
        else:
            final_df = merged
            if existing is None or not _dataframes_equivalent(existing, merged):
                merged.to_parquet(symbol_path, index=False)

        rows = len(final_df)
        if export_excel:
            _export_symbol_to_excel(standardized_symbol, adjust, local_root, timeframe)
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error_message = _format_exception_message(exc)
        if symbol_path.exists():
            try:
                final_df = pd.read_parquet(symbol_path)
            except Exception:  # noqa: BLE001
                final_df = pd.DataFrame(columns=pd.Index(["date", "symbol", "open", "high", "low", "close", "volume", "amount"]))

    _append_update_log(
        {
            "symbol": standardized_symbol,
            "timeframe": timeframe,
            "adjust": adjust,
            "start_date": start_date,
            "end_date": end_date,
            "rows": int(rows),
            "updated_at": updated_at,
            "status": status,
            "error_message": error_message,
        }
    )
    upsert_inventory_row(
        _build_inventory_row(
            symbol=standardized_symbol,
            timeframe=timeframe,
            adjust=adjust,
            file_path=symbol_path,
            final_df=final_df,
            status=status,
            error_message=error_message,
            updated_at=updated_at,
        )
    )
    return status == "success"


def update_symbol_metadata() -> None:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    symbols = AkshareProvider.fetch_symbol_list().copy()
    if "symbol" in symbols.columns:
        symbols["symbol"] = symbols["symbol"].map(AkshareProvider.to_standard_symbol)
    symbols.to_parquet(SYMBOLS_PATH, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="离线更新 A 股行情到本地 parquet")
    parser.add_argument("--symbols", type=str, default="", help="逗号分隔标准代码，如 000001.SZ,600519.SH；留空则按 symbols.parquet 全量更新")
    parser.add_argument("--start-date", type=str, default="20100101", help="起始日期，支持 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=datetime.now().strftime("%Y%m%d"), help="结束日期")
    parser.add_argument("--adjust", type=str, default="", choices=["", "qfq", "hfq"], help="复权类型，默认读取配置")
    parser.add_argument(
        "--timeframe",
        type=str,
        action="append",
        choices=["1d", "30m", "15m", "5m", "1m"],
        default=None,
        help="更新周期，可重复传参：--timeframe 1d --timeframe 30m",
    )
    parser.add_argument("--refresh-symbols", action="store_true", help="先刷新股票列表 metadata")
    parser.add_argument("--export-excel", action="store_true", help="更新完成后把对应 symbol 另存为 Excel")
    return parser.parse_args()


def _normalize_date(date_text: str) -> str:
    return pd.to_datetime(date_text).strftime("%Y-%m-%d")


def _normalize_timeframes(timeframes: list[str] | None) -> list[str]:
    if not timeframes:
        return ["1d"]
    deduped: list[str] = []
    seen: set[str] = set()
    for timeframe in timeframes:
        normalized = str(timeframe).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped or ["1d"]


def main() -> None:
    args = parse_args()
    config = read_config()
    if config["data_source"] != "local_parquet":
        raise ValueError("当前仅支持 local_parquet 数据源")

    selected_timeframes = _normalize_timeframes(cast(list[str] | None, args.timeframe))
    adjust = args.adjust or config["default_adjust"]
    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)

    if args.refresh_symbols or not SYMBOLS_PATH.exists():
        update_symbol_metadata()

    if args.symbols.strip():
        symbols = [AkshareProvider.to_standard_symbol(item.strip()) for item in args.symbols.split(",") if item.strip()]
    else:
        symbol_df = pd.read_parquet(SYMBOLS_PATH) if SYMBOLS_PATH.exists() else pd.DataFrame(columns=pd.Index(["symbol"]))
        if "symbol" in symbol_df.columns:
            symbol_series = cast(pd.Series, symbol_df["symbol"])
        else:
            symbol_series = pd.Series(dtype=str)
        symbols = [AkshareProvider.to_standard_symbol(symbol) for symbol in symbol_series.tolist()]

    for timeframe in selected_timeframes:
        local_root = resolve_local_data_root(str(ROOT / config["local_data_root"]), timeframe)
        success_count = 0
        failed_count = 0
        print(f"[timeframe={timeframe}] 开始更新，共 {len(symbols)} 只标的")
        for symbol in symbols:
            ok = update_one_symbol(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                local_root=local_root,
                export_excel=bool(args.export_excel),
                timeframe=timeframe,
            )
            if ok:
                success_count += 1
            else:
                failed_count += 1
        print(
            f"[timeframe={timeframe}] 更新完成：success={success_count}, failed={failed_count}, total={len(symbols)}"
        )


if __name__ == "__main__":
    main()
