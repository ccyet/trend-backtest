from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pandas as pd

from data.indicators.registry import IndicatorSpec, list_indicator_specs


ROOT = Path(__file__).resolve().parents[2]
INDICATOR_METADATA_DIR = ROOT / "data" / "indicators" / "metadata"
INDICATOR_INVENTORY_PATH = INDICATOR_METADATA_DIR / "indicator_inventory.parquet"
INDICATOR_REGISTRY_PATH = INDICATOR_METADATA_DIR / "indicator_registry.parquet"

INVENTORY_COLUMNS = [
    "indicator_key",
    "display_name",
    "source_type",
    "symbol",
    "timeframe",
    "adjust",
    "file_path",
    "row_count",
    "non_null_columns",
    "min_date",
    "max_date",
    "last_success_at",
    "last_update_status",
    "last_error_message",
    "updated_at",
]
REGISTRY_COLUMNS = [
    "indicator_key",
    "display_name",
    "source_type",
    "output_columns",
    "align_rule",
    "required_timeframe",
    "lookahead_policy",
    "allow_scan",
    "allow_filter",
    "allow_exit",
    "storage_subdir",
    "formula_name",
    "description",
]
DATETIME_COLUMNS = ["min_date", "max_date", "last_success_at", "updated_at"]


def _empty_inventory() -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(INVENTORY_COLUMNS))


def _empty_registry() -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(REGISTRY_COLUMNS))


def _coerce_inventory(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in DATETIME_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce")
    if "row_count" in out.columns:
        out["row_count"] = pd.to_numeric(out["row_count"], errors="coerce")
    for column in INVENTORY_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    return cast(pd.DataFrame, out[INVENTORY_COLUMNS])


def load_indicator_inventory(
    inventory_path: str | Path = INDICATOR_INVENTORY_PATH,
) -> pd.DataFrame:
    path = Path(inventory_path)
    if not path.exists():
        return _empty_inventory()
    try:
        return _coerce_inventory(pd.read_parquet(path))
    except Exception:
        return _empty_inventory()


def upsert_indicator_inventory_row(
    row: dict[str, Any],
    inventory_path: str | Path = INDICATOR_INVENTORY_PATH,
) -> pd.DataFrame:
    path = Path(inventory_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    current = load_indicator_inventory(path)
    incoming = _coerce_inventory(pd.DataFrame([{**row}]))
    for column in ("indicator_key", "source_type", "timeframe", "adjust"):
        incoming.at[0, column] = str(incoming.at[0, column] or "").strip()
    incoming.at[0, "symbol"] = str(incoming.at[0, "symbol"] or "").strip().upper()
    if pd.isna(incoming.at[0, "updated_at"]):
        incoming.at[0, "updated_at"] = pd.Timestamp.utcnow()

    if not current.empty:
        key_mask = (
            (current["indicator_key"].astype(str) == str(incoming.at[0, "indicator_key"]))
            & (current["symbol"].astype(str) == str(incoming.at[0, "symbol"]))
            & (current["timeframe"].astype(str) == str(incoming.at[0, "timeframe"]))
            & (current["adjust"].astype(str) == str(incoming.at[0, "adjust"]))
        )
        current = current.loc[~key_mask].copy()
    updated = incoming if current.empty else pd.concat([current, incoming], ignore_index=True)
    updated = _coerce_inventory(updated).sort_values(
        ["updated_at", "indicator_key", "symbol"], ascending=[False, True, True]
    )
    try:
        updated.to_parquet(path, index=False)
    except Exception:
        pass
    return updated.reset_index(drop=True)


def build_registry_manifest(specs: list[IndicatorSpec] | None = None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    resolved_specs = list_indicator_specs() if specs is None else specs
    for spec in resolved_specs:
        rows.append(
            {
                "indicator_key": spec.key,
                "display_name": spec.display_name,
                "source_type": spec.source_type,
                "output_columns": ", ".join(spec.output_columns),
                "align_rule": spec.align_rule,
                "required_timeframe": spec.required_timeframe,
                "lookahead_policy": spec.lookahead_policy,
                "allow_scan": bool(spec.allow_scan),
                "allow_filter": bool(spec.allow_filter),
                "allow_exit": bool(spec.allow_exit),
                "storage_subdir": spec.storage_key,
                "formula_name": spec.formula_name,
                "description": spec.description,
            }
        )
    return pd.DataFrame(rows, columns=pd.Index(REGISTRY_COLUMNS))


def sync_registry_manifest(
    specs: list[IndicatorSpec] | None = None,
    manifest_path: str | Path = INDICATOR_REGISTRY_PATH,
) -> pd.DataFrame:
    path = Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = build_registry_manifest(specs)
    try:
        manifest.to_parquet(path, index=False)
    except Exception:
        pass
    return manifest


def load_registry_manifest(
    manifest_path: str | Path = INDICATOR_REGISTRY_PATH,
) -> pd.DataFrame:
    path = Path(manifest_path)
    if path.exists():
        try:
            loaded = pd.read_parquet(path)
            for column in REGISTRY_COLUMNS:
                if column not in loaded.columns:
                    loaded[column] = pd.NA
            return cast(pd.DataFrame, loaded[REGISTRY_COLUMNS])
        except Exception:
            pass
    return sync_registry_manifest()


def list_indicator_symbols(
    indicator_key: str,
    timeframe: str = "1d",
    adjust: str | None = None,
    inventory_path: str | Path = INDICATOR_INVENTORY_PATH,
) -> list[str]:
    inventory = load_indicator_inventory(inventory_path)
    if inventory.empty:
        return []
    filtered = inventory.loc[
        (inventory["indicator_key"].astype(str) == str(indicator_key).strip())
        & (inventory["timeframe"].astype(str) == str(timeframe).strip())
    ].copy()
    if adjust is not None:
        filtered = filtered.loc[filtered["adjust"].astype(str) == str(adjust).strip()]
    successful = filtered.loc[filtered["last_update_status"].astype(str) == "success"]
    if not successful.empty:
        filtered = successful
    return sorted(
        symbol
        for symbol in filtered["symbol"].astype(str).str.strip().str.upper().unique().tolist()
        if symbol
    )


def summarize_indicator_availability(
    limit: int = 20,
    inventory_path: str | Path = INDICATOR_INVENTORY_PATH,
) -> pd.DataFrame:
    inventory = load_indicator_inventory(inventory_path)
    if inventory.empty:
        return pd.DataFrame(
            columns=pd.Index(
                [
                    "indicator_key",
                    "display_name",
                    "source_type",
                    "symbols",
                    "rows",
                    "date_range",
                    "status",
                    "updated_at",
                ]
            )
        )
    grouped_rows: list[dict[str, Any]] = []
    grouped = inventory.groupby(["indicator_key", "timeframe", "adjust"], dropna=False)
    for group_key, group in grouped:
        key_tuple = cast(tuple[Any, ...], group_key)
        indicator_key = key_tuple[0]
        timeframe = key_tuple[1]
        adjust = key_tuple[2]
        last = group.sort_values("updated_at", ascending=False).iloc[0]
        min_date = pd.to_datetime(group["min_date"], errors="coerce").min()
        max_date = pd.to_datetime(group["max_date"], errors="coerce").max()
        symbol_count = len(
            [
                symbol
                for symbol in group["symbol"].astype(str).str.strip().str.upper().unique().tolist()
                if symbol
            ]
        )
        row_count_series = cast(
            pd.Series, pd.to_numeric(group["row_count"], errors="coerce")
        ).fillna(0)
        date_range = ""
        if pd.notna(min_date) and pd.notna(max_date):
            date_range = f"{cast(pd.Timestamp, min_date).strftime('%Y-%m-%d')} → {cast(pd.Timestamp, max_date).strftime('%Y-%m-%d')}"
        grouped_rows.append(
            {
                "indicator_key": indicator_key,
                "display_name": str(last.get("display_name", indicator_key)),
                "source_type": str(last.get("source_type", "")),
                "timeframe": timeframe,
                "adjust": adjust,
                "symbols": symbol_count,
                "rows": int(row_count_series.sum()),
                "date_range": date_range,
                "status": str(last.get("last_update_status", "")),
                "updated_at": pd.to_datetime(last.get("updated_at"), errors="coerce"),
            }
        )
    summary = pd.DataFrame(grouped_rows)
    return summary.sort_values("updated_at", ascending=False).head(limit).reset_index(drop=True)


def summarize_indicator_quality(
    limit: int = 20,
    inventory_path: str | Path = INDICATOR_INVENTORY_PATH,
) -> pd.DataFrame:
    inventory = load_indicator_inventory(inventory_path)
    if inventory.empty:
        return pd.DataFrame(
            columns=pd.Index(
                ["indicator_key", "symbol", "timeframe", "columns_ready", "date_range", "status"]
            )
        )
    preview = inventory.copy().sort_values("updated_at", ascending=False).head(limit)
    preview["date_range"] = preview.apply(
        lambda row: (
            ""
            if pd.isna(row.get("min_date")) or pd.isna(row.get("max_date"))
            else f"{cast(pd.Timestamp, pd.to_datetime(row['min_date'])).strftime('%Y-%m-%d')} → {cast(pd.Timestamp, pd.to_datetime(row['max_date'])).strftime('%Y-%m-%d')}"
        ),
        axis=1,
    )
    display = cast(
        pd.DataFrame,
        preview[
            [
                "indicator_key",
                "symbol",
                "timeframe",
                "non_null_columns",
                "date_range",
                "last_update_status",
            ]
        ].copy(),
    )
    return cast(
        pd.DataFrame,
        display.rename(
            columns={
                "non_null_columns": "columns_ready",
                "last_update_status": "status",
            }
        ),
    )
