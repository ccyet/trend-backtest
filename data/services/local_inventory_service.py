from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
METADATA_DIR = ROOT / "data" / "market" / "metadata"
LOCAL_INVENTORY_PATH = METADATA_DIR / "local_inventory.parquet"
INVENTORY_KEY_COLUMNS = ["symbol", "timeframe", "adjust"]
INVENTORY_COLUMNS = [
    "symbol",
    "timeframe",
    "adjust",
    "file_path",
    "row_count",
    "min_date",
    "max_date",
    "file_size_bytes",
    "last_success_at",
    "last_update_status",
    "last_error_message",
    "updated_at",
]
DATETIME_COLUMNS = ["min_date", "max_date", "last_success_at", "updated_at"]


def _empty_inventory() -> pd.DataFrame:
    return pd.DataFrame(columns=pd.Index(INVENTORY_COLUMNS))


def _coerce_inventory_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in DATETIME_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce")
    for column in ["row_count", "file_size_bytes"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in INVENTORY_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    return cast(pd.DataFrame, out[INVENTORY_COLUMNS])


def load_inventory(inventory_path: str | Path = LOCAL_INVENTORY_PATH) -> pd.DataFrame:
    path = Path(inventory_path)
    if not path.exists():
        return _empty_inventory()
    try:
        loaded = pd.read_parquet(path)
    except Exception:
        return _empty_inventory()
    return _coerce_inventory_types(loaded)


def upsert_inventory_row(
    row: dict[str, Any], inventory_path: str | Path = LOCAL_INVENTORY_PATH
) -> pd.DataFrame:
    path = Path(inventory_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    current = load_inventory(path)
    incoming = _coerce_inventory_types(pd.DataFrame([{**row}]))
    for column, upper in (("symbol", True), ("timeframe", False), ("adjust", False)):
        value = incoming.at[0, column]
        text = "" if pd.isna(value) else str(value).strip()
        incoming.at[0, column] = text.upper() if upper else text
    if pd.isna(incoming.at[0, "updated_at"]):
        incoming.at[0, "updated_at"] = pd.Timestamp.utcnow()

    key_mask = pd.Series(True, index=current.index)
    for column in INVENTORY_KEY_COLUMNS:
        key_mask &= current[column].astype(str) == str(incoming.at[0, column])

    if key_mask.any():
        previous = cast(pd.Series, current.loc[key_mask].iloc[-1])
        for column in INVENTORY_COLUMNS:
            new_value = incoming.at[0, column]
            new_is_missing = bool(pd.isna(new_value)) or new_value == ""
            if new_is_missing and bool(pd.notna(previous[column])):
                incoming.at[0, column] = previous[column]
        current = current.loc[~key_mask].copy()

    updated = incoming.copy() if current.empty else pd.concat([current, incoming], ignore_index=True)
    updated = _coerce_inventory_types(updated)
    updated = updated.sort_values(["updated_at", "symbol", "timeframe", "adjust"], ascending=[False, True, True, True])
    updated.to_parquet(path, index=False)
    return updated.reset_index(drop=True)


def list_local_symbols_by_timeframe(
    timeframe: str,
    adjust: str | None = None,
    inventory_path: str | Path = LOCAL_INVENTORY_PATH,
) -> list[str]:
    inventory = load_inventory(inventory_path)
    if inventory.empty:
        return []
    filtered = inventory.loc[inventory["timeframe"].astype(str) == str(timeframe)].copy()
    if adjust is not None:
        filtered = filtered.loc[filtered["adjust"].astype(str) == str(adjust)]
    if filtered.empty or "symbol" not in filtered.columns:
        return []
    if "last_update_status" in filtered.columns:
        successful = filtered.loc[filtered["last_update_status"].astype(str) == "success"]
        if not successful.empty:
            filtered = successful
    symbols = filtered["symbol"].astype(str).str.strip().str.upper()
    return sorted(symbol for symbol in symbols.unique().tolist() if symbol)
