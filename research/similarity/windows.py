from __future__ import annotations

import numpy as np
import pandas as pd


def prepare_bars(bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(columns=["date", "stock_code", "open", "high", "low", "close", "volume", "amount"])

    frame = bars.copy()
    if "stock_code" not in frame.columns and "symbol" in frame.columns:
        frame = frame.rename(columns={"symbol": "stock_code"})
    frame["stock_code"] = frame["stock_code"].astype(str).str.upper()
    frame = frame.loc[frame["stock_code"] == symbol.upper()].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["date", "open", "high", "low", "close"])
    frame = frame.drop_duplicates(subset=["date"], keep="last")
    return frame.sort_values("date").reset_index(drop=True)


def normalized_close_path(window: pd.DataFrame) -> np.ndarray:
    values = window["close"].astype(float).to_numpy()
    if len(values) == 0:
        return np.array([], dtype=float)
    first = values[0]
    if not np.isfinite(first) or first == 0:
        return np.zeros(len(values), dtype=float)
    return values / first * 100.0


def z_normalize(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    if len(values) == 0:
        return values
    mean = float(np.nanmean(values))
    std = float(np.nanstd(values))
    if not np.isfinite(std) or std == 0:
        return values - mean
    return (values - mean) / std


def iter_candidate_windows(
    bars: pd.DataFrame,
    *,
    as_of_index: int,
    window_size: int,
    max_forward_window: int,
    exclusion_bars: int,
):
    latest_start = as_of_index - exclusion_bars - window_size + 1
    for start in range(0, max(0, latest_start + 1)):
        end = start + window_size - 1
        if end + max_forward_window >= len(bars):
            continue
        yield start, end, bars.iloc[start : end + 1]
