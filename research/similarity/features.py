from __future__ import annotations

import numpy as np
import pandas as pd

from research.similarity.windows import normalized_close_path


def realized_volatility(window: pd.DataFrame) -> float:
    returns = window["close"].astype(float).pct_change().dropna()
    if returns.empty:
        return 0.0
    return float(returns.std(ddof=0))


def max_drawdown_from_prices(values: np.ndarray) -> float:
    if len(values) == 0:
        return 0.0
    running_max = np.maximum.accumulate(values)
    drawdowns = values / np.where(running_max == 0, np.nan, running_max) - 1.0
    if np.isnan(drawdowns).all():
        return 0.0
    return float(np.nanmin(drawdowns))


def trend_slope(window: pd.DataFrame) -> float:
    path = normalized_close_path(window)
    if len(path) < 2:
        return 0.0
    x = np.arange(len(path), dtype=float)
    return float(np.polyfit(x, path, 1)[0])


def volume_amount_features(window: pd.DataFrame) -> dict[str, float]:
    returns = window["close"].astype(float).pct_change()
    amount = pd.to_numeric(window.get("amount"), errors="coerce")
    volume = pd.to_numeric(window.get("volume"), errors="coerce")
    liquidity = amount.fillna(volume)
    down_mask = returns < 0
    down_liquidity = liquidity.loc[down_mask].sum()
    total_liquidity = liquidity.sum()
    if pd.isna(total_liquidity) or float(total_liquidity) == 0:
        down_share = 0.0
    else:
        down_share = float(down_liquidity / total_liquidity)
    if len(returns.dropna()) >= 2 and len(liquidity.dropna()) >= 2:
        corr = float(returns.corr(liquidity))
        if not np.isfinite(corr):
            corr = 0.0
    else:
        corr = 0.0
    return {
        "amount_mean": float(liquidity.mean()) if len(liquidity.dropna()) else 0.0,
        "amount_concentration": float(liquidity.max() / total_liquidity)
        if float(total_liquidity or 0) != 0
        else 0.0,
        "down_liquidity_share": down_share,
        "return_liquidity_corr": corr,
    }


def window_feature_summary(window: pd.DataFrame) -> dict[str, float]:
    path = normalized_close_path(window)
    return {
        "window_return": float(path[-1] / path[0] - 1.0) if len(path) >= 2 and path[0] else 0.0,
        "realized_volatility": realized_volatility(window),
        "max_drawdown": max_drawdown_from_prices(path),
        "trend_slope": trend_slope(window),
        **volume_amount_features(window),
    }
