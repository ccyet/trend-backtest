from __future__ import annotations

import pandas as pd


def forward_outcomes(
    bars: pd.DataFrame,
    *,
    window_end_index: int,
    forward_windows: tuple[int, ...],
) -> dict[str, float]:
    close = bars["close"].astype(float).reset_index(drop=True)
    start_price = float(close.iloc[window_end_index])
    result: dict[str, float] = {}
    for horizon in forward_windows:
        label = f"t_plus_{horizon}"
        future_end = window_end_index + horizon
        if future_end >= len(close) or start_price == 0:
            result[f"{label}_return"] = float("nan")
            result[f"{label}_max_drawdown"] = float("nan")
            result[f"{label}_max_favorable"] = float("nan")
            continue
        future = close.iloc[window_end_index + 1 : future_end + 1]
        result[f"{label}_return"] = float(close.iloc[future_end] / start_price - 1.0)
        result[f"{label}_max_drawdown"] = float(future.min() / start_price - 1.0)
        result[f"{label}_max_favorable"] = float(future.max() / start_price - 1.0)
    return result


def summarize_strategy(cases: pd.DataFrame, *, primary_horizon: int, min_valid_cases: int) -> tuple[str, str]:
    if len(cases) < min_valid_cases:
        return "不可交易", f"有效样本不足：{len(cases)} / {min_valid_cases}"

    return_column = f"t_plus_{primary_horizon}_return"
    drawdown_column = f"t_plus_{primary_horizon}_max_drawdown"
    returns = pd.to_numeric(cases[return_column], errors="coerce").dropna()
    drawdowns = pd.to_numeric(cases[drawdown_column], errors="coerce").dropna()
    if len(returns) < min_valid_cases:
        return "不可交易", f"后验收益样本不足：{len(returns)} / {min_valid_cases}"

    win_rate = float((returns > 0).mean())
    median_return = float(returns.median())
    median_drawdown = abs(float(drawdowns.median())) if len(drawdowns) else 0.0

    if win_rate >= 0.6 and median_return > 0 and median_return >= median_drawdown:
        return "偏多", "历史相似阶段的正收益概率和收益/回撤结构较好。"
    if win_rate < 0.45 or median_drawdown > max(median_return, 0.0) * 2:
        return "偏防守", "历史相似阶段的胜率或回撤结构偏弱。"
    return "震荡", "历史相似阶段收益分布分歧较大。"
