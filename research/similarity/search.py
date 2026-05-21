from __future__ import annotations

import numpy as np
import pandas as pd

from research.similarity.features import window_feature_summary
from research.similarity.rank import bounded_similarity, dtw_distance
from research.similarity.windows import iter_candidate_windows, normalized_close_path, z_normalize

NEARBY_WINDOW_MIN_GAP_DAYS = 20


def candidate_rows(
    bars: pd.DataFrame,
    *,
    current_window: pd.DataFrame,
    as_of_index: int,
    window_size: int,
    max_forward_window: int,
    exclusion_bars: int,
    candidate_n: int,
) -> pd.DataFrame:
    current_path = z_normalize(normalized_close_path(current_window))
    current_features = window_feature_summary(current_window)
    rows: list[dict[str, object]] = []

    for start, end, candidate in iter_candidate_windows(
        bars,
        as_of_index=as_of_index,
        window_size=window_size,
        max_forward_window=max_forward_window,
        exclusion_bars=exclusion_bars,
    ):
        candidate_path = z_normalize(normalized_close_path(candidate))
        euclidean = float(np.linalg.norm(current_path - candidate_path))
        features = window_feature_summary(candidate)
        feature_distance = abs(current_features["realized_volatility"] - features["realized_volatility"])
        feature_distance += abs(current_features["max_drawdown"] - features["max_drawdown"])
        feature_distance += abs(current_features["trend_slope"] - features["trend_slope"]) / 100.0
        external_distance = abs(current_features["down_liquidity_share"] - features["down_liquidity_share"])
        external_distance += abs(current_features["return_liquidity_corr"] - features["return_liquidity_corr"])
        rows.append(
            {
                "window_start_index": start,
                "window_end_index": end,
                "window_start": bars.iloc[start]["date"],
                "window_end": bars.iloc[end]["date"],
                "path_distance": euclidean,
                "feature_distance": feature_distance,
                "external_distance": external_distance,
                "main_similarity": bounded_similarity(euclidean + feature_distance),
                "external_similarity": bounded_similarity(external_distance),
                "similar_reason": _similar_reason(current_features, features),
                "external_difference": _external_difference(current_features, features),
            }
        )

    if not rows:
        return pd.DataFrame()
    candidates = pd.DataFrame(rows)
    candidates = candidates.sort_values(["path_distance", "feature_distance"], ascending=True)
    return candidates.head(candidate_n).reset_index(drop=True)


def rank_candidates(
    bars: pd.DataFrame,
    candidates: pd.DataFrame,
    *,
    current_window: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    current_path = z_normalize(normalized_close_path(current_window))
    ranked = candidates.copy()
    dtw_distances: list[float] = []
    for row in ranked.itertuples(index=False):
        start = int(row.window_start_index)
        end = int(row.window_end_index)
        candidate_path = z_normalize(normalized_close_path(bars.iloc[start : end + 1]))
        dtw_distances.append(dtw_distance(current_path, candidate_path))
    ranked["dtw_distance"] = dtw_distances
    ranked["main_similarity"] = ranked["dtw_distance"].map(bounded_similarity)
    ranked["total_similarity"] = (
        ranked["main_similarity"].astype(float) * 0.65
        + ranked["external_similarity"].astype(float) * 0.35
    )
    sorted_ranked = ranked.sort_values("total_similarity", ascending=False)
    return filter_nearby_windows(
        sorted_ranked,
        min_gap_days=NEARBY_WINDOW_MIN_GAP_DAYS,
        top_n=top_n,
    )


def filter_nearby_windows(
    ranked: pd.DataFrame,
    *,
    min_gap_days: int = NEARBY_WINDOW_MIN_GAP_DAYS,
    top_n: int,
) -> pd.DataFrame:
    if ranked.empty:
        return ranked

    sorted_ranked = ranked.sort_values("total_similarity", ascending=False).reset_index(drop=True)
    selected_rows: list[pd.Series] = []
    for _, row in sorted_ranked.iterrows():
        if _is_near_selected_window(row, selected_rows, min_gap_days=min_gap_days):
            continue
        selected_rows.append(row)
        if len(selected_rows) >= top_n:
            break

    if not selected_rows:
        return sorted_ranked.iloc[:0].reset_index(drop=True)
    return pd.DataFrame(selected_rows).reset_index(drop=True)


def _is_near_selected_window(
    row: pd.Series,
    selected_rows: list[pd.Series],
    *,
    min_gap_days: int,
) -> bool:
    row_start = pd.Timestamp(row["window_start"])
    row_end = pd.Timestamp(row["window_end"])
    for selected in selected_rows:
        selected_start = pd.Timestamp(selected["window_start"])
        selected_end = pd.Timestamp(selected["window_end"])
        if _window_gap_days(row_start, row_end, selected_start, selected_end) < min_gap_days:
            return True
    return False


def _window_gap_days(
    left_start: pd.Timestamp,
    left_end: pd.Timestamp,
    right_start: pd.Timestamp,
    right_end: pd.Timestamp,
) -> int:
    if left_start <= right_end and right_start <= left_end:
        return 0
    if left_end < right_start:
        return int((right_start - left_end).days)
    return int((left_start - right_end).days)


def _similar_reason(current: dict[str, float], candidate: dict[str, float]) -> str:
    parts = []
    if _same_sign(current["window_return"], candidate["window_return"]):
        parts.append("方向相近")
    if abs(current["realized_volatility"] - candidate["realized_volatility"]) < 0.01:
        parts.append("波动率接近")
    if _same_sign(current["trend_slope"], candidate["trend_slope"]):
        parts.append("斜率同向")
    return "、".join(parts) or "路径距离较近"


def _external_difference(current: dict[str, float], candidate: dict[str, float]) -> str:
    differences = []
    if abs(current["down_liquidity_share"] - candidate["down_liquidity_share"]) > 0.2:
        differences.append("下跌放量占比差异较大")
    if abs(current["return_liquidity_corr"] - candidate["return_liquidity_corr"]) > 0.5:
        differences.append("量价相关差异较大")
    return "、".join(differences) or "成交环境接近"


def _same_sign(left: float, right: float) -> bool:
    return (left >= 0 and right >= 0) or (left < 0 and right < 0)
