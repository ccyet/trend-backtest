from __future__ import annotations

import numpy as np


def dtw_distance(left: np.ndarray, right: np.ndarray) -> float:
    left = np.asarray(left, dtype=float)
    right = np.asarray(right, dtype=float)
    if len(left) == 0 or len(right) == 0:
        return float("inf")

    rows = len(left) + 1
    cols = len(right) + 1
    costs = np.full((rows, cols), np.inf, dtype=float)
    costs[0, 0] = 0.0
    for i in range(1, rows):
        for j in range(1, cols):
            cost = abs(left[i - 1] - right[j - 1])
            costs[i, j] = cost + min(costs[i - 1, j], costs[i, j - 1], costs[i - 1, j - 1])
    return float(costs[-1, -1] / (len(left) + len(right)))


def bounded_similarity(distance: float) -> float:
    if not np.isfinite(distance):
        return 0.0
    return float(1.0 / (1.0 + max(0.0, distance)))
