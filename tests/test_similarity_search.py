from __future__ import annotations

import pandas as pd

from research.similarity.search import filter_nearby_windows


def test_filter_nearby_windows_keeps_best_sample_within_20_days() -> None:
    ranked = pd.DataFrame(
        {
            "window_start": pd.to_datetime(
                ["2016-12-15", "2016-12-17", "2017-02-10", "2017-03-05"]
            ),
            "window_end": pd.to_datetime(
                ["2017-01-06", "2017-01-08", "2017-03-01", "2017-03-20"]
            ),
            "total_similarity": [0.91, 0.95, 0.82, 0.78],
        }
    )

    filtered = filter_nearby_windows(ranked, min_gap_days=20, top_n=3)

    assert list(filtered["window_start"]) == [
        pd.Timestamp("2016-12-17"),
        pd.Timestamp("2017-02-10"),
    ]
    assert list(filtered["total_similarity"]) == [0.95, 0.82]
