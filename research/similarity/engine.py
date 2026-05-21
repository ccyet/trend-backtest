from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from research.similarity.outcomes import forward_outcomes, summarize_strategy
from research.similarity.search import candidate_rows, rank_candidates
from research.similarity.windows import prepare_bars

POST_MATCH_TRADING_DAYS = 10
BARS_PER_TRADING_DAY = {
    "1d": 1,
    "30m": 8,
}


@dataclass(frozen=True)
class SimilarityConfig:
    symbol: str
    timeframe: str
    as_of: pd.Timestamp
    window_size: int
    forward_windows: tuple[int, ...]
    candidate_n: int = 100
    top_n: int = 10
    min_valid_cases: int = 8
    exclusion_bars: int | None = None


@dataclass(frozen=True)
class SimilarityResult:
    status: str
    message: str
    cases: pd.DataFrame
    current_window: pd.DataFrame
    historical_windows: list[pd.DataFrame]
    historical_windows_with_forward: list[pd.DataFrame]
    outcome_summary: pd.DataFrame


def run_similarity_research(bars: pd.DataFrame, config: SimilarityConfig) -> SimilarityResult:
    symbol = config.symbol.upper()
    prepared = prepare_bars(bars, symbol)
    if prepared.empty:
        return _empty_result("不可交易", "未找到可用行情数据。")

    as_of = pd.Timestamp(config.as_of)
    available = prepared.loc[prepared["date"] <= as_of].copy()
    if available.empty:
        return _empty_result("不可交易", "as-of 时间之前没有可用数据。")
    as_of_index = int(available.index[-1])
    if as_of_index + 1 < config.window_size:
        return _empty_result("不可交易", "当前窗口之前的数据不足。", prepared.iloc[:0])

    current_window = prepared.iloc[as_of_index - config.window_size + 1 : as_of_index + 1]
    max_forward_window = max(config.forward_windows) if config.forward_windows else 0
    exclusion_bars = config.exclusion_bars
    if exclusion_bars is None:
        exclusion_bars = max(config.window_size, 1)

    candidates = candidate_rows(
        prepared,
        current_window=current_window,
        as_of_index=as_of_index,
        window_size=config.window_size,
        max_forward_window=max_forward_window,
        exclusion_bars=exclusion_bars,
        candidate_n=config.candidate_n,
    )
    ranked = rank_candidates(
        prepared,
        candidates,
        current_window=current_window,
        top_n=config.top_n,
    )
    if ranked.empty:
        return SimilarityResult(
            status="不可交易",
            message=f"有效样本不足：0 / {config.min_valid_cases}",
            cases=ranked,
            current_window=current_window.reset_index(drop=True),
            historical_windows=[],
            historical_windows_with_forward=[],
            outcome_summary=pd.DataFrame(),
        )

    outcome_rows: list[dict[str, float]] = []
    historical_windows: list[pd.DataFrame] = []
    historical_windows_with_forward: list[pd.DataFrame] = []
    chart_forward_bars = _post_match_forward_bars(config.timeframe)
    for row in ranked.itertuples(index=False):
        window_start_index = int(row.window_start_index)
        window_end_index = int(row.window_end_index)
        outcome_rows.append(
            forward_outcomes(
                prepared,
                window_end_index=window_end_index,
                forward_windows=config.forward_windows,
            )
        )
        historical_windows.append(
            prepared.iloc[window_start_index : window_end_index + 1].reset_index(drop=True)
        )
        historical_windows_with_forward.append(
            _window_with_forward_context(
                prepared,
                window_start_index=window_start_index,
                window_end_index=window_end_index,
                forward_bars=chart_forward_bars,
            )
        )
    outcomes = pd.DataFrame(outcome_rows)
    cases = pd.concat([ranked.reset_index(drop=True), outcomes], axis=1)
    primary_horizon = config.forward_windows[min(1, len(config.forward_windows) - 1)]
    status, message = summarize_strategy(
        cases,
        primary_horizon=primary_horizon,
        min_valid_cases=config.min_valid_cases,
    )
    return SimilarityResult(
        status=status,
        message=message,
        cases=cases,
        current_window=current_window.reset_index(drop=True),
        historical_windows=historical_windows,
        historical_windows_with_forward=historical_windows_with_forward,
        outcome_summary=_outcome_summary(cases, config.forward_windows),
    )


def _empty_result(
    status: str,
    message: str,
    current_window: pd.DataFrame | None = None,
) -> SimilarityResult:
    return SimilarityResult(
        status=status,
        message=message,
        cases=pd.DataFrame(),
        current_window=current_window if current_window is not None else pd.DataFrame(),
        historical_windows=[],
        historical_windows_with_forward=[],
        outcome_summary=pd.DataFrame(),
    )


def _post_match_forward_bars(timeframe: str) -> int:
    return POST_MATCH_TRADING_DAYS * BARS_PER_TRADING_DAY.get(str(timeframe), 1)


def _window_with_forward_context(
    bars: pd.DataFrame,
    *,
    window_start_index: int,
    window_end_index: int,
    forward_bars: int,
) -> pd.DataFrame:
    chart_end_index = min(window_end_index + forward_bars, len(bars) - 1)
    window = bars.iloc[window_start_index : chart_end_index + 1].reset_index(drop=True)
    match_length = window_end_index - window_start_index + 1
    window["similarity_phase"] = [
        "相似窗口" if index < match_length else "后10交易日"
        for index in range(len(window))
    ]
    return window


def _outcome_summary(cases: pd.DataFrame, forward_windows: tuple[int, ...]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for horizon in forward_windows:
        return_column = f"t_plus_{horizon}_return"
        drawdown_column = f"t_plus_{horizon}_max_drawdown"
        returns = pd.to_numeric(cases.get(return_column), errors="coerce").dropna()
        drawdowns = pd.to_numeric(cases.get(drawdown_column), errors="coerce").dropna()
        rows.append(
            {
                "horizon": horizon,
                "sample_count": int(len(returns)),
                "win_rate": float((returns > 0).mean()) if len(returns) else float("nan"),
                "median_return": float(returns.median()) if len(returns) else float("nan"),
                "median_max_drawdown": float(drawdowns.median()) if len(drawdowns) else float("nan"),
            }
        )
    return pd.DataFrame(rows)
