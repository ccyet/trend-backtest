from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, cast

import pandas as pd

from data.providers.akshare_provider import AkshareProvider
from data.providers.tdx_quant_provider import TdxQuantProvider


@dataclass(frozen=True)
class TdxLocalIndicatorSpec:
    key: str
    display_name: str
    formula_name: str
    output_candidates: dict[str, tuple[str, ...]]
    formula_arg: str = ""
    stock_period: str = "1d"
    lookback_days: int = 120


@dataclass(frozen=True)
class TdxLocalIndicatorProvider:
    INDICATOR_REGISTRY: ClassVar[dict[str, TdxLocalIndicatorSpec]] = {
        "board_ma": TdxLocalIndicatorSpec(
            key="board_ma",
            display_name="板块均线",
            formula_name="板块均线",
            output_candidates={
                "board_ma_ratio_20": ("均20占比", "NOTEXT1"),
                "board_ma_ratio_50": ("均50占比", "NOTEXT2"),
            },
            lookback_days=120,
        )
    }
    FORMULA_ADJUST_MAP: ClassVar[dict[str, int]] = {"": 0, "qfq": 1, "hfq": 2}

    @staticmethod
    def list_indicators() -> list[TdxLocalIndicatorSpec]:
        return list(TdxLocalIndicatorProvider.INDICATOR_REGISTRY.values())

    @staticmethod
    def discover_indicator_candidates(tdx_tqcenter_path: str = "") -> tuple[list[TdxLocalIndicatorSpec], str]:
        normalized_path = str(tdx_tqcenter_path).strip().strip('"')
        if not normalized_path:
            return [], "请先设置通达信安装目录。"
        return (
            TdxLocalIndicatorProvider.list_indicators(),
            "当前未发现通达信提供稳定的本地公式枚举接口，已展示系统内置支持项；若未命中，请手动输入公式名称和输出标识符。",
        )

    @staticmethod
    def build_manual_spec(
        *,
        indicator_key: str,
        formula_name: str,
        display_name: str | None = None,
        output_map: dict[str, str] | None = None,
    ) -> TdxLocalIndicatorSpec:
        normalized_key = str(indicator_key).strip() or "custom_indicator"
        normalized_formula = str(formula_name).strip()
        if not normalized_formula:
            raise ValueError("公式名称不能为空。")
        if not output_map:
            raise ValueError("输出映射不能为空。")
        output_candidates = {
            target_column: (str(source_key).strip(),)
            for target_column, source_key in output_map.items()
            if str(target_column).strip() and str(source_key).strip()
        }
        if not output_candidates:
            raise ValueError("输出映射不能为空。")
        return TdxLocalIndicatorSpec(
            key=normalized_key,
            display_name=str(display_name or normalized_formula).strip() or normalized_formula,
            formula_name=normalized_formula,
            output_candidates=output_candidates,
        )

    @staticmethod
    def get_indicator_spec(indicator_key: str) -> TdxLocalIndicatorSpec:
        spec = TdxLocalIndicatorProvider.INDICATOR_REGISTRY.get(str(indicator_key).strip())
        if spec is None:
            supported = ", ".join(sorted(TdxLocalIndicatorProvider.INDICATOR_REGISTRY))
            raise ValueError(f"不支持的本地指标: {indicator_key}。可选: {supported}")
        return spec

    @staticmethod
    def _fetch_formula_payload(
        *,
        tq: Any,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str,
        spec: TdxLocalIndicatorSpec,
    ) -> Any:
        start_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(start_date))).normalize()
        history_start = cast(pd.Timestamp, start_ts - pd.Timedelta(days=spec.lookback_days))
        formatted_start = history_start.strftime("%Y%m%d")
        formatted_end = TdxQuantProvider._format_market_time(end_date)

        dividend_type = TdxLocalIndicatorProvider.FORMULA_ADJUST_MAP.get(adjust)
        if dividend_type is None:
            raise ValueError("adjust 仅支持 qfq、hfq 或空字符串。")

        return_count = max(1, min(24000, (cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(end_date))).normalize() - history_start).days + 5))
        formula_payload = tq.formula_process_mul_zb(
            formula_name=spec.formula_name,
            formula_arg=spec.formula_arg,
            xsflag=-1,
            return_count=return_count,
            return_date=True,
            stock_list=[symbol],
            stock_period=spec.stock_period,
            start_time=formatted_start,
            end_time=formatted_end,
            count=0,
            dividend_type=dividend_type,
        )
        if not isinstance(formula_payload, dict):
            raise ValueError(f"formula_process_mul_zb returned {type(formula_payload).__name__}")
        return formula_payload

    @staticmethod
    def _normalize_formula_payload(
        *,
        payload: Any,
        symbol: str,
        spec: TdxLocalIndicatorSpec,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        if not isinstance(payload, dict):
            raise ValueError(f"formula payload must be dict, got {type(payload).__name__}")
        if str(payload.get("ErrorId", "0")) != "0":
            raise ValueError(str(payload.get("Error") or payload.get("Msg") or "公式调用失败"))

        data = payload.get(symbol)
        if not isinstance(data, dict):
            raise ValueError(f"formula payload missing symbol block: {symbol}")

        series_frames: list[pd.DataFrame] = []
        for output_column, candidate_keys in spec.output_candidates.items():
            matched_key = next((key for key in candidate_keys if isinstance(data.get(key), list)), None)
            if matched_key is None:
                raise ValueError(
                    f"formula output missing for {output_column}; tried keys={list(candidate_keys)}"
                )
            records = data.get(matched_key)
            if not isinstance(records, list):
                raise ValueError(f"formula field {matched_key} invalid")
            frame = pd.DataFrame(records)
            if "Date" not in frame.columns or "Value" not in frame.columns:
                raise ValueError(f"formula field {matched_key} missing Date/Value")
            frame = frame.rename(columns={"Date": "formula_date", "Value": output_column})
            frame["formula_date"] = pd.to_datetime(frame["formula_date"], errors="coerce")
            frame[output_column] = pd.to_numeric(frame[output_column], errors="coerce")
            frame = cast(pd.DataFrame, frame.dropna(subset=["formula_date"]).copy())
            series_frames.append(cast(pd.DataFrame, frame[["formula_date", output_column]]))

        if not series_frames:
            return pd.DataFrame(columns=pd.Index(["date", *spec.output_candidates.keys()]))

        frame = series_frames[0]
        for extra in series_frames[1:]:
            frame = frame.merge(extra, on="formula_date", how="outer")
        frame = frame.sort_values("formula_date").reset_index(drop=True)
        if frame.empty:
            return pd.DataFrame(columns=pd.Index(["date", *spec.output_candidates.keys()]))

        frame["date"] = cast(pd.Series, frame["formula_date"].shift(-1))
        frame = frame.dropna(subset=["date"]).drop(columns=["formula_date"]).reset_index(drop=True)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")

        start_ts, end_ts = TdxQuantProvider._build_filter_window(start_date, end_date)
        start_day = start_ts.normalize()
        end_day = end_ts.normalize()
        frame = frame.loc[frame["date"].between(start_day, end_day)].copy()
        for column in spec.output_candidates.keys():
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame[["date", *spec.output_candidates.keys()]].sort_values("date").reset_index(drop=True)
        return frame

    @staticmethod
    def import_indicator(
        *,
        symbol: str,
        indicator_key: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        formula_name: str | None = None,
        output_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        standardized_symbol = AkshareProvider.to_standard_symbol(symbol)
        spec = (
            TdxLocalIndicatorProvider.build_manual_spec(
                indicator_key=indicator_key,
                formula_name=str(formula_name or "").strip(),
                output_map=output_map,
            )
            if str(formula_name or "").strip()
            else TdxLocalIndicatorProvider.get_indicator_spec(indicator_key)
        )
        tq = TdxQuantProvider._ensure_initialized()

        try:
            payload = TdxLocalIndicatorProvider._fetch_formula_payload(
                tq=tq,
                symbol=standardized_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                spec=spec,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"TDX 本地指标导入失败（fetch阶段）：{spec.display_name}。根因: {exc.__class__.__name__}: {exc}"
            ) from exc

        try:
            result = TdxLocalIndicatorProvider._normalize_formula_payload(
                payload=payload,
                symbol=standardized_symbol,
                spec=spec,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"TDX 本地指标导入失败（normalize阶段）：{spec.display_name}。根因: {exc.__class__.__name__}: {exc}"
            ) from exc

        result["symbol"] = standardized_symbol
        return cast(pd.DataFrame, result[["date", "symbol", *spec.output_candidates.keys()]])
