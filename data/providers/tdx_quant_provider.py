from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import importlib
import os
from pathlib import Path
import re
import sys
from typing import Any, ClassVar, cast

import pandas as pd

from data.providers.akshare_provider import AkshareProvider


@dataclass(frozen=True)
class TdxQuantProvider:
    """Official TDX Quant local data provider for daily and selected minute K-lines."""

    TIMEFRAME_PERIODS: ClassVar[dict[str, str]] = {
        "1d": "1d",
        "30m": "30m",
        "15m": "15m",
        "5m": "5m",
        "1m": "1m",
    }
    ADJUST_MAP: ClassVar[dict[str, str]] = {"": "none", "qfq": "front", "hfq": "back"}
    REQUIRED_FIELDS: ClassVar[tuple[str, ...]] = (
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Amount",
    )
    FIELD_ALIASES: ClassVar[dict[str, tuple[str, ...]]] = {
        "Open": ("Open", "open"),
        "High": ("High", "high"),
        "Low": ("Low", "low"),
        "Close": ("Close", "close"),
        "Volume": ("Volume", "volume", "vol"),
        "Amount": ("Amount", "amount"),
    }
    OUTPUT_RENAME: ClassVar[dict[str, str]] = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Amount": "amount",
    }
    DEFAULT_COUNT: ClassVar[int] = -1
    DEFAULT_FILL_DATA: ClassVar[bool] = False
    TQCENTER_ENV_VAR: ClassVar[str] = "TDX_TQCENTER_PATH"
    _tq: ClassVar[Any | None] = None
    _initialized: ClassVar[bool] = False

    @staticmethod
    def _empty_bars() -> pd.DataFrame:
        return pd.DataFrame(
            columns=pd.Index(
                ["date", "symbol", "open", "high", "low", "close", "volume", "amount"]
            )
        )

    @staticmethod
    def _has_explicit_time(date_text: str) -> bool:
        return bool(re.search(r"\d{1,2}:\d{2}", str(date_text).strip()))

    @staticmethod
    def _build_filter_window(
        start_date: str, end_date: str
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(start_date)))
        end_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(end_date)))
        if not TdxQuantProvider._has_explicit_time(start_date):
            start_ts = cast(pd.Timestamp, start_ts.normalize())
        if not TdxQuantProvider._has_explicit_time(end_date):
            end_ts = cast(
                pd.Timestamp,
                end_ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1),
            )
        return start_ts, end_ts

    @staticmethod
    def _explicit_import_paths() -> list[Path]:
        raw_value = os.getenv(TdxQuantProvider.TQCENTER_ENV_VAR, "").strip()
        if not raw_value:
            return []

        def _expand_candidate(candidate: Path) -> list[Path]:
            normalized = candidate
            if normalized.name.lower() == "tqcenter.py":
                normalized = normalized.parent

            candidates: list[Path] = []
            if (
                normalized.name.lower() == "user"
                and normalized.parent.name.lower() == "pyplugins"
            ):
                candidates.append(normalized)
            elif normalized.name.lower() == "pyplugins":
                candidates.append(normalized / "user")
                candidates.append(normalized)
            else:
                candidates.append(normalized / "PYPlugins" / "user")
                candidates.append(normalized)
            return candidates

        paths: list[Path] = []
        seen: set[str] = set()
        for item in raw_value.split(os.pathsep):
            text = item.strip().strip('"')
            if not text:
                continue
            base_candidate = Path(text).expanduser()
            for expanded in _expand_candidate(base_candidate):
                key = str(expanded)
                if key in seen:
                    continue
                seen.add(key)
                paths.append(expanded)
        return paths

    @staticmethod
    def _format_market_time(value: str) -> str:
        parsed = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(value)))
        has_explicit_time = TdxQuantProvider._has_explicit_time(value)
        return parsed.strftime("%Y%m%d%H%M%S" if has_explicit_time else "%Y%m%d")

    @staticmethod
    def _import_tq() -> Any:
        if TdxQuantProvider._tq is not None:
            return TdxQuantProvider._tq

        errors: list[str] = []
        for path in TdxQuantProvider._explicit_import_paths():
            resolved = path.resolve()
            if not resolved.exists():
                errors.append(f"{resolved} 不存在")
                continue

            path_text = str(resolved)
            inserted = False
            if path_text not in sys.path:
                sys.path.insert(0, path_text)
                inserted = True

            try:
                module = importlib.import_module("tqcenter")
                tq = getattr(module, "tq")
                TdxQuantProvider._tq = tq
                return tq
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{path_text}: {exc}")
                if inserted:
                    try:
                        sys.path.remove(path_text)
                    except ValueError:
                        pass

        try:
            module = importlib.import_module("tqcenter")
            tq = getattr(module, "tq")
            TdxQuantProvider._tq = tq
            return tq
        except Exception as exc:  # noqa: BLE001
            errors.append(f"normal import: {exc}")
            message = (
                "无法导入 tqcenter。请先安装并登录本机通达信终端，"
                f"并优先通过环境变量 {TdxQuantProvider.TQCENTER_ENV_VAR} 指向终端的 PYPlugins/user 目录。"
            )
            raise ImportError(message + " 详情: " + " | ".join(errors)) from exc

    @staticmethod
    def _ensure_initialized() -> Any:
        try:
            tq = TdxQuantProvider._import_tq()
        except ImportError as exc:
            raise ImportError(
                "无法导入 tqcenter。请先安装并登录本机通达信终端，并通过环境变量或默认导入路径暴露终端的 PYPlugins/user。"
            ) from exc
        if TdxQuantProvider._initialized:
            return tq

        try:
            tq.initialize(__file__)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "TDX Quant 初始化失败。请确认本机通达信终端已启动并登录，且 tqcenter 可从 PYPlugins/user 正常加载。"
            ) from exc

        TdxQuantProvider._initialized = True
        return tq

    @staticmethod
    def _resolve_field_key(
        payload: Mapping[str, Any], canonical_field: str
    ) -> str | None:
        aliases = TdxQuantProvider.FIELD_ALIASES[canonical_field]
        for key in aliases:
            if key in payload:
                return key
        return None

    @staticmethod
    def _normalize_bars(
        raw_data: Any, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        standardized_symbol = AkshareProvider.to_standard_symbol(symbol)
        if raw_data is None:
            return TdxQuantProvider._empty_bars()

        if not isinstance(raw_data, Mapping):
            raise ValueError(
                f"official payload must be dict[field]->DataFrame, got {type(raw_data).__name__}"
            )

        payload = cast(Mapping[str, Any], raw_data)
        if not payload:
            return TdxQuantProvider._empty_bars()

        missing_fields: list[str] = []
        selected_frames: dict[str, pd.DataFrame] = {}
        symbol_column_presence: dict[str, bool] = {}
        for field in TdxQuantProvider.REQUIRED_FIELDS:
            resolved_key = TdxQuantProvider._resolve_field_key(payload, field)
            if resolved_key is None:
                missing_fields.append(field)
                symbol_column_presence[field] = False
                continue
            value = payload[resolved_key]
            if not isinstance(value, pd.DataFrame):
                raise ValueError(
                    f"field {resolved_key} must be DataFrame, got {type(value).__name__}"
                )
            selected_frames[field] = value
            symbol_column_presence[field] = standardized_symbol in {
                str(c) for c in value.columns
            }

        if missing_fields:
            raise ValueError(f"missing required fields: {missing_fields}")

        missing_symbol_fields = [
            field for field, present in symbol_column_presence.items() if not present
        ]
        if missing_symbol_fields:
            raise ValueError(
                f"missing symbol column {standardized_symbol} in fields: {missing_symbol_fields}"
            )

        if all(frame.empty for frame in selected_frames.values()):
            return TdxQuantProvider._empty_bars()

        series_map: dict[str, pd.Series] = {}
        for field, frame in selected_frames.items():
            column_map = {str(c): c for c in frame.columns}
            symbol_column = column_map[standardized_symbol]
            raw_series = cast(pd.Series, frame[symbol_column])
            numeric_values = pd.to_numeric(raw_series.values, errors="coerce")
            series = pd.Series(
                numeric_values,
                index=pd.to_datetime(frame.index, errors="coerce"),
                name=field,
            )
            series_map[field] = series

        assembled = (
            pd.concat(series_map, axis=1)
            .reset_index()
            .rename(columns={"index": "date"})
        )
        assembled = assembled.rename(columns=TdxQuantProvider.OUTPUT_RENAME)
        assembled["date"] = pd.to_datetime(assembled["date"], errors="coerce")

        start_ts, end_ts = TdxQuantProvider._build_filter_window(start_date, end_date)
        assembled = assembled[assembled["date"].between(start_ts, end_ts)]

        for column in ["open", "high", "low", "close", "volume", "amount"]:
            assembled[column] = pd.to_numeric(assembled[column], errors="coerce")

        assembled["symbol"] = standardized_symbol
        required_columns = cast(
            pd.DataFrame, assembled[["date", "open", "high", "low", "close"]]
        )
        out = assembled.loc[required_columns.notna().all(axis=1)].copy()
        out = out[
            ["date", "symbol", "open", "high", "low", "close", "volume", "amount"]
        ]
        out = out.loc[~out["date"].duplicated(keep="last")].copy()
        out = out.sort_values("date").reset_index(drop=True)
        return cast(pd.DataFrame, out)

    @staticmethod
    def _fetch_market_data(
        tq: Any,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str,
        dividend_type: str,
    ) -> Any:
        formatted_start = TdxQuantProvider._format_market_time(start_date)
        formatted_end = TdxQuantProvider._format_market_time(end_date)
        request_kwargs = {
            "field_list": list(TdxQuantProvider.REQUIRED_FIELDS),
            "stock_list": [symbol],
            "start_time": formatted_start,
            "end_time": formatted_end,
            "period": period,
            "count": TdxQuantProvider.DEFAULT_COUNT,
            "dividend_type": dividend_type,
            "fill_data": TdxQuantProvider.DEFAULT_FILL_DATA,
        }
        return tq.get_market_data(**request_kwargs)

    @staticmethod
    def fetch_bars(
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        period = TdxQuantProvider.TIMEFRAME_PERIODS.get(timeframe)
        if period is None:
            raise ValueError("timeframe 仅支持 1d、30m、15m、1m 或 5m。")

        dividend_type = TdxQuantProvider.ADJUST_MAP.get(adjust)
        if dividend_type is None:
            raise ValueError("adjust 仅支持 qfq、hfq 或空字符串。")

        standardized_symbol = AkshareProvider.to_standard_symbol(symbol)
        tq = TdxQuantProvider._ensure_initialized()

        try:
            raw_data = TdxQuantProvider._fetch_market_data(
                tq=tq,
                symbol=standardized_symbol,
                start_date=start_date,
                end_date=end_date,
                period=period,
                dividend_type=dividend_type,
            )
        except Exception as exc:  # noqa: BLE001
            root_message = str(exc).strip()
            detail = (
                f"{exc.__class__.__name__}: {root_message}"
                if root_message
                else exc.__class__.__name__
            )
            raise RuntimeError(
                "TDX Quant 行情下载失败（fetch阶段）。请确认本机通达信终端已启动并登录，且目标日线/分钟线数据接口可用。"
                f" 根因: {detail}"
            ) from exc

        try:
            return TdxQuantProvider._normalize_bars(
                raw_data, standardized_symbol, start_date, end_date
            )
        except Exception as exc:  # noqa: BLE001
            root_message = str(exc).strip()
            detail = (
                f"{exc.__class__.__name__}: {root_message}"
                if root_message
                else exc.__class__.__name__
            )
            raise RuntimeError(
                "TDX Quant 行情标准化失败（normalize阶段）。请检查返回数据结构是否符合目标K线字段约定。"
                f" 根因: {detail}"
            ) from exc
