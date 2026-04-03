from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import importlib
import os
from pathlib import Path
import re
import sys
from typing import Any, ClassVar, cast

import pandas as pd

from data.providers.akshare_provider import AkshareProvider


@dataclass(frozen=True)
class TdxOfficialFetchDiagnostics:
    symbol: str
    timeframe: str
    adjust: str
    start_date: str
    end_date: str
    formatted_start_time: str
    formatted_end_time: str
    request_kwargs: dict[str, Any]
    raw_payload_type: str
    raw_payload_summary: str
    returned_keys: list[str]
    field_shapes: dict[str, str]
    symbol_column_presence: dict[str, bool]
    raw_row_count: int
    assembled_row_count: int
    normalized_row_count: int
    dropped_row_count: int
    failure_stage: str
    failure_message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TdxOfficialFetchResult:
    bars: pd.DataFrame
    diagnostics: TdxOfficialFetchDiagnostics


class TdxOfficialStandaloneError(RuntimeError):
    def __init__(self, message: str, diagnostics: TdxOfficialFetchDiagnostics):
        super().__init__(message)
        self.diagnostics = diagnostics


class _NormalizePayloadError(ValueError):
    def __init__(self, message: str, *, symbol_column_presence: dict[str, bool], raw_row_count: int, assembled_row_count: int):
        super().__init__(message)
        self.symbol_column_presence = symbol_column_presence
        self.raw_row_count = raw_row_count
        self.assembled_row_count = assembled_row_count


@dataclass(frozen=True)
class TdxOfficialStandaloneProvider:
    """Standalone official TDX minute-bar fetcher for manual testing."""

    TIMEFRAME_PERIODS: ClassVar[dict[str, str]] = {"1m": "1m", "5m": "5m"}
    ADJUST_MAP: ClassVar[dict[str, str]] = {"": "none", "qfq": "front", "hfq": "back"}
    REQUIRED_FIELDS: ClassVar[tuple[str, ...]] = ("Open", "High", "Low", "Close", "Volume", "Amount")
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
        return pd.DataFrame(columns=pd.Index(["date", "symbol", "open", "high", "low", "close", "volume", "amount"]))

    @staticmethod
    def _initial_diagnostics(
        *,
        symbol: str,
        timeframe: str,
        adjust: str,
        start_date: str,
        end_date: str,
    ) -> TdxOfficialFetchDiagnostics:
        return TdxOfficialFetchDiagnostics(
            symbol=symbol,
            timeframe=timeframe,
            adjust=adjust,
            start_date=start_date,
            end_date=end_date,
            formatted_start_time="",
            formatted_end_time="",
            request_kwargs={},
            raw_payload_type="",
            raw_payload_summary="",
            returned_keys=[],
            field_shapes={},
            symbol_column_presence={},
            raw_row_count=0,
            assembled_row_count=0,
            normalized_row_count=0,
            dropped_row_count=0,
            failure_stage="",
            failure_message="",
        )

    @staticmethod
    def _update_diagnostics(base: TdxOfficialFetchDiagnostics, **changes: Any) -> TdxOfficialFetchDiagnostics:
        payload = base.to_dict()
        payload.update(changes)
        return TdxOfficialFetchDiagnostics(**payload)

    @staticmethod
    def _has_explicit_time(date_text: str) -> bool:
        return bool(re.search(r"\d{1,2}:\d{2}", str(date_text).strip()))

    @staticmethod
    def _format_market_time(value: str) -> str:
        parsed = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(value)))
        has_explicit_time = TdxOfficialStandaloneProvider._has_explicit_time(value)
        return parsed.strftime("%Y%m%d%H%M%S" if has_explicit_time else "%Y%m%d")

    @staticmethod
    def _build_filter_window(start_date: str, end_date: str) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(start_date)))
        end_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(end_date)))
        if not TdxOfficialStandaloneProvider._has_explicit_time(start_date):
            start_ts = cast(pd.Timestamp, start_ts.normalize())
        if not TdxOfficialStandaloneProvider._has_explicit_time(end_date):
            end_ts = cast(pd.Timestamp, end_ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1))
        return start_ts, end_ts

    @staticmethod
    def _explicit_import_paths(override_path: str | None = None) -> list[Path]:
        raw_value = (override_path or os.getenv(TdxOfficialStandaloneProvider.TQCENTER_ENV_VAR, "")).strip()
        if not raw_value:
            return []

        def _expand_candidate(candidate: Path) -> list[Path]:
            normalized = candidate
            if normalized.name.lower() == "tqcenter.py":
                normalized = normalized.parent

            if normalized.name.lower() == "user" and normalized.parent.name.lower() == "pyplugins":
                return [normalized]
            if normalized.name.lower() == "pyplugins":
                return [normalized / "user", normalized]
            return [normalized / "PYPlugins" / "user", normalized]

        out: list[Path] = []
        seen: set[str] = set()
        for item in raw_value.split(os.pathsep):
            text = item.strip().strip('"')
            if not text:
                continue
            for expanded in _expand_candidate(Path(text).expanduser()):
                key = str(expanded)
                if key in seen:
                    continue
                seen.add(key)
                out.append(expanded)
        return out

    @staticmethod
    def _import_tq(override_path: str | None = None) -> Any:
        if TdxOfficialStandaloneProvider._tq is not None:
            return TdxOfficialStandaloneProvider._tq

        errors: list[str] = []
        for path in TdxOfficialStandaloneProvider._explicit_import_paths(override_path):
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
                TdxOfficialStandaloneProvider._tq = tq
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
            TdxOfficialStandaloneProvider._tq = tq
            return tq
        except Exception as exc:  # noqa: BLE001
            errors.append(f"normal import: {exc}")
            raise ImportError(
                "无法导入 tqcenter。请先安装并登录本机通达信终端，并通过"
                f" {TdxOfficialStandaloneProvider.TQCENTER_ENV_VAR} 或 --tdx-tqcenter-path 指向 PYPlugins/user。"
                f" 详情: {' | '.join(errors)}"
            ) from exc

    @staticmethod
    def _ensure_initialized(tqcenter_path: str | None = None) -> Any:
        tq = TdxOfficialStandaloneProvider._import_tq(tqcenter_path)
        if TdxOfficialStandaloneProvider._initialized:
            return tq

        tq.initialize(__file__)
        TdxOfficialStandaloneProvider._initialized = True
        return tq

    @staticmethod
    def _resolve_field_key(payload: Mapping[str, Any], canonical_field: str) -> str | None:
        aliases = TdxOfficialStandaloneProvider.FIELD_ALIASES[canonical_field]
        for key in aliases:
            if key in payload:
                return key
        return None

    @staticmethod
    def _field_shapes(payload: Mapping[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in payload.items():
            if isinstance(value, pd.DataFrame):
                out[str(key)] = f"shape={value.shape}"
            elif isinstance(value, pd.Series):
                out[str(key)] = f"series_len={len(value)}"
            else:
                out[str(key)] = f"type={type(value).__name__}"
        return out

    @staticmethod
    def _raw_payload_summary(raw_data: Any) -> tuple[str, str, list[str], dict[str, str]]:
        payload_type = type(raw_data).__name__
        if raw_data is None:
            return payload_type, "none", [], {}
        if isinstance(raw_data, Mapping):
            keys = [str(k) for k in raw_data.keys()]
            return payload_type, f"mapping_keys={keys[:8]},size={len(raw_data)}", keys, TdxOfficialStandaloneProvider._field_shapes(raw_data)
        if isinstance(raw_data, pd.DataFrame):
            return payload_type, f"shape={raw_data.shape}", [], {}
        if isinstance(raw_data, pd.Series):
            return payload_type, f"series_len={len(raw_data)}", [], {}
        if isinstance(raw_data, (list, tuple)):
            return payload_type, f"sequence_len={len(raw_data)}", [], {}
        return payload_type, "scalar_or_unknown", [], {}

    @staticmethod
    def _assemble_field_centric_bars(
        *,
        raw_data: Any,
        standardized_symbol: str,
        start_date: str,
        end_date: str,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        if raw_data is None:
            return TdxOfficialStandaloneProvider._empty_bars(), {
                "raw_row_count": 0,
                "assembled_row_count": 0,
                "symbol_column_presence": {field: False for field in TdxOfficialStandaloneProvider.REQUIRED_FIELDS},
            }

        if not isinstance(raw_data, Mapping):
            raise _NormalizePayloadError(
                f"official payload must be dict[field]->DataFrame, got {type(raw_data).__name__}",
                symbol_column_presence={field: False for field in TdxOfficialStandaloneProvider.REQUIRED_FIELDS},
                raw_row_count=0,
                assembled_row_count=0,
            )

        payload = cast(Mapping[str, Any], raw_data)
        if not payload:
            return TdxOfficialStandaloneProvider._empty_bars(), {
                "raw_row_count": 0,
                "assembled_row_count": 0,
                "symbol_column_presence": {field: False for field in TdxOfficialStandaloneProvider.REQUIRED_FIELDS},
            }

        missing_fields: list[str] = []
        selected_frames: dict[str, pd.DataFrame] = {}
        symbol_column_presence: dict[str, bool] = {}
        for field in TdxOfficialStandaloneProvider.REQUIRED_FIELDS:
            resolved_key = TdxOfficialStandaloneProvider._resolve_field_key(payload, field)
            if resolved_key is None:
                missing_fields.append(field)
                symbol_column_presence[field] = False
                continue
            value = payload[resolved_key]
            if not isinstance(value, pd.DataFrame):
                raise _NormalizePayloadError(
                    f"field {resolved_key} must be DataFrame, got {type(value).__name__}",
                    symbol_column_presence=symbol_column_presence,
                    raw_row_count=0,
                    assembled_row_count=0,
                )
            selected_frames[field] = value
            has_symbol = standardized_symbol in {str(c) for c in value.columns}
            symbol_column_presence[field] = has_symbol

        if missing_fields:
            raise _NormalizePayloadError(
                f"missing required fields: {missing_fields}",
                symbol_column_presence=symbol_column_presence,
                raw_row_count=0,
                assembled_row_count=0,
            )

        missing_symbol_fields = [field for field, present in symbol_column_presence.items() if not present]
        if missing_symbol_fields:
            raise _NormalizePayloadError(
                f"missing symbol column {standardized_symbol} in fields: {missing_symbol_fields}",
                symbol_column_presence=symbol_column_presence,
                raw_row_count=0,
                assembled_row_count=0,
            )

        if all(frame.empty for frame in selected_frames.values()):
            return TdxOfficialStandaloneProvider._empty_bars(), {
                "raw_row_count": 0,
                "assembled_row_count": 0,
                "symbol_column_presence": symbol_column_presence,
            }

        series_map: dict[str, pd.Series] = {}
        raw_row_count = 0
        for field, frame in selected_frames.items():
            col_map = {str(c): c for c in frame.columns}
            symbol_col = col_map[standardized_symbol]
            raw_row_count = max(raw_row_count, int(len(frame)))
            raw_series = cast(pd.Series, frame[symbol_col])
            numeric_values = pd.to_numeric(raw_series.values, errors="coerce")
            series = pd.Series(numeric_values, index=pd.to_datetime(frame.index, errors="coerce"), name=field)
            series_map[field] = series

        assembled = pd.concat(series_map, axis=1).reset_index().rename(columns={"index": "date"})
        assembled_row_count = int(len(assembled))
        assembled = assembled.rename(columns=TdxOfficialStandaloneProvider.OUTPUT_RENAME)

        assembled["date"] = pd.to_datetime(assembled["date"], errors="coerce")
        start_ts, end_ts = TdxOfficialStandaloneProvider._build_filter_window(start_date, end_date)
        assembled = assembled[assembled["date"].between(start_ts, end_ts)]

        for column in ["open", "high", "low", "close", "volume", "amount"]:
            assembled[column] = pd.to_numeric(assembled[column], errors="coerce")

        assembled["symbol"] = standardized_symbol
        required_columns = cast(pd.DataFrame, assembled[["date", "open", "high", "low", "close"]])
        normalized = assembled.loc[required_columns.notna().all(axis=1)].copy()
        normalized = normalized[["date", "symbol", "open", "high", "low", "close", "volume", "amount"]]
        normalized = normalized.loc[~normalized["date"].duplicated(keep="last")].copy()
        normalized = normalized.sort_values("date").reset_index(drop=True)

        return cast(pd.DataFrame, normalized), {
            "raw_row_count": raw_row_count,
            "assembled_row_count": assembled_row_count,
            "symbol_column_presence": symbol_column_presence,
        }

    @staticmethod
    def fetch_bars_with_diagnostics(
        *,
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        tqcenter_path: str | None = None,
    ) -> TdxOfficialFetchResult:
        standardized_symbol = AkshareProvider.to_standard_symbol(symbol)
        diagnostics = TdxOfficialStandaloneProvider._initial_diagnostics(
            symbol=standardized_symbol,
            timeframe=timeframe,
            adjust=adjust,
            start_date=start_date,
            end_date=end_date,
        )

        period = TdxOfficialStandaloneProvider.TIMEFRAME_PERIODS.get(timeframe)
        if period is None:
            diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
                diagnostics,
                failure_stage="validate",
                failure_message="timeframe 仅支持 1m 或 5m。",
            )
            raise TdxOfficialStandaloneError("timeframe 仅支持 1m 或 5m。", diagnostics)

        dividend_type = TdxOfficialStandaloneProvider.ADJUST_MAP.get(adjust)
        if dividend_type is None:
            diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
                diagnostics,
                failure_stage="validate",
                failure_message="adjust 仅支持 qfq、hfq 或空字符串。",
            )
            raise TdxOfficialStandaloneError("adjust 仅支持 qfq、hfq 或空字符串。", diagnostics)

        formatted_start = TdxOfficialStandaloneProvider._format_market_time(start_date)
        formatted_end = TdxOfficialStandaloneProvider._format_market_time(end_date)
        request_kwargs: dict[str, Any] = {
            "field_list": list(TdxOfficialStandaloneProvider.REQUIRED_FIELDS),
            "stock_list": [standardized_symbol],
            "period": period,
            "start_time": formatted_start,
            "end_time": formatted_end,
            "count": TdxOfficialStandaloneProvider.DEFAULT_COUNT,
            "dividend_type": dividend_type,
            "fill_data": TdxOfficialStandaloneProvider.DEFAULT_FILL_DATA,
        }
        diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
            diagnostics,
            formatted_start_time=formatted_start,
            formatted_end_time=formatted_end,
            request_kwargs=request_kwargs,
        )

        try:
            tq = TdxOfficialStandaloneProvider._ensure_initialized(tqcenter_path)
        except Exception as exc:  # noqa: BLE001
            diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
                diagnostics,
                failure_stage="init",
                failure_message=f"{exc.__class__.__name__}: {exc}",
            )
            raise TdxOfficialStandaloneError(
                "TDX 官方接口初始化失败（init阶段）。请确认终端已启动登录且 tqcenter 可导入。", diagnostics
            ) from exc

        try:
            raw_data = tq.get_market_data(**request_kwargs)
        except Exception as exc:  # noqa: BLE001
            diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
                diagnostics,
                failure_stage="fetch",
                failure_message=f"{exc.__class__.__name__}: {exc}",
            )
            raise TdxOfficialStandaloneError("TDX 官方行情下载失败（fetch阶段）。", diagnostics) from exc

        payload_type, payload_summary, returned_keys, field_shapes = TdxOfficialStandaloneProvider._raw_payload_summary(raw_data)
        diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
            diagnostics,
            raw_payload_type=payload_type,
            raw_payload_summary=payload_summary,
            returned_keys=returned_keys,
            field_shapes=field_shapes,
        )

        try:
            normalized, normalize_meta = TdxOfficialStandaloneProvider._assemble_field_centric_bars(
                raw_data=raw_data,
                standardized_symbol=standardized_symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except _NormalizePayloadError as exc:
            diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
                diagnostics,
                raw_row_count=exc.raw_row_count,
                assembled_row_count=exc.assembled_row_count,
                symbol_column_presence=exc.symbol_column_presence,
                failure_stage="normalize",
                failure_message=f"{exc.__class__.__name__}: {exc}",
            )
            raise TdxOfficialStandaloneError("TDX 官方行情标准化失败（normalize阶段）。", diagnostics) from exc
        except Exception as exc:  # noqa: BLE001
            diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
                diagnostics,
                failure_stage="normalize",
                failure_message=f"{exc.__class__.__name__}: {exc}",
            )
            raise TdxOfficialStandaloneError("TDX 官方行情标准化失败（normalize阶段）。", diagnostics) from exc

        normalized_row_count = int(len(normalized))
        assembled_row_count = int(normalize_meta["assembled_row_count"])
        diagnostics = TdxOfficialStandaloneProvider._update_diagnostics(
            diagnostics,
            raw_row_count=int(normalize_meta["raw_row_count"]),
            assembled_row_count=assembled_row_count,
            normalized_row_count=normalized_row_count,
            dropped_row_count=max(0, assembled_row_count - normalized_row_count),
            symbol_column_presence=cast(dict[str, bool], normalize_meta["symbol_column_presence"]),
        )
        return TdxOfficialFetchResult(bars=normalized, diagnostics=diagnostics)

    @staticmethod
    def fetch_bars(
        *,
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        tqcenter_path: str | None = None,
    ) -> pd.DataFrame:
        result = TdxOfficialStandaloneProvider.fetch_bars_with_diagnostics(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            tqcenter_path=tqcenter_path,
        )
        return result.bars
