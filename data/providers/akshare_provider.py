from __future__ import annotations

from dataclasses import dataclass
import importlib
import re
from typing import Any, Callable, cast

import pandas as pd


@dataclass(frozen=True)
class AkshareProvider:
    """akshare 调用封装层（仅此模块允许直接依赖 akshare）。"""

    MINUTE_PERIOD_MAP = {"5m": "5", "15m": "15", "30m": "30"}

    @staticmethod
    def to_standard_symbol(raw_symbol: str) -> str:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            raise ValueError("symbol 不能为空")

        if "." in symbol:
            code, market = symbol.split(".", 1)
            if len(code) == 6 and market in {"SZ", "SH", "BJ"}:
                return f"{code}.{market}"

        if symbol.startswith("SZ") and len(symbol) == 8 and symbol[2:].isdigit():
            return f"{symbol[2:]}.SZ"
        if symbol.startswith("SH") and len(symbol) == 8 and symbol[2:].isdigit():
            return f"{symbol[2:]}.SH"
        if symbol.startswith("BJ") and len(symbol) == 8 and symbol[2:].isdigit():
            return f"{symbol[2:]}.BJ"

        if len(symbol) == 6 and symbol.isdigit():
            if symbol.startswith(("5", "6", "9")):
                return f"{symbol}.SH"
            if symbol.startswith(("0", "1", "2", "3")):
                return f"{symbol}.SZ"
            if symbol.startswith(("4", "8")):
                return f"{symbol}.BJ"

        raise ValueError(f"无法识别 symbol 格式: {raw_symbol}")

    @staticmethod
    def to_akshare_symbol(standard_symbol: str) -> str:
        # 东财源 stock_zh_a_hist 使用 6 位代码
        code, _market = AkshareProvider.to_standard_symbol(standard_symbol).split(".", 1)
        return code

    @staticmethod
    def to_akshare_prefixed_symbol(standard_symbol: str) -> str:
        # 新浪/腾讯常用 sz000001 / sh600519
        code, market = AkshareProvider.to_standard_symbol(standard_symbol).split(".", 1)
        return f"{market.lower()}{code}"

    @staticmethod
    def infer_asset_type(standard_symbol: str) -> str:
        code, market = AkshareProvider.to_standard_symbol(standard_symbol).split(".", 1)
        if market == "SH" and code.startswith("5"):
            return "etf"
        if market == "SZ" and code.startswith(("15", "16", "18")):
            return "etf"
        if market == "SH" and code.startswith(("000", "880", "930", "931", "932", "950")):
            return "index"
        if market == "SZ" and code.startswith(("399", "980")):
            return "index"
        return "stock"

    @staticmethod
    def _empty_bars() -> pd.DataFrame:
        return pd.DataFrame(columns=pd.Index(["date", "symbol", "open", "high", "low", "close", "volume", "amount"]))

    @staticmethod
    def _import_akshare() -> Any:
        return importlib.import_module("akshare")

    @staticmethod
    def _has_explicit_time(date_text: str) -> bool:
        return bool(re.search(r"\d{1,2}:\d{2}", str(date_text).strip()))

    @staticmethod
    def _build_filter_window(start_date: str, end_date: str) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(start_date)))
        end_ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(end_date)))
        if not AkshareProvider._has_explicit_time(start_date):
            start_ts = cast(pd.Timestamp, start_ts.normalize())
        if not AkshareProvider._has_explicit_time(end_date):
            end_ts = cast(
                pd.Timestamp,
                end_ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1),
            )
        return start_ts, end_ts

    @staticmethod
    def _normalize_bars(raw_df: pd.DataFrame, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        if raw_df.empty:
            return AkshareProvider._empty_bars()

        mapping = {
            "日期": "date",
            "时间": "date",
            "日期时间": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "datetime": "date",
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
        }

        out = raw_df.rename(columns=mapping).copy()
        if "date" not in out.columns:
            out = out.reset_index()
            first_column = str(out.columns[0])
            out = out.rename(columns={first_column: "date"})

        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        start_ts, end_ts = AkshareProvider._build_filter_window(start_date, end_date)
        out = out[out["date"].between(start_ts, end_ts)]

        for required in ["open", "high", "low", "close", "volume", "amount"]:
            if required not in out.columns:
                out[required] = pd.NA
            out[required] = pd.to_numeric(out[required], errors="coerce")

        out["symbol"] = AkshareProvider.to_standard_symbol(symbol)
        selected = cast(pd.DataFrame, out[["date", "symbol", "open", "high", "low", "close", "volume", "amount"]].copy())
        return cast(pd.DataFrame, selected.reset_index(drop=True))

    @staticmethod
    def _format_minute_api_datetime(date_text: str, *, is_end: bool) -> str:
        ts = cast(pd.Timestamp, pd.Timestamp(pd.to_datetime(date_text)))
        if not AkshareProvider._has_explicit_time(date_text):
            ts = cast(pd.Timestamp, ts.normalize())
            if is_end:
                ts = cast(pd.Timestamp, ts + pd.Timedelta(hours=23, minutes=59, seconds=59))
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def fetch_symbol_list() -> pd.DataFrame:
        ak = AkshareProvider._import_akshare()

        df = ak.stock_info_a_code_name()
        if df.empty:
            return pd.DataFrame(columns=pd.Index(["symbol", "name"]))

        renamed = df.rename(columns={"code": "raw_code", "name": "name"}).copy()
        renamed["symbol"] = renamed["raw_code"].map(AkshareProvider.to_standard_symbol)
        return renamed[["symbol", "name"]].drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    @staticmethod
    def _fetch_from_sina(ak: Any, symbol: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        prefixed = AkshareProvider.to_akshare_prefixed_symbol(symbol)
        if not hasattr(ak, "stock_zh_a_daily"):
            raise AttributeError("akshare 不存在 stock_zh_a_daily")
        func = getattr(ak, "stock_zh_a_daily")
        return func(
            symbol=prefixed,
            start_date=pd.to_datetime(start_date).strftime("%Y%m%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y%m%d"),
            adjust=adjust,
        )

    @staticmethod
    def _fetch_from_tencent(ak: Any, symbol: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        prefixed = AkshareProvider.to_akshare_prefixed_symbol(symbol)
        for name in ["stock_zh_a_hist_tx", "stock_zh_a_daily_tx"]:
            if not hasattr(ak, name):
                continue
            func = getattr(ak, name)
            # 兼容不同版本签名
            for kwargs in (
                {
                    "symbol": prefixed,
                    "start_date": pd.to_datetime(start_date).strftime("%Y%m%d"),
                    "end_date": pd.to_datetime(end_date).strftime("%Y%m%d"),
                    "adjust": adjust,
                },
                {
                    "symbol": prefixed,
                    "start_date": pd.to_datetime(start_date).strftime("%Y%m%d"),
                    "end_date": pd.to_datetime(end_date).strftime("%Y%m%d"),
                },
            ):
                try:
                    return func(**kwargs)
                except TypeError:
                    continue
        raise AttributeError("akshare 不存在可用腾讯源接口")

    @staticmethod
    def _fetch_from_eastmoney(ak: Any, symbol: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        if not hasattr(ak, "stock_zh_a_hist"):
            raise AttributeError("akshare 不存在 stock_zh_a_hist")

        return ak.stock_zh_a_hist(
            symbol=AkshareProvider.to_akshare_symbol(symbol),
            period="daily",
            start_date=pd.to_datetime(start_date).strftime("%Y%m%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y%m%d"),
            adjust=adjust,
        )

    @staticmethod
    def _fetch_etf_from_eastmoney(
        ak: Any, symbol: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame:
        if not hasattr(ak, "fund_etf_hist_em"):
            raise AttributeError("akshare 不存在 fund_etf_hist_em")

        return ak.fund_etf_hist_em(
            symbol=AkshareProvider.to_akshare_symbol(symbol),
            period="daily",
            start_date=pd.to_datetime(start_date).strftime("%Y%m%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y%m%d"),
            adjust=adjust,
        )

    @staticmethod
    def _fetch_index_from_eastmoney(
        ak: Any, symbol: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame:
        del adjust
        if not hasattr(ak, "index_zh_a_hist"):
            raise AttributeError("akshare 不存在 index_zh_a_hist")

        return ak.index_zh_a_hist(
            symbol=AkshareProvider.to_akshare_symbol(symbol),
            period="daily",
            start_date=pd.to_datetime(start_date).strftime("%Y%m%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y%m%d"),
        )

    @staticmethod
    def _fetch_minute_from_eastmoney(
        ak: Any,
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        adjust: str,
    ) -> pd.DataFrame:
        period = AkshareProvider.MINUTE_PERIOD_MAP.get(timeframe)
        if period is None:
            raise ValueError(f"不支持的分钟周期: {timeframe}")
        if not hasattr(ak, "stock_zh_a_hist_min_em"):
            raise AttributeError("akshare 不存在 stock_zh_a_hist_min_em")

        return ak.stock_zh_a_hist_min_em(
            symbol=AkshareProvider.to_akshare_symbol(symbol),
            period=period,
            adjust=adjust,
            start_date=AkshareProvider._format_minute_api_datetime(start_date, is_end=False),
            end_date=AkshareProvider._format_minute_api_datetime(end_date, is_end=True),
        )

    @staticmethod
    def _resolve_sources(
        symbol: str,
    ) -> list[tuple[str, Callable[[Any, str, str, str, str], pd.DataFrame]]]:
        asset_type = AkshareProvider.infer_asset_type(symbol)
        if asset_type == "etf":
            return [
                ("etf_eastmoney", AkshareProvider._fetch_etf_from_eastmoney),
                ("sina", AkshareProvider._fetch_from_sina),
                ("tencent", AkshareProvider._fetch_from_tencent),
                ("eastmoney", AkshareProvider._fetch_from_eastmoney),
            ]
        if asset_type == "index":
            return [
                ("index_eastmoney", AkshareProvider._fetch_index_from_eastmoney),
                ("sina", AkshareProvider._fetch_from_sina),
                ("tencent", AkshareProvider._fetch_from_tencent),
                ("eastmoney", AkshareProvider._fetch_from_eastmoney),
            ]
        return [
            ("sina", AkshareProvider._fetch_from_sina),
            ("tencent", AkshareProvider._fetch_from_tencent),
            ("eastmoney", AkshareProvider._fetch_from_eastmoney),
        ]

    @staticmethod
    def fetch_daily_bars(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        ak = AkshareProvider._import_akshare()

        errors: list[str] = []
        sources = AkshareProvider._resolve_sources(symbol)

        for source_name, fetcher in sources:
            try:
                raw_df = fetcher(ak, symbol, start_date, end_date, adjust)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{source_name}: {exc}")
                continue

            normalized = AkshareProvider._normalize_bars(raw_df, symbol, start_date, end_date)
            if not normalized.empty:
                return normalized

        if errors:
            source_names = "->".join(source_name for source_name, _ in sources)
            raise RuntimeError(f"日线下载失败，已尝试{source_names}: " + " | ".join(errors))
        return AkshareProvider._empty_bars()

    @staticmethod
    def fetch_bars(
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        if timeframe == "1d":
            return AkshareProvider.fetch_daily_bars(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )

        if timeframe not in AkshareProvider.MINUTE_PERIOD_MAP:
            raise ValueError("timeframe 仅支持 1d、30m、15m、5m。")

        start_ts, end_ts = AkshareProvider._build_filter_window(start_date, end_date)
        max_supported_span = pd.Timedelta(days=7)
        if end_ts - start_ts > max_supported_span:
            raise ValueError("AKShare 分钟接口当前仅适合近 5 个交易日窗口，请缩小更新时间范围。")

        ak = AkshareProvider._import_akshare()

        raw_df = AkshareProvider._fetch_minute_from_eastmoney(
            ak=ak,
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
        return AkshareProvider._normalize_bars(raw_df, symbol, start_date, end_date)
