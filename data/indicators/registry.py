from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IndicatorCapability:
    allow_scan: bool = False
    allow_filter: bool = False
    allow_exit: bool = False


@dataclass(frozen=True)
class IndicatorSourceType:
    value: str


MARKET_NATIVE = IndicatorSourceType("market_native")
TDX_FORMULA_LOCAL = IndicatorSourceType("tdx_formula_local")
COMPUTED_FEATURE = IndicatorSourceType("computed_feature")


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    display_name: str
    source_type: str
    output_candidates: dict[str, tuple[str, ...]]
    formula_name: str = ""
    formula_arg: str = ""
    stock_period: str = "1d"
    lookback_days: int = 120
    align_rule: str = "next_trade_date"
    required_timeframe: str = "1d"
    lookahead_policy: str = "shift_next_bar"
    allow_scan: bool = False
    allow_filter: bool = False
    allow_exit: bool = False
    storage_subdir: str = ""
    description: str = ""

    @property
    def output_columns(self) -> tuple[str, ...]:
        return tuple(self.output_candidates.keys())

    @property
    def storage_key(self) -> str:
        return self.storage_subdir or self.key


INDICATOR_REGISTRY: dict[str, IndicatorSpec] = {
    "board_ma": IndicatorSpec(
        key="board_ma",
        display_name="板块均线",
        source_type=TDX_FORMULA_LOCAL.value,
        formula_name="板块均线",
        output_candidates={
            "board_ma_ratio_20": ("均20占比", "NOTEXT1"),
            "board_ma_ratio_50": ("均50占比", "NOTEXT2"),
        },
        lookback_days=120,
        align_rule="next_trade_date",
        required_timeframe="1d",
        lookahead_policy="shift_next_bar",
        allow_filter=True,
        allow_exit=True,
        storage_subdir="board_ma",
        description="通达信本地公式导入的板块均线占比指标。",
    )
}


def list_indicator_specs() -> list[IndicatorSpec]:
    return list(INDICATOR_REGISTRY.values())


def get_indicator_spec(indicator_key: str) -> IndicatorSpec:
    spec = INDICATOR_REGISTRY.get(str(indicator_key).strip())
    if spec is None:
        supported = ", ".join(sorted(INDICATOR_REGISTRY))
        raise ValueError(f"不支持的指标: {indicator_key}。可选: {supported}")
    return spec


def build_manual_indicator_spec(
    *,
    indicator_key: str,
    formula_name: str,
    display_name: str | None = None,
    output_map: dict[str, str] | None = None,
    required_timeframe: str = "1d",
) -> IndicatorSpec:
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
    return IndicatorSpec(
        key=normalized_key,
        display_name=str(display_name or normalized_formula).strip() or normalized_formula,
        source_type=TDX_FORMULA_LOCAL.value,
        formula_name=normalized_formula,
        output_candidates=output_candidates,
        required_timeframe=required_timeframe,
        storage_subdir=normalized_key,
        description="手动指定通达信本地公式导入项。",
    )
