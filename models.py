from __future__ import annotations

from dataclasses import dataclass, field, replace

from config.strategy_capability import get_supported_strategy_timeframes


GAP_ENTRY_MODES = ("strict_break", "open_vs_prev_close_threshold")
ENTRY_FACTORS = (
    "gap",
    "trend_breakout",
    "volatility_contraction_breakout",
    "candle_run",
    "candle_run_acceleration",
    "early_surge_high_base",
)
SCAN_METRICS = (
    "signal_count",
    "closed_trade_candidates",
    "executed_trades",
    "strategy_win_rate_pct",
    "total_return_pct",
    "max_drawdown_pct",
    "final_net_value",
    "avg_holding_days",
    "profit_risk_ratio",
    "trade_return_volatility_pct",
)
SCAN_FIELD_CASTERS: dict[str, type[int] | type[float]] = {
    "gap_pct": float,
    "max_gap_filter_pct": float,
    "trend_breakout_lookback": int,
    "vcb_range_lookback": int,
    "vcb_breakout_lookback": int,
    "candle_run_length": int,
    "candle_run_min_body_pct": float,
    "candle_run_total_move_pct": float,
    "eshb_open_window_bars": int,
    "eshb_base_min_bars": int,
    "eshb_base_max_bars": int,
    "eshb_surge_min_pct": float,
    "eshb_max_base_pullback_pct": float,
    "eshb_max_base_range_pct": float,
    "eshb_max_anchor_breaks": int,
    "eshb_max_anchor_break_depth_pct": float,
    "eshb_min_open_volume_ratio": float,
    "eshb_min_breakout_volume_ratio": float,
    "eshb_trigger_buffer_pct": float,
    "atr_filter_period": int,
    "min_atr_filter_pct": float,
    "max_atr_filter_pct": float,
    "atr_trailing_period": int,
    "atr_trailing_multiplier": float,
    "time_stop_days": int,
    "time_stop_target_pct": float,
    "stop_loss_pct": float,
    "take_profit_pct": float,
    "profit_drawdown_pct": float,
    "min_profit_to_activate_profit_drawdown_pct": float,
    "exit_ma_period": int,
    "buy_slippage_pct": float,
    "sell_slippage_pct": float,
    "min_profit_to_activate_atr_trailing_pct": float,
    "partial_rule_1_target_profit_pct": float,
    "partial_rule_2_target_profit_pct": float,
    "partial_rule_3_target_profit_pct": float,
    "partial_rule_1_ma_period": int,
    "partial_rule_2_ma_period": int,
    "partial_rule_3_ma_period": int,
    "partial_rule_1_atr_period": int,
    "partial_rule_2_atr_period": int,
    "partial_rule_3_atr_period": int,
    "partial_rule_1_atr_multiplier": float,
    "partial_rule_2_atr_multiplier": float,
    "partial_rule_3_atr_multiplier": float,
    "partial_rule_1_drawdown_pct": float,
    "partial_rule_2_drawdown_pct": float,
    "partial_rule_3_drawdown_pct": float,
    "partial_rule_1_min_profit_to_activate_drawdown": float,
    "partial_rule_2_min_profit_to_activate_drawdown": float,
    "partial_rule_3_min_profit_to_activate_drawdown": float,
}
PARTIAL_EXIT_SCAN_FIELDS = frozenset(
    {
        "partial_rule_1_target_profit_pct",
        "partial_rule_2_target_profit_pct",
        "partial_rule_3_target_profit_pct",
        "partial_rule_1_ma_period",
        "partial_rule_2_ma_period",
        "partial_rule_3_ma_period",
        "partial_rule_1_atr_period",
        "partial_rule_2_atr_period",
        "partial_rule_3_atr_period",
        "partial_rule_1_atr_multiplier",
        "partial_rule_2_atr_multiplier",
        "partial_rule_3_atr_multiplier",
        "partial_rule_1_drawdown_pct",
        "partial_rule_2_drawdown_pct",
        "partial_rule_3_drawdown_pct",
        "partial_rule_1_min_profit_to_activate_drawdown",
        "partial_rule_2_min_profit_to_activate_drawdown",
        "partial_rule_3_min_profit_to_activate_drawdown",
    }
)
PARTIAL_EXIT_SCAN_FIELD_SPECS: dict[str, tuple[int, str, str]] = {
    "partial_rule_1_target_profit_pct": (1, "fixed_tp", "target_profit_pct"),
    "partial_rule_2_target_profit_pct": (2, "fixed_tp", "target_profit_pct"),
    "partial_rule_3_target_profit_pct": (3, "fixed_tp", "target_profit_pct"),
    "partial_rule_1_ma_period": (1, "ma_exit", "ma_period"),
    "partial_rule_2_ma_period": (2, "ma_exit", "ma_period"),
    "partial_rule_3_ma_period": (3, "ma_exit", "ma_period"),
    "partial_rule_1_atr_period": (1, "atr_trailing", "atr_period"),
    "partial_rule_2_atr_period": (2, "atr_trailing", "atr_period"),
    "partial_rule_3_atr_period": (3, "atr_trailing", "atr_period"),
    "partial_rule_1_atr_multiplier": (1, "atr_trailing", "atr_multiplier"),
    "partial_rule_2_atr_multiplier": (2, "atr_trailing", "atr_multiplier"),
    "partial_rule_3_atr_multiplier": (3, "atr_trailing", "atr_multiplier"),
    "partial_rule_1_drawdown_pct": (1, "profit_drawdown", "drawdown_pct"),
    "partial_rule_2_drawdown_pct": (2, "profit_drawdown", "drawdown_pct"),
    "partial_rule_3_drawdown_pct": (3, "profit_drawdown", "drawdown_pct"),
    "partial_rule_1_min_profit_to_activate_drawdown": (
        1,
        "profit_drawdown",
        "min_profit_to_activate_drawdown",
    ),
    "partial_rule_2_min_profit_to_activate_drawdown": (
        2,
        "profit_drawdown",
        "min_profit_to_activate_drawdown",
    ),
    "partial_rule_3_min_profit_to_activate_drawdown": (
        3,
        "profit_drawdown",
        "min_profit_to_activate_drawdown",
    ),
}
BASE_FACTOR_SCAN_FIELDS = frozenset(
    {
        "time_stop_days",
        "time_stop_target_pct",
        "atr_filter_period",
        "min_atr_filter_pct",
        "max_atr_filter_pct",
        "stop_loss_pct",
        "take_profit_pct",
        "profit_drawdown_pct",
        "min_profit_to_activate_profit_drawdown_pct",
        "exit_ma_period",
        "atr_trailing_period",
        "atr_trailing_multiplier",
        "min_profit_to_activate_atr_trailing_pct",
        "buy_slippage_pct",
        "sell_slippage_pct",
    }
)
FACTOR_SCAN_ELIGIBLE_FIELDS: dict[str, frozenset[str]] = {
    "gap": frozenset(
        {
            "gap_pct",
            "max_gap_filter_pct",
            *BASE_FACTOR_SCAN_FIELDS,
            *PARTIAL_EXIT_SCAN_FIELDS,
        }
    ),
    "trend_breakout": frozenset(
        {
            "trend_breakout_lookback",
            *BASE_FACTOR_SCAN_FIELDS,
            *PARTIAL_EXIT_SCAN_FIELDS,
        }
    ),
    "volatility_contraction_breakout": frozenset(
        {
            "vcb_range_lookback",
            "vcb_breakout_lookback",
            *BASE_FACTOR_SCAN_FIELDS,
            *PARTIAL_EXIT_SCAN_FIELDS,
        }
    ),
    "candle_run": frozenset(
        {
            "candle_run_length",
            "candle_run_min_body_pct",
            "candle_run_total_move_pct",
            *BASE_FACTOR_SCAN_FIELDS,
            *PARTIAL_EXIT_SCAN_FIELDS,
        }
    ),
    "candle_run_acceleration": frozenset(
        {
            "candle_run_length",
            "candle_run_min_body_pct",
            "candle_run_total_move_pct",
            *BASE_FACTOR_SCAN_FIELDS,
            *PARTIAL_EXIT_SCAN_FIELDS,
        }
    ),
    "early_surge_high_base": frozenset(
        {
            "eshb_open_window_bars",
            "eshb_base_min_bars",
            "eshb_base_max_bars",
            "eshb_surge_min_pct",
            "eshb_max_base_pullback_pct",
            "eshb_max_base_range_pct",
            "eshb_max_anchor_breaks",
            "eshb_max_anchor_break_depth_pct",
            "eshb_min_open_volume_ratio",
            "eshb_min_breakout_volume_ratio",
            "eshb_trigger_buffer_pct",
            *BASE_FACTOR_SCAN_FIELDS,
            *PARTIAL_EXIT_SCAN_FIELDS,
        }
    ),
}


EPSILON = 1e-12


@dataclass(frozen=True)
class PartialExitRule:
    enabled: bool
    weight_pct: float
    mode: str
    priority: int
    target_profit_pct: float | None = None
    ma_period: int | None = None
    drawdown_pct: float | None = None
    min_profit_to_activate_drawdown: float | None = None
    atr_period: int | None = None
    atr_multiplier: float | None = None
    indicator_key: str | None = None
    indicator_column: str | None = None
    indicator_operator: str | None = None
    indicator_threshold: float | None = None

    @property
    def weight_ratio(self) -> float:
        return self.weight_pct / 100.0

    @property
    def target_profit_ratio(self) -> float | None:
        if self.target_profit_pct is None:
            return None
        return self.target_profit_pct / 100.0

    @property
    def drawdown_ratio(self) -> float | None:
        if self.drawdown_pct is None:
            return None
        return self.drawdown_pct / 100.0

    @property
    def min_profit_to_activate_drawdown_ratio(self) -> float:
        threshold = self.min_profit_to_activate_drawdown
        if threshold is None:
            threshold = 5.0
        return threshold / 100.0


@dataclass(frozen=True)
class ImportedIndicatorRule:
    enabled: bool
    indicator_key: str
    column: str
    operator: str
    threshold: float
    priority: int = 1


@dataclass(frozen=True)
class TradeFill:
    sell_date: str
    sell_price: float
    weight: float
    exit_type: str
    holding_days: int


@dataclass(frozen=True)
class ParamScanAxis:
    field_name: str
    values: tuple[int | float, ...]


@dataclass(frozen=True)
class ParamScanConfig:
    enabled: bool = False
    axes: tuple[ParamScanAxis, ...] = ()
    metric: str = "total_return_pct"
    max_combinations: int = 25

    @property
    def combination_count(self) -> int:
        if not self.enabled or not self.axes:
            return 0
        total = 1
        for axis in self.axes:
            total *= len(axis.values)
        return total


@dataclass(frozen=True)
class AnalysisParams:
    data_source_type: str
    db_path: str
    table_name: str | None
    column_overrides: dict[str, str]
    excel_sheet_name: str | None
    start_date: str
    end_date: str
    stock_codes: tuple[str, ...]
    gap_direction: str
    gap_pct: float
    max_gap_filter_pct: float
    use_ma_filter: bool
    fast_ma_period: int
    slow_ma_period: int
    time_stop_days: int
    time_stop_target_pct: float
    stop_loss_pct: float
    take_profit_pct: float
    enable_take_profit: bool
    enable_profit_drawdown_exit: bool
    profit_drawdown_pct: float
    enable_ma_exit: bool
    exit_ma_period: int
    ma_exit_batches: int
    partial_exit_enabled: bool
    partial_exit_count: int
    partial_exit_rules: tuple[PartialExitRule, ...]
    buy_cost_pct: float
    sell_cost_pct: float
    time_exit_mode: str
    entry_factor: str = "gap"
    gap_entry_mode: str = "strict_break"
    trend_breakout_lookback: int = 20
    vcb_range_lookback: int = 7
    vcb_breakout_lookback: int = 20
    buy_slippage_pct: float = 0.0
    sell_slippage_pct: float = 0.0
    min_profit_to_activate_profit_drawdown_pct: float = 5.0
    enable_atr_trailing_exit: bool = False
    atr_trailing_period: int = 14
    atr_trailing_multiplier: float = 3.0
    min_profit_to_activate_atr_trailing_pct: float = 5.0
    enable_atr_filter: bool = False
    atr_filter_period: int = 14
    min_atr_filter_pct: float = 0.0
    max_atr_filter_pct: float = 100.0
    enable_board_ma_filter: bool = False
    board_ma_filter_line: str = "20"
    board_ma_filter_operator: str = ">="
    board_ma_filter_threshold: float = 0.0
    enable_imported_indicator_filter: bool = False
    imported_indicator_filter_key: str = ""
    imported_indicator_filter_column: str = ""
    imported_indicator_filter_operator: str = ">="
    imported_indicator_filter_threshold: float = 0.0
    imported_indicator_filters: tuple[ImportedIndicatorRule, ...] = ()
    enable_imported_indicator_exit: bool = False
    imported_indicator_exit_key: str = ""
    imported_indicator_exit_column: str = ""
    imported_indicator_exit_operator: str = "<="
    imported_indicator_exit_threshold: float = 0.0
    imported_indicator_exits: tuple[ImportedIndicatorRule, ...] = ()
    enable_board_ma_exit: bool = False
    board_ma_exit_line: str = "20"
    board_ma_exit_operator: str = "<="
    board_ma_exit_threshold: float = 0.0
    candle_run_length: int = 2
    candle_run_min_body_pct: float = 1.0
    candle_run_total_move_pct: float = 2.0
    eshb_open_window_bars: int = 6
    eshb_base_min_bars: int = 2
    eshb_base_max_bars: int = 8
    eshb_surge_min_pct: float = 3.0
    eshb_max_base_pullback_pct: float = 2.5
    eshb_max_base_range_pct: float = 2.0
    eshb_max_anchor_breaks: int = 1
    eshb_max_anchor_break_depth_pct: float = 0.8
    eshb_min_open_volume_ratio: float = 1.2
    eshb_min_breakout_volume_ratio: float = 1.0
    eshb_trigger_buffer_pct: float = 0.05
    timeframe: str = "1d"
    local_data_root: str = "data/market/daily"
    adjust: str = "qfq"
    scan_config: ParamScanConfig = field(default_factory=ParamScanConfig)

    @property
    def gap_ratio(self) -> float:
        return self.gap_pct / 100.0

    @property
    def time_stop_target_ratio(self) -> float:
        return self.time_stop_target_pct / 100.0

    @property
    def stop_loss_ratio(self) -> float:
        return self.stop_loss_pct / 100.0

    @property
    def take_profit_ratio(self) -> float:
        return self.take_profit_pct / 100.0

    @property
    def profit_drawdown_ratio(self) -> float:
        return self.profit_drawdown_pct / 100.0

    @property
    def min_profit_to_activate_profit_drawdown_ratio(self) -> float:
        return self.min_profit_to_activate_profit_drawdown_pct / 100.0

    @property
    def min_profit_to_activate_atr_trailing_ratio(self) -> float:
        return self.min_profit_to_activate_atr_trailing_pct / 100.0

    @property
    def buy_cost_ratio(self) -> float:
        return self.buy_cost_pct / 100.0

    @property
    def sell_cost_ratio(self) -> float:
        return self.sell_cost_pct / 100.0

    @property
    def effective_imported_indicator_filters(self) -> tuple[ImportedIndicatorRule, ...]:
        if not self.enable_imported_indicator_filter:
            return ()
        enabled_rules = tuple(
            rule for rule in self.imported_indicator_filters if rule.enabled
        )
        if enabled_rules:
            return enabled_rules
        return (
            ImportedIndicatorRule(
                enabled=True,
                indicator_key=self.imported_indicator_filter_key,
                column=self.imported_indicator_filter_column,
                operator=self.imported_indicator_filter_operator,
                threshold=self.imported_indicator_filter_threshold,
                priority=1,
            ),
        )

    @property
    def effective_imported_indicator_exits(self) -> tuple[ImportedIndicatorRule, ...]:
        if not self.enable_imported_indicator_exit:
            return ()
        enabled_rules = tuple(
            rule for rule in self.imported_indicator_exits if rule.enabled
        )
        if enabled_rules:
            return tuple(sorted(enabled_rules, key=lambda rule: rule.priority))
        return (
            ImportedIndicatorRule(
                enabled=True,
                indicator_key=self.imported_indicator_exit_key,
                column=self.imported_indicator_exit_column,
                operator=self.imported_indicator_exit_operator,
                threshold=self.imported_indicator_exit_threshold,
                priority=1,
            ),
        )

    @property
    def buy_slippage_ratio(self) -> float:
        return self.buy_slippage_pct / 100.0

    @property
    def sell_slippage_ratio(self) -> float:
        return self.sell_slippage_pct / 100.0

    @property
    def required_lookback_days(self) -> int:
        lookback = 5
        if self.use_ma_filter:
            lookback = max(lookback, max(self.fast_ma_period, self.slow_ma_period) + 5)
        if self.enable_ma_exit:
            lookback = max(lookback, self.exit_ma_period + 5)
        if self.enable_atr_trailing_exit:
            lookback = max(lookback, self.atr_trailing_period + 5)
        if self.enable_atr_filter:
            lookback = max(lookback, self.atr_filter_period + 5)
        if self.partial_exit_enabled:
            partial_ma_periods = [
                rule.ma_period
                for rule in self.partial_exit_rules
                if rule.enabled and rule.ma_period is not None
            ]
            if partial_ma_periods:
                lookback = max(lookback, max(partial_ma_periods) + 5)
            partial_atr_periods = [
                rule.atr_period
                for rule in self.partial_exit_rules
                if rule.enabled and rule.atr_period is not None
            ]
            if partial_atr_periods:
                lookback = max(lookback, max(partial_atr_periods) + 5)
        if self.entry_factor == "trend_breakout":
            lookback = max(lookback, self.trend_breakout_lookback + 5)
        if self.entry_factor == "volatility_contraction_breakout":
            lookback = max(
                lookback,
                max(self.vcb_range_lookback, self.vcb_breakout_lookback) + 5,
            )
        if self.entry_factor in {"candle_run", "candle_run_acceleration"}:
            lookback = max(lookback, self.candle_run_length + 5)
        if self.entry_factor == "early_surge_high_base":
            lookback = max(
                lookback,
                self.eshb_open_window_bars + self.eshb_base_max_bars + 10,
            )
        return lookback

    @property
    def required_lookahead_days(self) -> int:
        return self.time_stop_days + 5

    @property
    def partial_exit_indicator_keys(self) -> tuple[str, ...]:
        keys = {
            str(rule.indicator_key).strip()
            for rule in self.partial_exit_rules
            if rule.enabled
            and rule.mode == "indicator_threshold"
            and str(rule.indicator_key or "").strip()
        }
        return tuple(sorted(keys))

    @property
    def execution_indicator_keys(self) -> tuple[str, ...]:
        keys: set[str] = set(self.partial_exit_indicator_keys)
        if self.enable_board_ma_exit:
            keys.add("board_ma")
        keys.update(
            str(rule.indicator_key).strip()
            for rule in self.effective_imported_indicator_exits
            if str(rule.indicator_key).strip()
        )
        return tuple(sorted(keys))


def normalize_stock_codes(raw_text: str) -> tuple[str, ...]:
    if not raw_text.strip():
        return ()
    separators = [",", "，", "\n", "\t", " "]
    normalized = raw_text
    for separator in separators[1:]:
        normalized = normalized.replace(separator, separators[0])
    parts = [item.strip().upper() for item in normalized.split(separators[0])]
    return tuple(code for code in parts if code)


def normalize_column_overrides(raw_values: dict[str, str]) -> dict[str, str]:
    return {key: value.strip() for key, value in raw_values.items() if value.strip()}


def parse_scan_values(field_name: str, raw_text: str) -> tuple[int | float, ...]:
    caster = SCAN_FIELD_CASTERS[field_name]
    if not raw_text.strip():
        return ()
    normalized = raw_text
    for separator in ["，", "\n", "\t", " "]:
        normalized = normalized.replace(separator, ",")
    values: list[int | float] = []
    for chunk in normalized.split(","):
        text = chunk.strip()
        if not text:
            continue
        number = float(text)
        values.append(int(number) if caster is int else number)
    return tuple(values)


def apply_scan_overrides(
    params: AnalysisParams, overrides: dict[str, int | float]
) -> AnalysisParams:
    normalized: dict[str, int | float] = {}
    for field_name, value in overrides.items():
        caster = SCAN_FIELD_CASTERS[field_name]
        normalized[field_name] = int(value) if caster is int else float(value)
    plain_overrides: dict[str, int | float] = {}
    partial_rule_overrides: dict[int, dict[str, int | float]] = {}
    for field_name, value in normalized.items():
        partial_spec = PARTIAL_EXIT_SCAN_FIELD_SPECS.get(field_name)
        if partial_spec is None:
            plain_overrides[field_name] = value
            continue
        rule_slot, _, rule_attr = partial_spec
        slot_overrides = partial_rule_overrides.setdefault(rule_slot, {})
        slot_overrides[rule_attr] = value

    updated_params = replace(params, **plain_overrides)
    if not partial_rule_overrides:
        return updated_params

    updated_rules = list(updated_params.partial_exit_rules)
    for rule_slot, slot_overrides in partial_rule_overrides.items():
        rule_index = rule_slot - 1
        if rule_index >= len(updated_rules):
            continue
        updated_rules[rule_index] = replace(updated_rules[rule_index], **slot_overrides)
    return replace(updated_params, partial_exit_rules=tuple(updated_rules))


def _validate_partial_exit_scan_field(
    params: AnalysisParams, field_name: str, errors: list[str]
) -> None:
    partial_spec = PARTIAL_EXIT_SCAN_FIELD_SPECS.get(field_name)
    if partial_spec is None:
        return

    if not params.partial_exit_enabled:
        errors.append(f"扫描字段 {field_name} 依赖分批止盈，请先启用分批止盈。")
        return

    rule_slot, expected_mode, _ = partial_spec
    if rule_slot > params.partial_exit_count:
        errors.append(
            f"扫描字段 {field_name} 对应第 {rule_slot} 批，但当前分批数量仅为 {params.partial_exit_count}。"
        )
        return

    if rule_slot > len(params.partial_exit_rules):
        errors.append(f"扫描字段 {field_name} 缺少对应的分批规则配置。")
        return

    target_rule = params.partial_exit_rules[rule_slot - 1]
    if not target_rule.enabled:
        errors.append(f"扫描字段 {field_name} 对应批次未启用。")
    if target_rule.mode != expected_mode:
        errors.append(
            f"扫描字段 {field_name} 仅支持 mode={expected_mode}，当前为 {target_rule.mode}。"
        )


def validate_params(params: AnalysisParams) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if params.data_source_type not in {"sqlite", "file", "local_parquet"}:
        errors.append("数据来源类型不合法。")

    if params.data_source_type == "sqlite" and not params.db_path.strip():
        errors.append("请填写 SQLite 数据库文件路径。")

    if params.gap_direction not in {"up", "down"}:
        errors.append("跳空方向只能是向上或向下。")

    if params.time_exit_mode not in {"strict", "force_close"}:
        errors.append("时间到期处理方式不合法。")

    if params.entry_factor not in ENTRY_FACTORS:
        errors.append("入场因子不合法。")
    else:
        supported_timeframes = get_supported_strategy_timeframes(params.entry_factor)
        if params.timeframe not in supported_timeframes:
            supported_label = " / ".join(supported_timeframes)
            errors.append(f"{params.entry_factor} 仅支持 timeframe={supported_label}。")

    if params.entry_factor == "gap":
        if params.gap_entry_mode not in GAP_ENTRY_MODES:
            errors.append("开仓信号模式不合法。")
    elif params.gap_entry_mode != "strict_break":
        errors.append("entry_factor 非 gap 时，不支持设置 gap_entry_mode。")

    if params.adjust not in {"qfq", "hfq"}:
        errors.append("复权方式只能为 qfq 或 hfq。")

    if params.timeframe not in {"1d", "30m", "15m", "5m"}:
        errors.append("timeframe 只能为 1d、30m、15m 或 5m。")
    elif params.timeframe in {"30m", "15m", "5m"} and not params.stock_codes:
        warnings.append("分钟级数据在未指定股票池时 IO 开销较高，建议先限定股票池。")

    if params.gap_pct < 0:
        errors.append("跳空幅度不能为负数。")

    if params.max_gap_filter_pct < 0:
        errors.append("最大高开/低开过滤不能为负数。")

    if params.time_stop_days < 1:
        errors.append("最多持有天数必须大于等于 1。")

    if params.trend_breakout_lookback < 1:
        errors.append("trend_breakout_lookback 必须大于等于 1。")

    if params.vcb_range_lookback < 1:
        errors.append("vcb_range_lookback 必须大于等于 1。")

    if params.vcb_breakout_lookback < 1:
        errors.append("vcb_breakout_lookback 必须大于等于 1。")

    if params.candle_run_length < 2:
        errors.append("candle_run_length 必须大于等于 2。")

    if params.candle_run_min_body_pct < 0:
        errors.append("candle_run_min_body_pct 不能为负数。")

    if params.candle_run_total_move_pct < 0:
        errors.append("candle_run_total_move_pct 不能为负数。")

    if params.eshb_open_window_bars < 1:
        errors.append("eshb_open_window_bars 必须大于等于 1。")

    if params.eshb_base_min_bars < 1:
        errors.append("eshb_base_min_bars 必须大于等于 1。")

    if params.eshb_base_max_bars < params.eshb_base_min_bars:
        errors.append("eshb_base_max_bars 不能小于 eshb_base_min_bars。")

    if params.eshb_surge_min_pct < 0:
        errors.append("eshb_surge_min_pct 不能为负数。")

    if params.eshb_max_base_pullback_pct < 0:
        errors.append("eshb_max_base_pullback_pct 不能为负数。")

    if params.eshb_max_base_range_pct < 0:
        errors.append("eshb_max_base_range_pct 不能为负数。")

    if params.eshb_max_anchor_breaks < 0:
        errors.append("eshb_max_anchor_breaks 不能为负数。")

    if params.eshb_max_anchor_break_depth_pct < 0:
        errors.append("eshb_max_anchor_break_depth_pct 不能为负数。")

    if params.eshb_min_open_volume_ratio < 0:
        errors.append("eshb_min_open_volume_ratio 不能为负数。")

    if params.eshb_min_breakout_volume_ratio < 0:
        errors.append("eshb_min_breakout_volume_ratio 不能为负数。")

    if params.eshb_trigger_buffer_pct < 0:
        errors.append("eshb_trigger_buffer_pct 不能为负数。")

    if params.entry_factor == "early_surge_high_base":
        if params.timeframe != "30m":
            errors.append(
                "early_surge_high_base 仅支持 timeframe=30m（30m 形态 + 5m 执行）。"
            )
        if params.data_source_type != "local_parquet":
            errors.append("early_surge_high_base 仅支持 local_parquet 数据源。")
        if params.gap_direction != "up":
            errors.append(
                "early_surge_high_base 当前仅支持向上方向（gap_direction=up）。"
            )

    if params.stop_loss_pct < 0:
        errors.append("止损比例不能为负数。")

    if params.take_profit_pct < 0:
        errors.append("止盈比例不能为负数。")

    if params.profit_drawdown_pct < 0:
        errors.append("盈利回撤比例不能为负数。")

    if params.enable_profit_drawdown_exit and params.profit_drawdown_pct > 100:
        warnings.append("盈利回撤比例大于 100%，通常会导致很难触发，请确认设置。")

    if params.min_profit_to_activate_profit_drawdown_pct < 0:
        errors.append("盈利回撤止盈激活浮盈不能为负数。")

    if params.enable_ma_exit and params.exit_ma_period < 1:
        errors.append("止盈参考均线周期必须大于等于 1。")

    if params.enable_ma_exit and not (2 <= params.ma_exit_batches <= 3):
        errors.append("均线离场分批数必须在 2 到 3 之间。")

    if params.partial_exit_count not in {2, 3}:
        errors.append("分批数量只能为 2 或 3。")

    if params.partial_exit_enabled:
        enabled_rules = [rule for rule in params.partial_exit_rules if rule.enabled]
        if len(enabled_rules) != params.partial_exit_count:
            errors.append("启用分批止盈时，启用规则数必须与分批数量一致。")

        total_weight = sum(rule.weight_pct for rule in enabled_rules)
        if abs(total_weight - 100.0) > EPSILON:
            errors.append("启用分批止盈时，规则仓位比例之和必须为 100%。")

        priorities = [rule.priority for rule in enabled_rules]
        if len(priorities) != len(set(priorities)):
            errors.append("启用分批止盈时，每批 priority 必须唯一。")

        valid_modes = {
            "fixed_tp",
            "ma_exit",
            "profit_drawdown",
            "atr_trailing",
            "indicator_threshold",
        }
        for index, rule in enumerate(enabled_rules, start=1):
            if rule.mode not in valid_modes:
                errors.append(f"第 {index} 批退出方式不合法。")
                continue

            if rule.weight_pct < 0:
                errors.append(f"第 {index} 批仓位比例不能为负数。")

            if rule.mode == "fixed_tp":
                if rule.target_profit_pct is None:
                    errors.append(f"第 {index} 批 fixed_tp 必须填写目标收益。")
                elif rule.target_profit_pct < 0:
                    errors.append(f"第 {index} 批目标收益不能为负数。")

            if rule.mode == "ma_exit":
                if rule.ma_period is None:
                    errors.append(f"第 {index} 批 ma_exit 必须填写均线周期。")
                elif rule.ma_period < 1:
                    errors.append(f"第 {index} 批 ma_exit 均线周期必须大于等于 1。")

            if rule.mode == "profit_drawdown":
                if rule.drawdown_pct is None:
                    errors.append(f"第 {index} 批 profit_drawdown 必须填写回撤比例。")
                elif rule.drawdown_pct < 0:
                    errors.append(f"第 {index} 批回撤比例不能为负数。")

                if (
                    rule.min_profit_to_activate_drawdown is not None
                    and rule.min_profit_to_activate_drawdown < 0
                ):
                    errors.append(f"第 {index} 批最小浮盈激活门槛不能为负数。")

            if rule.mode == "atr_trailing":
                if rule.atr_period is None:
                    errors.append(f"第 {index} 批 atr_trailing 必须填写 ATR 周期。")
                elif rule.atr_period < 1:
                    errors.append(
                        f"第 {index} 批 atr_trailing ATR 周期必须大于等于 1。"
                    )

                if rule.atr_multiplier is None:
                    errors.append(f"第 {index} 批 atr_trailing 必须填写 ATR 倍数。")
                elif rule.atr_multiplier <= 0:
                    errors.append(f"第 {index} 批 atr_trailing ATR 倍数必须大于 0。")

            if rule.mode == "indicator_threshold":
                if not str(rule.indicator_key or "").strip():
                    errors.append(f"第 {index} 批导入指标阈值止盈必须选择指标。")
                if not str(rule.indicator_column or "").strip():
                    errors.append(f"第 {index} 批导入指标阈值止盈必须选择输出列。")
                if str(rule.indicator_operator or "").strip() not in {">=", "<="}:
                    errors.append(
                        f"第 {index} 批导入指标阈值止盈比较方向仅支持 >= 或 <=。"
                    )
                if rule.indicator_threshold is None:
                    errors.append(f"第 {index} 批导入指标阈值止盈必须填写阈值。")

    if params.time_stop_days < 1:
        errors.append("启用时间退出时，time_stop_days 必须大于等于 1。")

    if params.time_exit_mode not in {"strict", "force_close"}:
        errors.append("time_exit_mode 只能为 strict 或 force_close。")

    if params.buy_cost_pct < 0 or params.sell_cost_pct < 0:
        errors.append("买入成本和卖出成本都不能为负数。")

    if params.buy_slippage_pct < 0 or params.sell_slippage_pct < 0:
        errors.append("买入滑点和卖出滑点都不能为负数。")

    if params.enable_atr_trailing_exit and params.atr_trailing_period < 1:
        errors.append("ATR 跟踪周期必须大于等于 1。")

    if params.enable_atr_trailing_exit and params.atr_trailing_multiplier <= 0:
        errors.append("ATR 跟踪倍数必须大于 0。")

    if params.min_profit_to_activate_atr_trailing_pct < 0:
        errors.append("ATR 跟踪止盈激活浮盈不能为负数。")

    if params.enable_atr_filter and params.atr_filter_period < 1:
        errors.append("ATR 过滤周期必须大于等于 1。")

    if params.enable_atr_filter and params.min_atr_filter_pct < 0:
        errors.append("最小 ATR 波动过滤不能为负数。")

    if params.enable_atr_filter and params.max_atr_filter_pct < 0:
        errors.append("最大 ATR 波动过滤不能为负数。")

    if (
        params.enable_atr_filter
        and params.min_atr_filter_pct > params.max_atr_filter_pct
    ):
        errors.append("ATR 波动过滤下限不能大于上限。")

    if params.enable_board_ma_filter and params.board_ma_filter_line not in {
        "20",
        "50",
    }:
        errors.append("板块均线开仓过滤仅支持 20 或 50 日占比。")

    if params.enable_board_ma_filter and params.board_ma_filter_operator not in {
        ">=",
        "<=",
    }:
        errors.append("板块均线开仓过滤比较方向仅支持 >= 或 <=。")

    if params.enable_board_ma_filter and params.board_ma_filter_threshold < 0:
        errors.append("板块均线开仓过滤阈值不能为负数。")

    if params.enable_board_ma_filter and params.board_ma_filter_threshold > 100:
        errors.append("板块均线开仓过滤阈值不能大于 100。")

    for index, rule in enumerate(params.effective_imported_indicator_filters, start=1):
        if not rule.indicator_key.strip():
            errors.append(f"导入指标过滤规则 {index} 未选择指标。")
        if not rule.column.strip():
            errors.append(f"导入指标过滤规则 {index} 未选择输出列。")
        if rule.operator not in {">=", "<="}:
            errors.append(f"导入指标过滤规则 {index} 比较方向仅支持 >= 或 <=。")

    for index, rule in enumerate(params.effective_imported_indicator_exits, start=1):
        if not rule.indicator_key.strip():
            errors.append(f"导入指标离场规则 {index} 未选择指标。")
        if not rule.column.strip():
            errors.append(f"导入指标离场规则 {index} 未选择输出列。")
        if rule.operator not in {">=", "<="}:
            errors.append(f"导入指标离场规则 {index} 比较方向仅支持 >= 或 <=。")

    if params.enable_board_ma_exit and params.board_ma_exit_line not in {"20", "50"}:
        errors.append("板块均线离场仅支持 20 或 50 日占比。")

    if params.enable_board_ma_exit and params.board_ma_exit_operator not in {
        ">=",
        "<=",
    }:
        errors.append("板块均线离场比较方向仅支持 >= 或 <=。")

    if params.enable_board_ma_exit and params.board_ma_exit_threshold < 0:
        errors.append("板块均线离场阈值不能为负数。")

    if params.enable_board_ma_exit and params.board_ma_exit_threshold > 100:
        errors.append("板块均线离场阈值不能大于 100。")

    if params.use_ma_filter:
        if params.fast_ma_period < 1 or params.slow_ma_period < 1:
            errors.append("均线周期必须大于等于 1。")
        if params.fast_ma_period > params.slow_ma_period:
            warnings.append(
                "快线周期大于慢线周期是允许的，但请确认这符合您的研究习惯。"
            )

    for canonical, label in (
        ("date", "日期列名"),
        ("stock_code", "股票代码列名"),
        ("open", "开盘价列名"),
        ("high", "最高价列名"),
        ("low", "最低价列名"),
        ("close", "收盘价列名"),
        ("volume", "成交量列名"),
    ):
        if (
            canonical in params.column_overrides
            and not params.column_overrides[canonical].strip()
        ):
            errors.append(f"{label}不能为空。")

    if params.stop_loss_pct >= 100:
        warnings.append("止损比例大于等于 100%，这通常不符合常见交易设置。")

    if params.take_profit_pct >= 100:
        warnings.append("止盈比例大于等于 100%，请确认这是否为预期值。")

    if params.time_stop_target_pct < -100:
        warnings.append("到期最低目标涨幅过低，请确认是否填写正确。")

    if params.start_date > params.end_date:
        errors.append("开始日期不能晚于结束日期。")

    scan_config = params.scan_config
    if scan_config.enabled:
        if not (1 <= len(scan_config.axes) <= 2):
            errors.append("参数扫描当前只支持 1 到 2 个扫描维度。")
        field_names = [axis.field_name for axis in scan_config.axes]
        if len(field_names) != len(set(field_names)):
            errors.append("参数扫描字段不能重复。")
        for axis in scan_config.axes:
            if axis.field_name not in SCAN_FIELD_CASTERS:
                errors.append(f"参数扫描字段不支持：{axis.field_name}")
                continue
            if not axis.values:
                errors.append(f"参数扫描字段 {axis.field_name} 必须提供至少一个值。")
            eligible_fields = FACTOR_SCAN_ELIGIBLE_FIELDS.get(params.entry_factor)
            if eligible_fields is not None and axis.field_name not in eligible_fields:
                errors.append(
                    f"entry_factor={params.entry_factor} 不支持扫描字段：{axis.field_name}"
                )
            _validate_partial_exit_scan_field(params, axis.field_name, errors)
        if scan_config.metric not in SCAN_METRICS:
            errors.append("参数扫描排序指标不合法。")
        if scan_config.max_combinations < 1:
            errors.append("参数扫描最大组合数必须大于等于 1。")
        if scan_config.combination_count > scan_config.max_combinations:
            errors.append("参数扫描组合数超出上限，请减少扫描值数量。")

    return errors, warnings
