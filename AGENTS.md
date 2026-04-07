# AGENTS.md

## What this repo is
- `Trend-backtest` is a **Streamlit + Pandas A-share research workbench**, not a tick-level simulator. Keep semantics stable and reproducible before adding features.
- Real backtest flow is: `app.py` (UI/workbench) → `models.py` (params/validation) → `analyzer.py` (candidate/scan/equity aggregation) → `rules.py` (signal + execution + exits).

## Highest-value files to read first
- `README.md` — product scope and user-facing promises.
- `app.py` — actual UI wiring and many test-locked contracts.
- `models.py`, `analyzer.py`, `rules.py` — core execution semantics.
- `data_loader.py` — local parquet / SQLite / file ingestion, shared config reading, TDX path normalization, and offline subprocess helpers.
- `scripts/update_data.py` — offline market data update CLI.
- `scripts/import_tdx_local_indicators.py` + `data/providers/tdx_local_indicator_provider.py` — TDX formula-driven local indicator import.

## Commands that matter
- Install deps: `pip install -r requirements.txt`
- Run app: `streamlit run app.py`
- Full tests: `pytest -q`
- Focused display-format regression: `python -m unittest tests.test_app_display_formatters_unittest -v`
- Focused regression used often after data/TDX/app changes:
  - `pytest tests/test_app_strategy_options.py tests/test_tdx_quant_provider.py tests/test_import_tdx_local_indicators.py tests/test_update_data_export.py tests/test_repo_handoff_contract.py tests/test_app_result_tabs_contract.py -q`

## Repo-specific constraints
- `README.md` is part of the tested contract. Do not casually rewrite scope/version notes; `tests/test_repo_handoff_contract.py` asserts specific README content and dependency floors.
- Some `app.py` layout/text is source-tested, not just behavior-tested. In particular, result-tab partitioning and sidebar-only sections are asserted by `tests/test_app_result_tabs_contract.py`.
- Streamlit option sets are test-locked in places. Example: current UI exposes backtest/update timeframe options `1d/30m/15m/5m`; changing labels/options breaks `tests/test_app_strategy_options.py`.

## Data/update workflow gotchas
- Config source of truth is `config/data_source.yaml`; shared parsing lives in `data_loader.read_data_source_config()`.
- `scripts/update_data.py` only supports `data_source: local_parquet`.
- `scripts/update_data.py` accepts repeated `--timeframe` flags and dedupes while preserving order.
- Provider routing is asymmetric by design:
  - `1m` / `5m` → `TdxQuantProvider`
  - `15m` / `30m` / `1d` → existing AKShare/offline path
- `resolve_local_data_root()` derives intraday directories from the daily root. Do not hardcode `daily/...` for non-`1d` timeframes.
- Update runs append metadata/inventory rows even on failure; don’t remove that bookkeeping lightly.

## TDX / formula integration
- `TDX_TQCENTER_PATH` is the key env var. It may point to the install root, `PYPlugins`, `PYPlugins/user`, or `tqcenter.py`; `data_loader.normalize_tdx_tqcenter_path()` normalizes it.
- TDX terminal integration assumes a locally installed and logged-in terminal with TQ support.
- Formula import is intentionally **manual-friendly**: `scripts/import_tdx_local_indicators.py` supports `--formula-name` and `--output-map`; auto-enumeration is not relied on.
- Built-in indicator discovery is limited on purpose; the app/test contract expects the manual fallback message to remain available.

## Practical change guidance
- If changing strategy semantics, update tests first/alongside: entry-factor tests, trade-rule tests, and scan tests are the real contract.
- If changing only offline update / TDX subprocess behavior, prefer editing `data_loader.py` + `scripts/update_data.py` rather than adding more app-layer logic.
- Keep `app.py` UI-specific. Low-risk extractions are helpers/utilities; avoid moving source-tested tab/sidebar blocks unless you also update their tests.
