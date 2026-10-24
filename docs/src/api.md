# API

This page summarizes the current public Python surface.

## Workflow Task Models

`finance-flow` exports provider-neutral `ccflow` callable task models for research and portfolio workflows.

### First-Wave Tasks

- `BuildUniverseModel` + `BuildUniverseContext` -> `list[UniverseMember]`
- `CalculateSignalsModel` + `CalculateSignalsContext` -> `list[SignalRecord]`
- `OptimizePortfolioModel` + `OptimizePortfolioContext` -> `list[OptimizerAllocation]`
- `ConstructTargetPositionsModel` + `ConstructTargetPositionsContext` -> `list[TargetPositionRecord]`

### Second-Wave Tasks

- `BacktestPortfolioModel` + `BacktestPortfolioContext` -> `list[BacktestResultRecord]`
- `EvaluateRealPortfolioModel` + `EvaluateRealPortfolioContext` -> evaluation summary payload
- `BuildAlphaReportModel` + `BuildAlphaReportContext` -> alpha report payload
- `BuildRiskReportModel` + `BuildRiskReportContext` -> risk report payload

All task contexts support date-only list context coercion for CLI workflows (`+context=[YYYY-MM-DD]`).

## Task Registry (Hydra Group)

`finance-flow` publishes the Hydra group `task` with these entries:

- `build_universe`
- `calculate_signals`
- `optimize_portfolio`
- `construct_target_positions`
- `backtest_portfolio`
- `evaluate_real_portfolio`
- `build_alpha_report`
- `build_risk_report`

Each task config binds `callable` to `/task_model` so shared `cc-etl`/`cc-etl-explain` commands can resolve tasks uniformly.

Representative task output payload shapes are documented in `docs/src/task-payloads.md`.

## Explain Commands

Use `cc-etl-explain` to inspect task contracts and context coercion through the shared registry.

```bash
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=build_universe +context=[2025-01-02]
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=calculate_signals +context=[2025-01-02]
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=optimize_portfolio +context=[2025-01-02]
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=construct_target_positions +context=[2025-01-02]
```

Second-wave explain commands:

```bash
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=backtest_portfolio +context=[2025-01-02]
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=evaluate_real_portfolio +context=[2025-01-02]
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=build_alpha_report +context=[2025-01-02]
cc-etl-explain --no-gui --config-path "$(pgc-etl-config-path)" +task=build_risk_report +context=[2025-01-02]
```

## Daily Bars

```python
from finance_flow import DailyBar
```

`DailyBar` is the current OHLCV record schema used by normalization workflows.

## Security Master

`load_security_master(root, as_of=None, active_only=False)` returns a Polars `LazyFrame` with one row per security known
on `as_of`: its ticker at that time, name, type, primary exchange, composite and share-class FIGIs, the ticker's
`valid_from` and `valid_to`, and whether it was still listed. `root` is the directory holding the curated
`curated/securities/listings/v1/...` and `curated/securities/symbol-history/v1/...` partitions.

```python
from datetime import date

import polars as pl

from finance_flow import SecurityMaster, load_security_master

# Every security listed on 2024-06-03, with the ticker it traded under that day
master = load_security_master("/data", as_of=date(2024, 6, 3), active_only=True)
master.filter(pl.col("type") == "CS").collect()

# Point-in-time lookups; a weekend or holiday resolves to the session before it
securities = SecurityMaster.from_root("/data")
securities.lookup(["META"], as_of=date(2021, 10, 27)).collect()  # empty: Meta traded as FB until 2022-06-09
securities.lookup(["FB"], as_of=date(2021, 10, 27)).select("ticker", "security_id").collect()
securities.history(["BBG000MM2P62"], as_of=date(2024, 6, 3)).collect()  # FB, then META

# Key ticker-based data, such as bars, by security for each row's own date
bars = pl.scan_parquet("/data/bars/*.parquet")
securities.attach_security_id(bars, start=date(2024, 1, 2), end=date(2024, 6, 28))
```

Everything is computed from data known on `as_of`, so delisted securities keep their last ticker and later ticker
reuse does not leak backwards. Histories are read from the stored symbol history for the nearest date on or before
`as_of`, or computed from the listings when none is stored.

## Normalization

```python
from finance_flow import MassiveDailyBarsNormalizeContext, MassiveDailyBarsNormalizeModel, normalize_massive_daily_bars
```

`normalize_massive_daily_bars` converts Massive-shaped daily aggregate payloads into `DailyBar` objects. `MassiveDailyBarsNormalizeModel` exposes the same transform as a `ccflow` callable model.

Future API additions should favor provider-neutral names and typed workflow artifacts so private packages can extend the public workflows without replacing them.
