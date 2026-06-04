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

## Normalization

```python
from finance_flow import MassiveDailyBarsNormalizeContext, MassiveDailyBarsNormalizeModel, normalize_massive_daily_bars
```

`normalize_massive_daily_bars` converts Massive-shaped daily aggregate payloads into `DailyBar` objects. `MassiveDailyBarsNormalizeModel` exposes the same transform as a `ccflow` callable model.

Future API additions should favor provider-neutral names and typed workflow artifacts so private packages can extend the public workflows without replacing them.
