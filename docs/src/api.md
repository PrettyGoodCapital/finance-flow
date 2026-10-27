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
`valid_from` and `valid_to`, and whether it was still listed. `root` is the directory holding the curated listings, symbol history, daily bars, and adjustment
factor partitions. The master also carries each security's SEC `cik`.

```python
from datetime import date

import polars as pl

from finance_flow import SecurityMaster, load_security_master

# Every security listed on 2024-06-03, with the ticker it traded under that day
master = load_security_master("/data", as_of=date(2024, 6, 3), active_only=True)
master.filter(pl.col("type") == "CS").collect()

# Point-in-time lookups; a weekend or holiday resolves to the session before it
securities = SecurityMaster.from_root("/data")
# Tickers are reused, so the same symbol can be a different security on another date
securities.lookup(["FB"], as_of=date(2021, 10, 27)).collect()    # Facebook, BBG000MM2P62
securities.lookup(["META"], as_of=date(2021, 10, 27)).collect()  # Roundhill Ball Metaverse ETF
securities.lookup(["META"], as_of=date(2024, 6, 3)).collect()    # Meta Platforms, BBG000MM2P62
securities.lookup(["FB"], as_of=date(2026, 10, 23)).collect()    # ProShares S&P 500 Dynamic Buffer ETF
securities.history(["BBG000MM2P62"], as_of=date(2024, 6, 3)).collect()  # FB until 2022-06-09, then META

# Find a security by any identifier as of a date, with all its identifiers
securities.resolve(["META.O"], by="ric", as_of=date(2024, 6, 3)).collect()
securities.resolve(["FB US Equity"], by="bloomberg_ticker", as_of=date(2021, 10, 27)).collect()
securities.resolve(["30303M102"], by="cusip").collect()
securities.resolve(["0001326801"], by="cik").select("ticker", "ric", "isin", "company_name").collect()

# Daily prices by stable ID: one series across FB -> META, adjusted for splits and dividends known today
securities.prices(["BBG000MM2P62"], start=date(2022, 5, 16), end=date(2022, 6, 15)).collect()
# As traded, or adjusted only with events known by a date
securities.prices(["BBG000MM2P62"], start=date(2022, 5, 16), end=date(2022, 6, 15), adjust="none")
securities.prices(["BBG009S39JX6"], start=date(2022, 5, 16), end=date(2022, 6, 15), adjust="split", as_of=date(2022, 6, 15))

# Key ticker-based data, such as bars, by security for each row's own date
bars = pl.scan_parquet("/data/bars/*.parquet")
securities.attach_security_id(bars, start=date(2024, 1, 2), end=date(2024, 6, 28))
```

Identifiers come from `SecurityIdentifiersModel`, one row per listing per session:

| Column                | Source                                                                                                                                                     |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ric`                 | Licensed RICs where an override covers the listing, otherwise derived from the ticker and primary exchange (`BRKb.N`, `ABR_pd.N`); `ric_source` says which |
| `bloomberg_ticker`    | OpenFIGI's ticker for the FIGI while the listing still trades under it (`BRK/B US Equity`), otherwise derived from the listing ticker                      |
| `cusip`, `isin`       | The session's symbol master (such as the WRDS NYSE TAQ master), carried forward between masters; ISIN computed for US CUSIPs                               |
| `cik`, `company_name` | The listing's SEC CIK and the SEC name in effect on the session                                                                                            |
| `mic`, `share_class`  | Primary exchange MIC, and the class letter from the name or ticker                                                                                         |

Everything is computed from data known on `as_of`, so delisted securities keep their last ticker and later ticker
reuse does not leak backwards. Histories are read from the stored symbol history for the nearest date on or before
`as_of`, or computed from the listings when none is stored.

## Macro Data

`load_fred(root, series=None, as_of=None)` returns FRED observations as they were published on `as_of`, from the
curated vintage table under `root` (`curated/fred/observations/v1/latest.parquet`). Use the session date to get what
was known by its close; omit `as_of` for current values. `fred_as_of(frame, as_of, series)` applies the same filter to
an already loaded table.

```python
from datetime import date

from finance_flow import load_fred

# Payrolls and unemployment as they stood on 2024-06-03, before later revisions
load_fred("/data", ["PAYEMS", "UNRATE"], as_of=date(2024, 6, 3)).collect()
```

## Normalization

```python
from finance_flow import MassiveDailyBarsNormalizeContext, MassiveDailyBarsNormalizeModel, normalize_massive_daily_bars
```

`normalize_massive_daily_bars` converts Massive-shaped daily aggregate payloads into `DailyBar` objects. `MassiveDailyBarsNormalizeModel` exposes the same transform as a `ccflow` callable model.

Future API additions should favor provider-neutral names and typed workflow artifacts so private packages can extend the public workflows without replacing them.
