# Transforms

`finance-flow` transforms provider-shaped data into reusable finance structures. It does not fetch provider data, manage credentials, or choose storage destinations.

## Massive Daily Bars

`normalize_massive_daily_bars` accepts Massive REST daily aggregate rows and synthetic fixture rows, then returns `DailyBar` objects.

```python
from finance_flow import normalize_massive_daily_bars

bars = normalize_massive_daily_bars(
    {
        "results": [
            {
                "T": "AAPL",
                "o": 184.22,
                "h": 185.88,
                "l": 183.43,
                "c": 184.95,
                "v": 58414500,
                "vw": 184.71,
                "n": 521321,
            }
        ]
    },
    ticker="AAPL",
    session_date="2024-01-03",
)
```

The callable wrapper exposes the same normalization through `ccflow`:

```python
from finance_flow import MassiveDailyBarsNormalizeContext, MassiveDailyBarsNormalizeModel

result = MassiveDailyBarsNormalizeModel()(
    MassiveDailyBarsNormalizeContext(
        payload=[{"ticker": "AAPL", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100}],
        ticker="AAPL",
        session_date="2024-01-03",
    )
)
```

Transforms should validate required fields and fail loudly on corrupt payloads instead of silently fabricating market data.

`MassiveDailyBarsArtifactModel` wraps that transform for artifact workflows. It can optionally expose a raw extract model through `__deps__`, then reads the raw daily aggregate artifact and writes parquet rows keyed by date and ticker.

```python
from finance_flow import MassiveDailyBarsArtifactContext, MassiveDailyBarsArtifactModel

result = MassiveDailyBarsArtifactModel(input_store=store, output=store)(
    MassiveDailyBarsArtifactContext(ticker="AAPL", date="2024-01-03")
)
```

## First-Wave Chain Composition

The canonical first-wave chain composes four callable tasks with typed handoffs:

1. `BuildUniverseModel`: emits `UniverseMember` rows
2. `CalculateSignalsModel`: consumes universe rows, emits `SignalRecord` rows
3. `OptimizePortfolioModel`: consumes signals, emits `OptimizerAllocation` rows
4. `ConstructTargetPositionsModel`: consumes allocations, emits `TargetPositionRecord` rows

This chain is validated by the integration test in `finance_flow/tests/test_task_chain_integration.py`.
