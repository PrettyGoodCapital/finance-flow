# finance flow

Standard financial flow models

[![Build Status](https://github.com/PrettyGoodCapital/finance-flow/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/PrettyGoodCapital/finance-flow/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/PrettyGoodCapital/finance-flow/branch/main/graph/badge.svg)](https://codecov.io/gh/PrettyGoodCapital/finance-flow)
[![License](https://img.shields.io/github/license/PrettyGoodCapital/finance-flow)](https://github.com/PrettyGoodCapital/finance-flow)
[![PyPI](https://img.shields.io/pypi/v/finance-flow.svg)](https://pypi.python.org/pypi/finance-flow)

## Overview

`finance-flow` provides reusable finance-specific callable models, schemas, and transformations. It owns provider-neutral research and portfolio workflow logic: normalize market data, validate finance structures, build universes, calculate signals, produce target positions, backtest, and generate reports.

It does not own extraction credentials, provider clients, storage destinations, or application orchestration. Those concerns belong in `finance-etl`, connector packages, or downstream applications.

## Quick Start

Normalize provider-shaped daily aggregate rows into typed daily bars:

```python
from finance_flow import normalize_massive_daily_bars

bars = normalize_massive_daily_bars(
    {
        "results": [
            {"T": "AAPL", "o": 184.22, "h": 185.88, "l": 183.43, "c": 184.95, "v": 58414500}
        ]
    },
    ticker="AAPL",
    session_date="2024-01-03",
)
```

Use the callable wrapper when composing the transform inside a `ccflow` graph:

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

Publish normalized daily bars as parquet artifacts:

```python
from finance_flow import MassiveDailyBarsArtifactContext, MassiveDailyBarsArtifactModel

result = MassiveDailyBarsArtifactModel(input_store=store, output=store)(
    MassiveDailyBarsArtifactContext(ticker="AAPL", date="2024-01-03")
)
```

The artifact task reads `massive/stocks/rest/daily-aggs/json/{date}/{ticker}.json` and writes `massive/stocks/bars/daily/parquet/{date}/{ticker}.parquet`.

Build market-wide daily bars from Massive day-aggregate flat files with `MassiveDailyBarsFlatFileModel`. It materializes `massive/stocks/s3/day-aggs/{year}/{month}/{date}.csv.gz` to a local workspace, streams it through `MassiveDailyBarsFlatFileTransformModel` into one validated Parquet file per session, and writes that file through a local writer and an optional backup writer under `massive/stocks/curated/bars/daily/v1/{year}/{month}/{date}.parquet`.

Each session also gets a `{date}.metadata.json` sidecar, written through the same writers. It records:

- the source object's key, URI, size, and, when the store reports them, ETag, version ID, and last-modified time;
- the transform class, a SHA-256 fingerprint of its configuration, and the `finance-flow` and `pyarrow` versions;
- the output key, size, SHA-256, and row count;
- quality counters recomputed from the written Parquet: row and ticker counts, duplicate tickers, null required values, negative volume or transactions, OHLC violations, and zero-volume and zero-transaction rows.

When a partition already exists but its sidecar does not, the model writes only the sidecar, so older sessions can be backfilled without re-transforming.

## Documentation

- [Schemas](docs/src/schemas.md)
- [Transforms](docs/src/transforms.md)
- [API](docs/src/api.md)
- [Task Payload Contracts](docs/src/task-payloads.md)
- [Development](docs/src/development.md)

## Dependency Contract

- Depends on `ccflow` for callable model integration and may depend on dataframe and validation libraries needed for finance transformations.
- May be consumed by `finance-etl` and application-specific packages.
- Must not depend on connector packages unless a transformation genuinely needs optional I/O support, and must not depend on application-specific packages.

## Test Convention

Default tests should use small synthetic finance datasets and run without external services or provider credentials.
