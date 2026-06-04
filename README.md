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
