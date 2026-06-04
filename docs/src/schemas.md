# Schemas

`finance-flow` owns reusable finance workflow schemas. These schemas are intentionally small at first and are meant to grow into provider-neutral contracts consumed by research and portfolio workflows.

## Daily Bars

`DailyBar` represents one OHLCV bar for one symbol and one session date:

| Field          | Meaning                                 |
| -------------- | --------------------------------------- |
| `ticker`       | Symbol or provider ticker.              |
| `date`         | Session date.                           |
| `open`         | Opening price.                          |
| `high`         | High price.                             |
| `low`          | Low price.                              |
| `close`        | Closing price.                          |
| `volume`       | Total traded volume.                    |
| `vwap`         | Optional volume-weighted average price. |
| `transactions` | Optional provider transaction count.    |

```python
from finance_flow import DailyBar

bar = DailyBar(
    ticker="AAPL",
    date="2024-01-03",
    open=184.22,
    high=185.88,
    low=183.43,
    close=184.95,
    volume=58414500,
)
```

Future schemas should stay provider-neutral. Provider-specific cleanup belongs at package edges, while downstream workflow models should exchange typed finance structures.
