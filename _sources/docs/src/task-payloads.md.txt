# Task Payload Contracts

This page captures the expected output payload shapes for the finance-flow task registry.

## First-Wave Outputs

### build_universe

Output: `list[UniverseMember]`

Representative element:

```json
{
  "as_of_date": "2025-01-02",
  "symbol": "AAPL",
  "exchange": "XNYS",
  "instrument_id": "XNYS:AAPL",
  "close": 184.95,
  "currency": "USD"
}
```

### calculate_signals

Output: `list[SignalRecord]`

Representative element:

```json
{
  "as_of_date": "2025-01-02",
  "instrument_id": "XNYS:AAPL",
  "signal_name": "alpha",
  "horizon_days": 1,
  "value": 0.37
}
```

### optimize_portfolio

Output: `list[OptimizerAllocation]`

Representative element:

```json
{
  "as_of_date": "2025-01-02",
  "instrument_id": "XNYS:AAPL",
  "weight": 0.12,
  "score": 0.37
}
```

### construct_target_positions

Output: `list[TargetPositionRecord]`

Representative element:

```json
{
  "as_of_date": "2025-01-02",
  "instrument_id": "XNYS:AAPL",
  "target_weight": 0.12,
  "target_notional": 120000.0,
  "target_quantity": 600.0,
  "currency": "USD"
}
```

## Second-Wave Outputs

### backtest_portfolio

Output: `list[BacktestResultRecord]`

Representative element:

```json
{
  "strategy_id": "default-strategy",
  "as_of_date": "2025-01-02",
  "return_pct": 0.001,
  "turnover": 0.5,
  "drawdown": -0.00025
}
```

### evaluate_real_portfolio

Output: summary dictionary

Representative payload:

```json
{
  "as_of_date": "2025-01-02",
  "tolerance_bps": 25.0,
  "mismatch_count": 1
}
```

### build_alpha_report

Output: dictionary with `report` and `top_signals`

Representative payload:

```json
{
  "report": {
    "report_id": "alpha-2025-01-02",
    "report_type": "alpha",
    "as_of_date": "2025-01-02",
    "generated_at": "2026-05-31T12:00:00Z",
    "schema_metadata": {
      "schema_name": "alpha-report",
      "schema_version": 1,
      "generated_at": "2026-05-31T12:00:00Z"
    }
  },
  "top_signals": [
    {
      "as_of_date": "2025-01-02",
      "instrument_id": "XNYS:AAPL",
      "signal_name": "alpha",
      "horizon_days": 1,
      "value": 0.37
    }
  ]
}
```

### build_risk_report

Output: dictionary with `report` and `risk_summary`

Representative payload:

```json
{
  "report": {
    "report_id": "risk-2025-01-02",
    "report_type": "risk",
    "as_of_date": "2025-01-02",
    "generated_at": "2026-05-31T12:00:00Z",
    "schema_metadata": {
      "schema_name": "risk-report",
      "schema_version": 1,
      "generated_at": "2026-05-31T12:00:00Z"
    }
  },
  "risk_summary": {
    "gross_exposure": 1.0,
    "net_exposure": 0.2
  }
}
```
