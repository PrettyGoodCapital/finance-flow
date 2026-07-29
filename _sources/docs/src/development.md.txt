# Development

`finance-flow` is the reusable research and portfolio workflow layer. It should consume typed finance structures, produce typed workflow artifacts, and stay independent of provider credentials and deployment storage.

## Package Boundaries

`finance-flow` may depend on `ccflow` and finance transformation libraries. It may be consumed by `finance-etl` and downstream applications.

It should not depend on connector packages unless a transformation genuinely needs optional I/O support. It must not depend on application-specific packages.

## What Belongs Here

Good fits for this package include:

- provider-neutral finance schemas
- normalization and validation transforms
- universe construction workflows
- signal calculation workflows
- portfolio optimization and target-position workflows
- backtest, performance, alpha-report, and risk-report workflows

## What Belongs Elsewhere

Provider clients and credentials belong in `finance-etl` or connector packages. Generic ETL execution belongs in `ccflow-etl`. Private orchestration and environment-specific config belong in downstream application packages.

## Test Convention

Default tests should use small synthetic finance datasets. They should not require live providers, private credentials, live storage, or private package imports.
