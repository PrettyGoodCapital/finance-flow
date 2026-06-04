from datetime import date
from typing import Dict, List, Optional, Type

from ccflow import CallableModel, ContextBase, ContextType, Flow, GenericResult, ResultType
from finance_datagen import generate_prices, generate_signal
from finance_etl import (
    BacktestResultRecord,
    MarketPartitionContext,
    OptimizerAllocation,
    PortfolioDatePartitionContext,
    PortfolioSnapshot,
    ReportDatePartitionContext,
    ReportMetadata,
    SchemaMetadata,
    SignalRecord,
    StrategyDatePartitionContext,
    TargetPositionRecord,
    UniverseMember,
)
from pydantic import Field, model_validator

__all__ = (
    "BuildUniverseContext",
    "BuildUniverseModel",
    "CalculateSignalsContext",
    "CalculateSignalsModel",
    "OptimizePortfolioContext",
    "OptimizePortfolioModel",
    "ConstructTargetPositionsContext",
    "ConstructTargetPositionsModel",
    "BacktestPortfolioContext",
    "BacktestPortfolioModel",
    "EvaluateRealPortfolioContext",
    "EvaluateRealPortfolioModel",
    "BuildAlphaReportContext",
    "BuildAlphaReportModel",
    "BuildRiskReportContext",
    "BuildRiskReportModel",
)


def _normalize_weights(values: List[float]) -> List[float]:
    gross = sum(abs(value) for value in values)
    if gross == 0:
        raise ValueError("Cannot normalize zero signal vector.")
    return [value / gross for value in values]


def _coerce_date_only_context(cls, value, handler):
    if not isinstance(value, (cls, dict)):
        if isinstance(value, (tuple, list)) and len(value) == 1:
            value = value[0]
        value = {"as_of_date": value}
    return handler(value)


def _synthetic_signals(as_of_date: date, n_assets: int, symbols: List[str], ic: float, seed: Optional[int]) -> List[SignalRecord]:
    symbols = symbols or [f"A{i:04d}" for i in range(n_assets)]
    frame = generate_signal(n_dates=1, n_assets=len(symbols), symbols=symbols, start=as_of_date, ic=ic, seed=seed)
    return [
        SignalRecord(
            as_of_date=as_of_date,
            instrument_id=f"GEN:{str(row['symbol'])}",
            value=float(row["signal"]),
        )
        for row in frame.iter_rows(named=True)
    ]


class BuildUniverseContext(ContextBase):
    as_of_date: date
    symbols: List[str] = Field(default_factory=list)
    exchange: str = "XNYS"
    market_partition: Optional[MarketPartitionContext] = None
    n_assets: int = 10
    seed: Optional[int] = None

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)

    @model_validator(mode="after")
    def apply_market_partition(self):
        if self.market_partition:
            if self.market_partition.as_of_date != self.as_of_date:
                raise ValueError("market_partition.as_of_date must match as_of_date.")
            if self.market_partition.exchange:
                self.exchange = self.market_partition.exchange
        return self


class BuildUniverseModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return BuildUniverseContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: BuildUniverseContext) -> GenericResult:
        symbols = context.symbols or [f"A{i:04d}" for i in range(context.n_assets)]
        members: List[UniverseMember] = []
        for index, symbol in enumerate(symbols):
            frame = generate_prices(symbol=symbol, n_steps=1, seed=(None if context.seed is None else context.seed + index))
            close = float(frame["price"][-1])
            members.append(
                UniverseMember(
                    as_of_date=context.as_of_date,
                    symbol=symbol,
                    exchange=context.exchange,
                    close=close,
                )
            )
        if self.explain:
            return GenericResult(
                value={
                    "task": "build_universe",
                    "as_of_date": context.as_of_date.isoformat(),
                    "exchange": context.exchange,
                    "symbols": symbols,
                    "seed": context.seed,
                    "uses_datagen": True,
                    "planned_outputs": ["UniverseMember"],
                }
            )
        return GenericResult(value=members)


class CalculateSignalsContext(ContextBase):
    as_of_date: date
    universe: List[UniverseMember] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    strategy_partition: Optional[StrategyDatePartitionContext] = None
    n_assets: int = 10
    ic: float = 0.05
    seed: Optional[int] = None

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)

    @model_validator(mode="after")
    def validate_input_shape(self):
        if not self.universe and not self.symbols and self.n_assets <= 0:
            raise ValueError("calculate_signals requires universe, symbols, or n_assets > 0.")
        if self.strategy_partition and self.strategy_partition.as_of_date != self.as_of_date:
            raise ValueError("strategy_partition.as_of_date must match as_of_date.")
        return self


class CalculateSignalsModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return CalculateSignalsContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: CalculateSignalsContext) -> GenericResult:
        members = context.universe
        if members:
            symbols = [member.symbol for member in members]
            instrument_ids = {member.symbol: member.instrument_id for member in members}
        else:
            symbols = context.symbols or [f"A{i:04d}" for i in range(context.n_assets)]
            instrument_ids = {symbol: f"GEN:{symbol}" for symbol in symbols}

        frame = generate_signal(
            n_dates=1,
            n_assets=len(symbols),
            symbols=symbols,
            start=context.as_of_date,
            ic=context.ic,
            seed=context.seed,
        )

        signals: List[SignalRecord] = []
        for row in frame.iter_rows(named=True):
            symbol = str(row["symbol"])
            signals.append(
                SignalRecord(
                    as_of_date=context.as_of_date,
                    instrument_id=instrument_ids[symbol],
                    value=float(row["signal"]),
                )
            )

        if self.explain:
            return GenericResult(
                value={
                    "task": "calculate_signals",
                    "as_of_date": context.as_of_date.isoformat(),
                    "symbols": symbols,
                    "ic": context.ic,
                    "seed": context.seed,
                    "uses_datagen": True,
                    "planned_outputs": ["SignalRecord"],
                }
            )

        return GenericResult(value=signals)


class OptimizePortfolioContext(ContextBase):
    as_of_date: date
    signals: List[SignalRecord] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    strategy_partition: Optional[StrategyDatePartitionContext] = None
    n_assets: int = 10
    ic: float = 0.05
    seed: Optional[int] = None
    long_only: bool = False
    max_abs_weight: float = 1.0
    top_k: Optional[int] = None

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)

    @model_validator(mode="after")
    def validate_max_abs_weight(self):
        if self.max_abs_weight <= 0:
            raise ValueError("max_abs_weight must be positive.")
        if self.strategy_partition and self.strategy_partition.as_of_date != self.as_of_date:
            raise ValueError("strategy_partition.as_of_date must match as_of_date.")
        return self


class OptimizePortfolioModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return OptimizePortfolioContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: OptimizePortfolioContext) -> GenericResult:
        signals = context.signals or _synthetic_signals(
            as_of_date=context.as_of_date,
            n_assets=context.n_assets,
            symbols=context.symbols,
            ic=context.ic,
            seed=context.seed,
        )

        ordered = sorted(signals, key=lambda signal: abs(signal.value), reverse=True)
        if context.top_k is not None:
            ordered = ordered[: context.top_k]

        raw = []
        for signal in ordered:
            score = max(signal.value, 0.0) if context.long_only else signal.value
            raw.append(score)

        normalized = _normalize_weights(raw)
        capped = []
        for value in normalized:
            if value > context.max_abs_weight:
                capped.append(context.max_abs_weight)
            elif value < -context.max_abs_weight:
                capped.append(-context.max_abs_weight)
            else:
                capped.append(value)
        final_weights = _normalize_weights(capped)

        allocations = [
            OptimizerAllocation(
                as_of_date=context.as_of_date,
                instrument_id=signal.instrument_id,
                score=signal.value,
                weight=weight,
            )
            for signal, weight in zip(ordered, final_weights)
        ]
        if self.explain:
            return GenericResult(
                value={
                    "task": "optimize_portfolio",
                    "as_of_date": context.as_of_date.isoformat(),
                    "input_count": len(ordered),
                    "long_only": context.long_only,
                    "max_abs_weight": context.max_abs_weight,
                    "uses_datagen": not bool(context.signals),
                    "planned_outputs": ["OptimizerAllocation"],
                }
            )
        return GenericResult(value=allocations)


class ConstructTargetPositionsContext(ContextBase):
    as_of_date: date
    allocations: List[OptimizerAllocation] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    portfolio_partition: Optional[PortfolioDatePartitionContext] = None
    n_assets: int = 10
    ic: float = 0.05
    seed: Optional[int] = None
    long_only: bool = False
    max_abs_weight: float = 1.0
    top_k: Optional[int] = None
    portfolio_notional: float = 1_000_000.0
    prices: Dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)

    @model_validator(mode="after")
    def validate_portfolio_notional(self):
        if self.portfolio_notional <= 0:
            raise ValueError("portfolio_notional must be positive.")
        if self.portfolio_partition and self.portfolio_partition.as_of_date != self.as_of_date:
            raise ValueError("portfolio_partition.as_of_date must match as_of_date.")
        return self


class ConstructTargetPositionsModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return ConstructTargetPositionsContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: ConstructTargetPositionsContext) -> GenericResult:
        allocations = context.allocations
        if not allocations:
            allocations = OptimizePortfolioModel()(
                OptimizePortfolioContext(
                    as_of_date=context.as_of_date,
                    symbols=context.symbols,
                    n_assets=context.n_assets,
                    ic=context.ic,
                    seed=context.seed,
                    long_only=context.long_only,
                    max_abs_weight=context.max_abs_weight,
                    top_k=context.top_k,
                )
            ).value

        targets: List[TargetPositionRecord] = []
        for allocation in allocations:
            target_notional = allocation.weight * context.portfolio_notional
            price = context.prices.get(allocation.instrument_id)
            quantity = None if not price else target_notional / price
            targets.append(
                TargetPositionRecord(
                    as_of_date=context.as_of_date,
                    instrument_id=allocation.instrument_id,
                    target_weight=allocation.weight,
                    target_notional=target_notional,
                    target_quantity=quantity,
                )
            )
        if self.explain:
            return GenericResult(
                value={
                    "task": "construct_target_positions",
                    "as_of_date": context.as_of_date.isoformat(),
                    "allocation_count": len(allocations),
                    "portfolio_notional": context.portfolio_notional,
                    "uses_datagen": not bool(context.allocations),
                    "planned_outputs": ["TargetPositionRecord"],
                }
            )
        return GenericResult(value=targets)


class BacktestPortfolioContext(ContextBase):
    as_of_date: date
    strategy_partition: Optional[StrategyDatePartitionContext] = None
    targets: List[TargetPositionRecord] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    n_assets: int = 10
    ic: float = 0.05
    seed: Optional[int] = None
    portfolio_notional: float = 1_000_000.0
    prices: Dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)


class BacktestPortfolioModel(CallableModel):
    explain: bool = False
    strategy_id: str = "default-strategy"

    @property
    def context_type(self) -> Type[ContextType]:
        return BacktestPortfolioContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: BacktestPortfolioContext) -> GenericResult:
        targets = context.targets
        if not targets:
            targets = ConstructTargetPositionsModel()(
                ConstructTargetPositionsContext(
                    as_of_date=context.as_of_date,
                    symbols=context.symbols,
                    n_assets=context.n_assets,
                    ic=context.ic,
                    seed=context.seed,
                    portfolio_notional=context.portfolio_notional,
                    prices=context.prices,
                )
            ).value

        gross_exposure = sum(abs(target.target_weight) for target in targets)
        synthetic_return = 0.001 * gross_exposure
        synthetic_turnover = 0.5 * gross_exposure

        record = BacktestResultRecord(
            strategy_id=(context.strategy_partition.strategy_id if context.strategy_partition else self.strategy_id),
            as_of_date=context.as_of_date,
            return_pct=synthetic_return,
            turnover=synthetic_turnover,
            drawdown=-0.25 * synthetic_return,
        )

        if self.explain:
            return GenericResult(
                value={
                    "task": "backtest_portfolio",
                    "as_of_date": context.as_of_date.isoformat(),
                    "target_count": len(targets),
                    "planned_outputs": ["BacktestResultRecord"],
                }
            )

        return GenericResult(value=[record])


class EvaluateRealPortfolioContext(ContextBase):
    as_of_date: date
    portfolio_partition: Optional[PortfolioDatePartitionContext] = None
    expected_targets: List[TargetPositionRecord] = Field(default_factory=list)
    realized_positions: List[PortfolioSnapshot] = Field(default_factory=list)
    tolerance_bps: float = 25.0

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)


class EvaluateRealPortfolioModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return EvaluateRealPortfolioContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: EvaluateRealPortfolioContext) -> GenericResult:
        expected = {target.instrument_id: target.target_weight for target in context.expected_targets}
        realized = {position.instrument_id: position.market_value for position in context.realized_positions}

        mismatch_count = 0
        for instrument_id, expected_weight in expected.items():
            realized_weight = realized.get(instrument_id, 0.0)
            if abs(expected_weight - realized_weight) * 10_000 > context.tolerance_bps:
                mismatch_count += 1

        if self.explain:
            return GenericResult(
                value={
                    "task": "evaluate_real_portfolio",
                    "as_of_date": context.as_of_date.isoformat(),
                    "expected_count": len(context.expected_targets),
                    "realized_count": len(context.realized_positions),
                    "planned_outputs": ["evaluation_summary"],
                }
            )

        return GenericResult(
            value={
                "as_of_date": context.as_of_date.isoformat(),
                "tolerance_bps": context.tolerance_bps,
                "mismatch_count": mismatch_count,
            }
        )


class BuildAlphaReportContext(ContextBase):
    as_of_date: date
    report_partition: Optional[ReportDatePartitionContext] = None
    strategy_partition: Optional[StrategyDatePartitionContext] = None
    signals: List[SignalRecord] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    n_assets: int = 10
    ic: float = 0.05
    seed: Optional[int] = None

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)


class BuildAlphaReportModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return BuildAlphaReportContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: BuildAlphaReportContext) -> GenericResult:
        signals = context.signals or _synthetic_signals(
            as_of_date=context.as_of_date,
            n_assets=context.n_assets,
            symbols=context.symbols,
            ic=context.ic,
            seed=context.seed,
        )
        top = sorted(signals, key=lambda signal: signal.value, reverse=True)[:5]

        report = ReportMetadata(
            report_id=(context.report_partition.report_id if context.report_partition else f"alpha-{context.as_of_date.isoformat()}"),
            report_type="alpha",
            as_of_date=context.as_of_date,
            schema_metadata=SchemaMetadata(schema_name="alpha-report", schema_version=1),
        )

        if self.explain:
            return GenericResult(
                value={
                    "task": "build_alpha_report",
                    "as_of_date": context.as_of_date.isoformat(),
                    "signal_count": len(signals),
                    "planned_outputs": ["ReportMetadata", "top_signals"],
                }
            )

        return GenericResult(value={"report": report.model_dump(mode="json"), "top_signals": [item.model_dump(mode="json") for item in top]})


class BuildRiskReportContext(ContextBase):
    as_of_date: date
    report_partition: Optional[ReportDatePartitionContext] = None
    portfolio_partition: Optional[PortfolioDatePartitionContext] = None
    targets: List[TargetPositionRecord] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    n_assets: int = 10
    ic: float = 0.05
    seed: Optional[int] = None
    portfolio_notional: float = 1_000_000.0
    prices: Dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        return _coerce_date_only_context(cls, value, handler)


class BuildRiskReportModel(CallableModel):
    explain: bool = False

    @property
    def context_type(self) -> Type[ContextType]:
        return BuildRiskReportContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: BuildRiskReportContext) -> GenericResult:
        targets = context.targets
        if not targets:
            targets = ConstructTargetPositionsModel()(
                ConstructTargetPositionsContext(
                    as_of_date=context.as_of_date,
                    symbols=context.symbols,
                    n_assets=context.n_assets,
                    ic=context.ic,
                    seed=context.seed,
                    portfolio_notional=context.portfolio_notional,
                    prices=context.prices,
                )
            ).value

        gross = sum(abs(target.target_weight) for target in targets)
        net = sum(target.target_weight for target in targets)

        report = ReportMetadata(
            report_id=(context.report_partition.report_id if context.report_partition else f"risk-{context.as_of_date.isoformat()}"),
            report_type="risk",
            as_of_date=context.as_of_date,
            schema_metadata=SchemaMetadata(schema_name="risk-report", schema_version=1),
        )

        if self.explain:
            return GenericResult(
                value={
                    "task": "build_risk_report",
                    "as_of_date": context.as_of_date.isoformat(),
                    "target_count": len(targets),
                    "planned_outputs": ["ReportMetadata", "risk_summary"],
                }
            )

        return GenericResult(
            value={
                "report": report.model_dump(mode="json"),
                "risk_summary": {
                    "gross_exposure": gross,
                    "net_exposure": net,
                },
            }
        )
