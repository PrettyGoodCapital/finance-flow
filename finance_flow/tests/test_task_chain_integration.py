from finance_etl import OptimizerAllocation, SignalRecord, TargetPositionRecord, UniverseMember

from finance_flow import (
    BuildUniverseContext,
    BuildUniverseModel,
    CalculateSignalsContext,
    CalculateSignalsModel,
    ConstructTargetPositionsContext,
    ConstructTargetPositionsModel,
    OptimizePortfolioContext,
    OptimizePortfolioModel,
)


def test_first_wave_task_chain_composes_with_typed_handoffs():
    as_of_date = "2024-01-03"
    symbols = ["AAPL", "MSFT", "NVDA", "AMZN"]

    universe_result = BuildUniverseModel()(BuildUniverseContext(as_of_date=as_of_date, symbols=symbols, seed=101))
    universe = universe_result.value
    assert len(universe) == 4
    assert all(isinstance(member, UniverseMember) for member in universe)

    signals_result = CalculateSignalsModel()(CalculateSignalsContext(as_of_date=as_of_date, universe=universe, seed=102))
    signals = signals_result.value
    assert len(signals) == 4
    assert all(isinstance(signal, SignalRecord) for signal in signals)

    allocations_result = OptimizePortfolioModel()(OptimizePortfolioContext(as_of_date=as_of_date, signals=signals))
    allocations = allocations_result.value
    assert len(allocations) == 4
    assert all(isinstance(allocation, OptimizerAllocation) for allocation in allocations)

    targets_result = ConstructTargetPositionsModel()(
        ConstructTargetPositionsContext(
            as_of_date=as_of_date,
            allocations=allocations,
            portfolio_notional=1_000_000,
            prices={
                "XNYS:AAPL": 200.0,
                "XNYS:MSFT": 400.0,
                "XNYS:NVDA": 800.0,
                "XNYS:AMZN": 100.0,
            },
        )
    )
    targets = targets_result.value
    assert len(targets) == 4
    assert all(isinstance(target, TargetPositionRecord) for target in targets)
    assert sum(abs(target.target_weight) for target in targets) > 0
