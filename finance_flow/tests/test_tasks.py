import pytest
from finance_etl import OptimizerAllocation, SignalRecord, UniverseMember

from finance_flow import (
    BacktestPortfolioContext,
    BacktestPortfolioModel,
    BuildAlphaReportContext,
    BuildAlphaReportModel,
    BuildRiskReportContext,
    BuildRiskReportModel,
    BuildUniverseContext,
    BuildUniverseModel,
    CalculateSignalsContext,
    CalculateSignalsModel,
    ConstructTargetPositionsContext,
    ConstructTargetPositionsModel,
    EvaluateRealPortfolioContext,
    EvaluateRealPortfolioModel,
    OptimizePortfolioContext,
    OptimizePortfolioModel,
)


def test_build_universe_model_uses_datagen_prices_for_mock_members():
    result = BuildUniverseModel()(
        BuildUniverseContext(
            as_of_date="2024-01-03",
            symbols=["AAPL", "MSFT", "NVDA"],
            exchange="XNYS",
            seed=7,
        )
    )

    members = result.value
    assert len(members) == 3
    assert all(isinstance(member, UniverseMember) for member in members)
    assert [member.symbol for member in members] == ["AAPL", "MSFT", "NVDA"]
    assert all(member.close is not None and member.close > 0 for member in members)


def test_calculate_signals_model_uses_datagen_signal_generator():
    universe = [
        UniverseMember(symbol="AAPL", exchange="XNYS", as_of_date="2024-01-03", close=184.95),
        UniverseMember(symbol="MSFT", exchange="XNYS", as_of_date="2024-01-03", close=367.12),
    ]

    result = CalculateSignalsModel()(
        CalculateSignalsContext(
            as_of_date="2024-01-03",
            universe=universe,
            seed=11,
            ic=0.1,
        )
    )

    signals = result.value
    assert len(signals) == 2
    assert all(isinstance(signal, SignalRecord) for signal in signals)
    assert {signal.instrument_id for signal in signals} == {"XNYS:AAPL", "XNYS:MSFT"}


def test_optimize_portfolio_model_produces_normalized_weights():
    signals = [
        SignalRecord(as_of_date="2024-01-03", instrument_id="XNYS:AAPL", value=0.8),
        SignalRecord(as_of_date="2024-01-03", instrument_id="XNYS:MSFT", value=-0.2),
        SignalRecord(as_of_date="2024-01-03", instrument_id="XNYS:NVDA", value=0.5),
    ]

    result = OptimizePortfolioModel()(
        OptimizePortfolioContext(
            as_of_date="2024-01-03",
            signals=signals,
            max_abs_weight=0.6,
        )
    )

    allocations = result.value
    assert all(isinstance(allocation, OptimizerAllocation) for allocation in allocations)
    gross = sum(abs(allocation.weight) for allocation in allocations)
    assert round(gross, 6) == 1.0


def test_construct_target_positions_model_maps_allocations_to_notional_and_quantity():
    allocations = [
        OptimizerAllocation(as_of_date="2024-01-03", instrument_id="XNYS:AAPL", weight=0.4, score=1.0),
        OptimizerAllocation(as_of_date="2024-01-03", instrument_id="XNYS:MSFT", weight=-0.2, score=-0.5),
    ]

    result = ConstructTargetPositionsModel()(
        ConstructTargetPositionsContext(
            as_of_date="2024-01-03",
            allocations=allocations,
            portfolio_notional=1_000_000,
            prices={"XNYS:AAPL": 200.0, "XNYS:MSFT": 400.0},
        )
    )

    targets = result.value
    assert len(targets) == 2
    assert targets[0].target_notional == 400000.0
    assert targets[0].target_quantity == 2000.0
    assert targets[1].target_notional == -200000.0
    assert targets[1].target_quantity == -500.0


def test_task_models_support_date_only_explain_context_for_cli_paths():
    build = BuildUniverseModel(explain=True)(["2025-01-02"]).value
    signals = CalculateSignalsModel(explain=True)(["2025-01-02"]).value
    optimize = OptimizePortfolioModel(explain=True)(["2025-01-02"]).value
    targets = ConstructTargetPositionsModel(explain=True)(["2025-01-02"]).value
    backtest = BacktestPortfolioModel(explain=True)(["2025-01-02"]).value
    evaluate = EvaluateRealPortfolioModel(explain=True)(["2025-01-02"]).value
    alpha = BuildAlphaReportModel(explain=True)(["2025-01-02"]).value
    risk = BuildRiskReportModel(explain=True)(["2025-01-02"]).value

    assert build["task"] == "build_universe"
    assert signals["task"] == "calculate_signals"
    assert optimize["task"] == "optimize_portfolio"
    assert targets["task"] == "construct_target_positions"
    assert backtest["task"] == "backtest_portfolio"
    assert evaluate["task"] == "evaluate_real_portfolio"
    assert alpha["task"] == "build_alpha_report"
    assert risk["task"] == "build_risk_report"
    assert build["as_of_date"] == "2025-01-02"
    assert signals["as_of_date"] == "2025-01-02"
    assert optimize["as_of_date"] == "2025-01-02"
    assert targets["as_of_date"] == "2025-01-02"
    assert backtest["as_of_date"] == "2025-01-02"
    assert evaluate["as_of_date"] == "2025-01-02"
    assert alpha["as_of_date"] == "2025-01-02"
    assert risk["as_of_date"] == "2025-01-02"


def test_backtest_portfolio_model_returns_backtest_record():
    result = BacktestPortfolioModel()(
        BacktestPortfolioContext(
            as_of_date="2024-01-03",
            symbols=["AAPL", "MSFT", "NVDA"],
            seed=17,
        )
    )

    records = result.value
    assert len(records) == 1
    assert records[0].as_of_date.isoformat() == "2024-01-03"
    assert records[0].turnover >= 0


def test_evaluate_real_portfolio_model_returns_summary():
    expected = [
        {
            "as_of_date": "2024-01-03",
            "instrument_id": "XNYS:AAPL",
            "target_weight": 0.2,
            "target_notional": 200_000.0,
            "target_quantity": 1000.0,
        }
    ]
    realized = [
        {
            "portfolio_id": "core",
            "as_of_date": "2024-01-03",
            "instrument_id": "XNYS:AAPL",
            "quantity": 1000.0,
            "market_value": 0.2,
        }
    ]

    result = EvaluateRealPortfolioModel()(
        EvaluateRealPortfolioContext(
            as_of_date="2024-01-03",
            expected_targets=expected,
            realized_positions=realized,
            tolerance_bps=10.0,
        )
    )

    assert result.value["mismatch_count"] == 0
    assert result.value["tolerance_bps"] == 10.0


def test_build_alpha_report_model_returns_top_signals_payload():
    result = BuildAlphaReportModel()(
        BuildAlphaReportContext(
            as_of_date="2024-01-03",
            symbols=["AAPL", "MSFT", "NVDA", "AMZN"],
            seed=13,
        )
    )

    assert result.value["report"]["report_type"] == "alpha"
    assert len(result.value["top_signals"]) > 0


def test_build_risk_report_model_returns_risk_summary_payload():
    result = BuildRiskReportModel()(
        BuildRiskReportContext(
            as_of_date="2024-01-03",
            symbols=["AAPL", "MSFT", "NVDA", "AMZN"],
            seed=19,
        )
    )

    assert result.value["report"]["report_type"] == "risk"
    assert "gross_exposure" in result.value["risk_summary"]


def test_task_models_validate_invalid_inputs():
    with pytest.raises(ValueError, match="max_abs_weight must be positive"):
        OptimizePortfolioModel()(
            OptimizePortfolioContext(
                as_of_date="2024-01-03",
                max_abs_weight=0,
            )
        )

    with pytest.raises(ValueError, match="portfolio_notional must be positive"):
        ConstructTargetPositionsModel()(
            ConstructTargetPositionsContext(
                as_of_date="2024-01-03",
                portfolio_notional=0,
            )
        )
