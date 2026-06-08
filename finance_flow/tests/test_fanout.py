from typing import Type

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType
from finance_etl import SymbolUniverseResult
from pydantic import Field

from finance_flow import SymbolFanoutModel


class ChildContext(DateContext):
    ticker: str
    adjusted: bool = True


class FakeUniverseModel(CallableModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return DateContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        return GenericResult(value=SymbolUniverseResult(as_of_date=context.date, symbols=["MSFT", "aapl", "AAPL"]))


class FakeChildModel(CallableModel):
    calls: list[ChildContext] = Field(default_factory=list)

    @property
    def context_type(self) -> Type[ContextType]:
        return ChildContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: ChildContext) -> GenericResult:
        self.calls.append(context)
        return GenericResult(value={"ticker": context.ticker, "date": context.date.isoformat()})


def test_symbol_fanout_exposes_universe_and_child_deps():
    model = SymbolFanoutModel(
        universe_model=FakeUniverseModel(),
        model=FakeChildModel(),
        symbol_field="ticker",
        context_values={"adjusted": False},
    )
    context = DateContext(date="2025-01-02")

    deps = model.__deps__(context)

    assert deps[0][0] is model.universe_model
    assert deps[0][1] == [context]
    assert deps[1][0] is model.model
    assert [(step.ticker, step.date.isoformat(), step.adjusted) for step in deps[1][1]] == [
        ("AAPL", "2025-01-02", False),
        ("MSFT", "2025-01-02", False),
    ]


def test_symbol_fanout_calls_child_model_for_each_symbol():
    child = FakeChildModel()
    model = SymbolFanoutModel(universe_model=FakeUniverseModel(), model=child, symbol_field="ticker")

    payload = model(DateContext(date="2025-01-02")).value

    assert payload["symbols"] == 2
    assert [output["context"]["ticker"] for output in payload["outputs"]] == ["AAPL", "MSFT"]
    assert [call.ticker for call in child.calls] == ["AAPL", "MSFT"]
