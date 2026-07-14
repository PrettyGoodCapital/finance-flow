from typing import Any, Type

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
    output: Any = None

    @property
    def context_type(self) -> Type[ContextType]:
        return ChildContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: ChildContext) -> GenericResult:
        self.calls.append(context)
        return GenericResult(value={"ticker": context.ticker, "date": context.date.isoformat(), "status": "written"})

    def output_key(self, context: ChildContext) -> str:
        return f"daily/{context.date.isoformat()}/{context.ticker}.json"


class FakeOutput:
    keys: set[str]
    prefixes: list[str]

    def __init__(self, keys):
        self.keys = set(keys)
        self.prefixes = []

    def list_keys(self, prefix=""):
        self.prefixes.append(prefix)
        return sorted(key for key in self.keys if key.startswith(prefix))


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
    assert payload["status_counts"] == {"written": 2}
    assert [call.ticker for call in child.calls] == ["AAPL", "MSFT"]


def test_symbol_fanout_can_omit_child_outputs_from_result():
    child = FakeChildModel()
    model = SymbolFanoutModel(universe_model=FakeUniverseModel(), model=child, symbol_field="ticker", include_outputs=False)

    payload = model(DateContext(date="2025-01-02")).value

    assert payload == {"symbols": 2, "status_counts": {"written": 2}}
    assert [call.ticker for call in child.calls] == ["AAPL", "MSFT"]


def test_symbol_fanout_keeps_deps_contexts_per_parent_context():
    child = FakeChildModel()
    model = SymbolFanoutModel(universe_model=FakeUniverseModel(), model=child, symbol_field="ticker")
    first = DateContext(date="2025-01-02")
    second = DateContext(date="2025-01-03")

    model.__deps__(first)
    model.__deps__(second)

    first_payload = model(first).value
    second_payload = model(second).value

    assert [output["context"]["date"] for output in first_payload["outputs"]] == ["2025-01-02", "2025-01-02"]
    assert [output["context"]["date"] for output in second_payload["outputs"]] == ["2025-01-03", "2025-01-03"]


def test_symbol_fanout_bulk_inventories_and_schedules_only_missing_outputs():
    output = FakeOutput({"daily/2025-01-02/AAPL.json"})
    child = FakeChildModel()
    child.output = output
    model = SymbolFanoutModel(universe_model=FakeUniverseModel(), model=child, symbol_field="ticker", skip_existing=True)
    context = DateContext(date="2025-01-02")

    deps = model.__deps__(context)
    payload = model(context).value

    assert output.prefixes == ["daily/2025-01-02"]
    assert [step.ticker for step in deps[1][1]] == ["MSFT"]
    assert payload["symbols"] == 2
    assert payload["missing"] == 1
    assert payload["existing"] == 1
    assert [call.ticker for call in child.calls] == ["MSFT"]
