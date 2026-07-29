import json
from typing import ClassVar

import pyarrow.parquet as pq
import pytest
from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType
from ccflow_etl import LocalFileOutput, NoOpArtifactStore
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from finance_flow import *
from finance_flow import (
    DailyBar,
    MassiveDailyBarsArtifactContext,
    MassiveDailyBarsArtifactModel,
    MassiveDailyBarsNormalizeContext,
    MassiveDailyBarsNormalizeModel,
    normalize_massive_daily_bars,
)


class RawDailyAggregateContext(DateContext):
    ticker: str


class FakeRawDailyAggregateModel(CallableModel):
    explain: bool = False
    calls: ClassVar[list[RawDailyAggregateContext]] = []

    @property
    def context_type(self) -> type[ContextType]:
        return RawDailyAggregateContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: RawDailyAggregateContext) -> GenericResult:
        self.calls.append(context)
        return GenericResult(
            value={
                "status": "planned" if self.explain else "written",
                "request": {"url": f"/v2/aggs/ticker/{context.ticker}/range/1/day/{context.date}/{context.date}", "params": {"adjusted": True}},
            }
        )


class RecordingInputStore:
    def __init__(self, payload):
        self.payload = payload
        self.reads = []

    def artifact_uri(self, key):
        return f"memory://{key}"

    def read(self, key):
        self.reads.append(key)
        return json.dumps(self.payload).encode("utf-8")


def test_all():
    assert True


def test_normalize_massive_daily_bars_from_synthetic_fixture_rows():
    bars = normalize_massive_daily_bars(
        [
            {
                "date": "2024-01-03",
                "ticker": "AAA",
                "open": "102.50",
                "high": "104.20",
                "low": "101.90",
                "close": "103.60",
                "volume": "1320000",
                "vwap": "103.12",
                "transactions": "11880",
            }
        ],
        ticker="AAA",
        session_date="2024-01-03",
    )

    assert bars == [
        DailyBar(
            ticker="AAA",
            date="2024-01-03",
            open=102.50,
            high=104.20,
            low=101.90,
            close=103.60,
            volume=1320000,
            vwap=103.12,
            transactions=11880,
        )
    ]


def test_normalize_massive_daily_bars_from_live_response_shape():
    bars = normalize_massive_daily_bars(
        {
            "ticker": "AAPL",
            "results": [
                {
                    "T": "AAPL",
                    "t": 1704240000000,
                    "o": 184.22,
                    "h": 185.88,
                    "l": 183.43,
                    "c": 184.95,
                    "v": 58414500,
                    "vw": 184.71,
                    "n": 521321,
                }
            ],
        },
        ticker="AAPL",
        session_date="2024-01-03",
    )

    assert bars[0].model_dump() == {
        "ticker": "AAPL",
        "date": "2024-01-03",
        "open": 184.22,
        "high": 185.88,
        "low": 183.43,
        "close": 184.95,
        "volume": 58414500,
        "vwap": 184.71,
        "transactions": 521321,
    }


def test_massive_daily_bars_normalize_model_returns_daily_bars():
    result = MassiveDailyBarsNormalizeModel()(
        MassiveDailyBarsNormalizeContext(
            payload=[
                {
                    "date": "2024-01-03",
                    "ticker": "AAA",
                    "open": "102.50",
                    "high": "104.20",
                    "low": "101.90",
                    "close": "103.60",
                    "volume": "1320000",
                    "vwap": "103.12",
                    "transactions": "11880",
                }
            ],
            ticker="AAA",
            session_date="2024-01-03",
        )
    )

    assert result.value == [
        DailyBar(
            ticker="AAA",
            date="2024-01-03",
            open=102.50,
            high=104.20,
            low=101.90,
            close=103.60,
            volume=1320000,
            vwap=103.12,
            transactions=11880,
        )
    ]


def test_normalize_massive_daily_bars_rejects_corrupt_payloads():
    with pytest.raises(ValueError, match="Missing .* field"):
        normalize_massive_daily_bars({"results": [{"T": "AAA", "o": 102.5}]}, ticker="AAA", session_date="2024-01-03")


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"status": "ERROR", "results": []}, "status is not OK"),
        ({"results": [{"T": "BBB", "o": 1, "h": 2, "l": 1, "c": 2, "v": 100}]}, "Ticker mismatch"),
        ({"results": [{"T": "AAA", "date": "2024-01-04", "o": 1, "h": 2, "l": 1, "c": 2, "v": 100}]}, "Date mismatch"),
        ({"results": [{"T": "AAA", "o": 1, "h": 2, "l": 1, "c": 2, "v": -1}]}, "non-negative"),
        (
            {
                "results": [
                    {"T": "AAA", "o": 1, "h": 2, "l": 1, "c": 2, "v": 100},
                    {"T": "AAA", "o": 1, "h": 2, "l": 1, "c": 2, "v": 200},
                ]
            },
            "Duplicate daily bar",
        ),
    ],
)
def test_normalize_massive_daily_bars_validates_provider_payload(payload, match):
    with pytest.raises(ValueError, match=match):
        normalize_massive_daily_bars(payload, ticker="AAA", session_date="2024-01-03")


def test_massive_daily_bars_artifact_model_explain_plans_raw_and_parquet_output():
    raw_model = FakeRawDailyAggregateModel()
    model = MassiveDailyBarsArtifactModel(
        input_store=NoOpArtifactStore(uri_prefix="noop://raw"),
        output=NoOpArtifactStore(uri_prefix="noop://bars"),
        raw_model=raw_model,
        explain=True,
    )

    payload = model(MassiveDailyBarsArtifactContext(ticker="AAPL", date="2024-01-03")).value

    assert payload["status"] == "planned"
    assert payload["input_key"] == "massive/stocks/rest/daily-aggs/json/2024-01-03/AAPL.json"
    assert payload["output_key"] == "massive/stocks/bars/daily/parquet/2024-01-03/AAPL.parquet"
    assert payload["schema_name"] == "daily_bar"
    assert payload["schema_version"] == "1"
    assert payload["output_writes"][0]["status"] == "planned"
    assert payload["output_writes"][0]["artifact"]["media_type"] == "application/vnd.apache.parquet"
    assert payload["raw_plan"]["request"]["url"] == "/v2/aggs/ticker/AAPL/range/1/day/2024-01-03/2024-01-03"


def test_massive_daily_bars_parquet_task_config_resolves(tmp_path):
    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - output: /outputs/noop
    - task: massive_daily_bars_parquet

hydra:
    searchpath:
        - pkg://ccflow_etl.config
        - pkg://finance_flow.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    task = instantiate(cfg.task_model)

    assert isinstance(task, MassiveDailyBarsArtifactModel)
    assert isinstance(task.input_store, NoOpArtifactStore)
    assert isinstance(task.output, NoOpArtifactStore)
    assert cfg.callable == "/task_model"


def test_massive_daily_bars_artifact_model_exposes_raw_dependency_context():
    raw_model = FakeRawDailyAggregateModel()
    model = MassiveDailyBarsArtifactModel(input_store=NoOpArtifactStore(), output=NoOpArtifactStore(), raw_model=raw_model)

    deps = model.__deps__(MassiveDailyBarsArtifactContext(ticker="AAPL", date="2024-01-03"))

    assert deps == [(raw_model, [RawDailyAggregateContext(ticker="AAPL", date="2024-01-03")])]


def test_massive_daily_bars_artifact_model_writes_parquet(tmp_path):
    input_store = RecordingInputStore(
        {
            "status": "OK",
            "ticker": "AAPL",
            "results": [{"T": "AAPL", "o": 184.22, "h": 185.88, "l": 183.43, "c": 184.95, "v": 58414500, "vw": 184.71, "n": 521321}],
        }
    )
    output = LocalFileOutput(path=tmp_path)
    model = MassiveDailyBarsArtifactModel(input_store=input_store, output=output)

    payload = model(MassiveDailyBarsArtifactContext(ticker="AAPL", date="2024-01-03")).value

    output_path = tmp_path / "massive" / "stocks" / "bars" / "daily" / "parquet" / "2024-01-03" / "AAPL.parquet"
    assert payload["status"] == "written"
    assert payload["row_count"] == 1
    assert payload["input_read"]["uri"] == "memory://massive/stocks/rest/daily-aggs/json/2024-01-03/AAPL.json"
    assert input_store.reads == ["massive/stocks/rest/daily-aggs/json/2024-01-03/AAPL.json"]
    assert pq.read_table(output_path).to_pylist() == [
        {
            "ticker": "AAPL",
            "date": "2024-01-03",
            "open": 184.22,
            "high": 185.88,
            "low": 183.43,
            "close": 184.95,
            "volume": 58414500,
            "vwap": 184.71,
            "transactions": 521321,
        }
    ]


def test_massive_daily_bars_artifact_model_skips_existing_output(tmp_path):
    output_path = tmp_path / "massive" / "stocks" / "bars" / "daily" / "parquet" / "2024-01-03" / "AAPL.parquet"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"existing")
    input_store = RecordingInputStore({"status": "OK", "results": []})
    model = MassiveDailyBarsArtifactModel(input_store=input_store, output=LocalFileOutput(path=tmp_path))

    payload = model(MassiveDailyBarsArtifactContext(ticker="AAPL", date="2024-01-03")).value

    assert payload["status"] == "exists"
    assert payload["will_read_input"] is False
    assert input_store.reads == []
