import json
from datetime import date
from typing import Any

from ccflow import CallableModel, ContextBase, ContextType, DateContext, Flow, GenericResult, ResultType
from ccflow_etl import ArtifactReadContext, ArtifactReadModel, ArtifactWriteContext, ArtifactWriteModel, PayloadCodec
from pydantic import BaseModel, field_serializer

__all__ = (
    "DailyBar",
    "MassiveDailyBarsArtifactContext",
    "MassiveDailyBarsArtifactModel",
    "MassiveDailyBarsNormalizeContext",
    "MassiveDailyBarsNormalizeModel",
    "normalize_massive_daily_bars",
)


class DailyBar(BaseModel):
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float | None = None
    transactions: int | None = None

    @field_serializer("date")
    def serialize_date(self, value: date) -> str:
        return value.isoformat()


class MassiveDailyBarsNormalizeContext(ContextBase):
    payload: dict[str, Any] | list[dict[str, Any]]
    ticker: str
    session_date: date


class MassiveDailyBarsNormalizeModel(CallableModel):
    @property
    def context_type(self) -> type[ContextType]:
        return MassiveDailyBarsNormalizeContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: MassiveDailyBarsNormalizeContext) -> GenericResult:
        return GenericResult(value=normalize_massive_daily_bars(context.payload, ticker=context.ticker, session_date=context.session_date))


class MassiveDailyBarsArtifactContext(DateContext):
    ticker: str


class MassiveDailyBarsArtifactModel(CallableModel):
    input_store: Any
    output: Any
    raw_model: CallableModel | None = None
    explain: bool = False
    input_key_template: str = "massive/stocks/rest/daily-aggs/json/{date}/{ticker}.json"
    output_key_prefix: str = "massive/stocks/bars/daily"
    overwrite_output: bool = False
    dataset_name: str = "massive-stocks-bars-daily"
    provider_name: str = "massive"
    schema_name: str = "daily_bar"
    schema_version: str = "1"
    return_type: str = "parquet"

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveDailyBarsArtifactContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def input_key(self, context: MassiveDailyBarsArtifactContext) -> str:
        return self.input_key_template.format(date=_date_value(context.date), ticker=context.ticker)

    def output_key(self, context: MassiveDailyBarsArtifactContext) -> str:
        suffix = PayloadCodec(format=self.return_type).suffix or ".bin"
        return f"{self.output_key_prefix.strip('/')}/{self.return_type}/{_date_value(context.date)}/{context.ticker}{suffix}"

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "provider": self.provider_name,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "return_type": self.return_type,
            "partition_keys": ["date", "ticker"],
            "media_types": [PayloadCodec(format=self.return_type).media_type],
        }

    def _raw_context(self, context: MassiveDailyBarsArtifactContext) -> ContextType | None:
        if self.raw_model is None:
            return None
        values = context.model_dump(mode="python")
        values.pop("type_", None)
        return self.raw_model.context_type.model_validate(values)

    def _raw_plan(self, context: MassiveDailyBarsArtifactContext) -> dict[str, Any] | None:
        raw_context = self._raw_context(context)
        if self.raw_model is None or raw_context is None:
            return None
        raw_model = self.raw_model
        if "explain" in getattr(type(raw_model), "model_fields", {}):
            raw_model = raw_model.model_copy(update={"explain": True})
        result = raw_model(raw_context)
        value = result.value if isinstance(result, GenericResult) else result
        return value if isinstance(value, dict) else value.model_dump(mode="json")

    def _metadata(self, context: MassiveDailyBarsArtifactContext, row_count: int | None = None) -> dict[str, Any]:
        metadata = {
            "date": _date_value(context.date),
            "ticker": context.ticker,
            "provider": self.provider_name,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
        }
        if row_count is not None:
            metadata["row_count"] = row_count
        return metadata

    def _output_exists(self, context: MassiveDailyBarsArtifactContext) -> bool:
        return bool(not self.overwrite_output and hasattr(self.output, "exists") and self.output.exists(self.output_key(context)))

    def _planned_write(self, context: MassiveDailyBarsArtifactContext) -> Any:
        codec = PayloadCodec(format=self.return_type)
        return ArtifactWriteModel(store=self.output)(
            ArtifactWriteContext(
                key=self.output_key(context),
                payload=b"",
                media_type=codec.media_type,
                dataset=self.dataset_name,
                stage="transform",
                overwrite=self.overwrite_output,
                dry_run=True,
                metadata=self._metadata(context),
            )
        )

    def _existing_write(self, context: MassiveDailyBarsArtifactContext) -> Any:
        codec = PayloadCodec(format=self.return_type)
        return ArtifactWriteModel(store=self.output)(
            ArtifactWriteContext(
                key=self.output_key(context),
                payload=b"",
                media_type=codec.media_type,
                dataset=self.dataset_name,
                stage="transform",
                metadata=self._metadata(context),
            )
        )

    def _write_output(self, context: MassiveDailyBarsArtifactContext, rows: list[dict[str, Any]]) -> Any:
        codec = PayloadCodec(format=self.return_type)
        return ArtifactWriteModel(store=self.output)(
            ArtifactWriteContext(
                key=self.output_key(context),
                payload=codec.encode(rows),
                media_type=codec.media_type,
                dataset=self.dataset_name,
                stage="transform",
                overwrite=self.overwrite_output,
                metadata=self._metadata(context, row_count=len(rows)),
            )
        )

    def _read_payload(self, context: MassiveDailyBarsArtifactContext) -> tuple[dict[str, Any], dict[str, Any]]:
        read_result = ArtifactReadModel(store=self.input_store)(ArtifactReadContext(key=self.input_key(context)))
        return json.loads(read_result.payload), {"key": read_result.key, "uri": read_result.uri, "status": read_result.status}

    def _plan(self, context: MassiveDailyBarsArtifactContext) -> dict[str, Any]:
        input_key = self.input_key(context)
        output_key = self.output_key(context)
        return {
            "dataset": self.dataset_name,
            "provider": self.provider_name,
            "date": _date_value(context.date),
            "ticker": context.ticker,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "return_type": self.return_type,
            "input_key": input_key,
            "input_uri": _artifact_uri(self.input_store, input_key),
            "output_key": output_key,
            "output_uri": _artifact_uri(self.output, output_key),
            "dataset_metadata": self.dataset_metadata(),
            "will_read_input": False,
            "will_publish_output": False,
            "output_writes": [self._planned_write(context).model_dump(mode="json")],
        }

    @Flow.deps
    def __deps__(self, context: MassiveDailyBarsArtifactContext) -> list[tuple[CallableModel, list[ContextType]]]:
        raw_context = self._raw_context(context)
        return [] if self.raw_model is None or raw_context is None else [(self.raw_model, [raw_context])]

    @Flow.call
    def __call__(self, context: MassiveDailyBarsArtifactContext) -> GenericResult:
        plan = self._plan(context)
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "raw_plan": self._raw_plan(context)})
        if self._output_exists(context):
            return GenericResult(
                value={
                    **plan,
                    "status": "exists",
                    "output_writes": [self._existing_write(context).model_dump(mode="json")],
                    "row_count": None,
                    "results": [],
                }
            )
        payload, input_read = self._read_payload(context)
        bars = normalize_massive_daily_bars(payload, ticker=context.ticker, session_date=context.date)
        rows = [bar.model_dump(mode="json") for bar in bars]
        write_result = self._write_output(context, rows)
        return GenericResult(
            value={
                **plan,
                "status": write_result.status,
                "will_read_input": True,
                "will_publish_output": True,
                "input_read": input_read,
                "row_count": len(rows),
                "results": rows,
                "output_writes": [write_result.model_dump(mode="json")],
            }
        )


def _items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get("results", payload.get("rows", []))
        return list(value or [])
    return list(payload or [])


def _value(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def _float(row: dict[str, Any], *names: str) -> float:
    value = _value(row, *names)
    if value is None:
        raise ValueError(f"Missing numeric field; expected one of {names}.")
    return float(value)


def _int(row: dict[str, Any], *names: str) -> int:
    value = _value(row, *names)
    if value is None:
        raise ValueError(f"Missing integer field; expected one of {names}.")
    return int(value)


def _optional_float(row: dict[str, Any], *names: str) -> float | None:
    value = _value(row, *names)
    return None if value is None else float(value)


def _optional_int(row: dict[str, Any], *names: str) -> int | None:
    value = _value(row, *names)
    return None if value is None else int(value)


def _date_value(value: str | date) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _artifact_uri(store: Any, key: str) -> str:
    if hasattr(store, "artifact_uri"):
        return store.artifact_uri(key)
    if hasattr(store, "uri"):
        return store.uri(key)
    return key


def _bar_date(value: Any, fallback: date) -> date:
    if value is None:
        return fallback
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _validate_payload_status(payload: Any) -> None:
    if isinstance(payload, dict) and "status" in payload and str(payload["status"]).upper() != "OK":
        raise ValueError(f"Massive response status is not OK: {payload['status']!r}.")


def normalize_massive_daily_bars(payload: dict[str, Any] | list[dict[str, Any]], ticker: str, session_date: str | date) -> list[DailyBar]:
    business_date = date.fromisoformat(session_date) if isinstance(session_date, str) else session_date
    expected_ticker = ticker.strip().upper()
    seen = set()
    _validate_payload_status(payload)
    bars = []
    for row in _items(payload):
        row_ticker = str(_value(row, "ticker", "T") or ticker).strip().upper()
        if row_ticker != expected_ticker:
            raise ValueError(f"Ticker mismatch: expected {expected_ticker}, got {row_ticker}.")
        row_date = _bar_date(_value(row, "date"), business_date)
        if row_date != business_date:
            raise ValueError(f"Date mismatch: expected {business_date.isoformat()}, got {row_date.isoformat()}.")
        row_key = (row_ticker, row_date)
        if row_key in seen:
            raise ValueError(f"Duplicate daily bar for {row_ticker} on {row_date.isoformat()}.")
        seen.add(row_key)
        volume = _int(row, "volume", "v")
        if volume < 0:
            raise ValueError("Daily bar volume must be non-negative.")
        bars.append(
            DailyBar(
                ticker=row_ticker,
                date=row_date,
                open=_float(row, "open", "o"),
                high=_float(row, "high", "h"),
                low=_float(row, "low", "l"),
                close=_float(row, "close", "c"),
                volume=volume,
                vwap=_optional_float(row, "vwap", "vw"),
                transactions=_optional_int(row, "transactions", "n"),
            )
        )
    return bars
