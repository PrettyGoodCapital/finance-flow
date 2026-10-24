import json
from datetime import date, timedelta
from typing import Any, Literal

from ccflow import BaseModel, CallableModel, ContextType, DateContext, Flow, GenericResult, GraphDepList
from ccflow_etl import DatasetDefinition
from pydantic import Field

from .lineage import source_identity, transform_provenance

__all__ = (
    "FACTOR_SCHEMA",
    "AdjustedBars",
    "AdjustmentFactorsModel",
)

FACTOR_SCHEMA = {
    "date": "date",
    "event_id": "str",
    "event_type": "str",
    "ticker": "str",
    "security_id": "str",
    "split_from": "float",
    "split_to": "float",
    "split_factor": "float",
    "cash_amount": "float",
    "currency": "str",
    "distribution_type": "str",
    "previous_date": "date",
    "previous_close": "float",
    "dividend_factor": "float",
    "massive_historical_factor": "float",
    "status": "str",
}


def _polars_schema() -> dict[str, Any]:
    import polars as pl

    types = {"date": pl.Date, "str": pl.String, "float": pl.Float64}
    return {name: types[kind] for name, kind in FACTOR_SCHEMA.items()}


class AdjustmentFactorsModel(CallableModel):
    """Write the price adjustment factors for the splits and dividends that take effect on a session.

    A split's factor is ``split_from / split_to``: prices before the execution date are multiplied by it. A cash
    dividend's factor is ``1 - cash_amount / previous close``, using the unadjusted close of the previous session, and
    is computed only for dividends paid in ``currency``. Each event is keyed by the ``security_id`` its ticker has in
    that session's listings, or the previous session's; events whose ticker is not listed keep a blank ID and status
    ``unlisted``. Sessions without events get an empty partition.
    """

    splits: DatasetDefinition = Field(description="Split partitions by execution date: JSON with records under records_key.")
    dividends: DatasetDefinition = Field(description="Dividend partitions by ex-dividend date: JSON with records under records_key.")
    listings: DatasetDefinition
    bars: DatasetDefinition = Field(description="Unadjusted daily bars keyed by ticker, for previous closes.")
    output: DatasetDefinition
    upstream_models: dict[str, CallableModel] = Field(
        default_factory=dict, description="Models producing the inputs for a date, declared as dependencies."
    )
    records_key: str = "results"
    currency: str = "USD"
    lookback_days: int = Field(default=10, ge=1)
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def _will_build(self, context: DateContext) -> bool:
        return not self.explain and (self.overwrite_output or not self.output.exists(context.date))

    def _dependency_context(self, model: CallableModel, value: date) -> ContextType:
        return model.context_type.model_validate({"date": value})

    @Flow.deps
    def __deps__(self, context: DateContext) -> GraphDepList:
        if not self._will_build(context):
            return []
        return [(model, [self._dependency_context(model, context.date)]) for model in self.upstream_models.values()]

    def _records(self, dataset: DatasetDefinition, value: date) -> list[dict[str, Any]]:
        if not dataset.exists(value):
            return []
        payload = json.loads(dataset.read_bytes(value))
        records = payload.get(self.records_key, []) if isinstance(payload, dict) else payload
        return [record for record in records if isinstance(record, dict) and record.get("ticker")]

    def previous_session(self, value: date) -> date | None:
        sessions = self.output.partitions(value - timedelta(days=self.lookback_days), value - timedelta(days=1))
        return sessions[-1] if sessions else None

    def _security_ids(self, value: date, previous: date | None) -> dict[str, str]:
        ids: dict[str, str] = {}
        for day in (previous, value):
            if day is not None and self.listings.exists(day):
                frame = self.listings.read_frame(day)
                ids.update({ticker: sid for ticker, sid in zip(frame["ticker"].to_list(), frame["security_id"].to_list(), strict=True) if sid})
        return ids

    def _previous_closes(self, previous: date | None) -> tuple[dict[str, float], dict[str, float]]:
        """Previous-session closes by security ID (through that session's listings) and by ticker."""
        if previous is None or not self.bars.exists(previous):
            return {}, {}
        frame = self.bars.read_frame(previous).select("ticker", "close")
        by_ticker = dict(zip(frame["ticker"].to_list(), frame["close"].to_list(), strict=True))
        if not self.listings.exists(previous):
            return {}, by_ticker
        joined = frame.join(self.listings.read_frame(previous).select("ticker", "security_id"), on="ticker").drop_nulls("security_id")
        return dict(zip(joined["security_id"].to_list(), joined["close"].to_list(), strict=True)), by_ticker

    def _rows(self, context: DateContext) -> list[dict[str, Any]]:
        previous = self.previous_session(context.date)
        ids = self._security_ids(context.date, previous)
        closes_by_id, closes_by_ticker = self._previous_closes(previous)
        rows = []
        for record in self._records(self.splits, context.date):
            split_from, split_to = record.get("split_from"), record.get("split_to")
            valid = bool(split_from) and bool(split_to) and split_from > 0 and split_to > 0
            security_id = ids.get(record["ticker"])
            rows.append(
                {
                    "event_id": record.get("id"),
                    "event_type": record.get("adjustment_type") or "split",
                    "ticker": record["ticker"],
                    "security_id": security_id,
                    "split_from": split_from,
                    "split_to": split_to,
                    "split_factor": split_from / split_to if valid else None,
                    "massive_historical_factor": record.get("historical_adjustment_factor"),
                    "status": "invalid" if not valid else ("ok" if security_id else "unlisted"),
                }
            )
        for record in self._records(self.dividends, context.date):
            cash, currency = record.get("cash_amount"), record.get("currency")
            security_id = ids.get(record["ticker"])
            # The ticker may have changed on the ex-date, so prefer the security's previous close.
            close = closes_by_id.get(security_id) if security_id else None
            close = close if close is not None else closes_by_ticker.get(record["ticker"])
            factor, status = None, "ok" if security_id else "unlisted"
            if currency != self.currency:
                status = "non_usd" if self.currency == "USD" else f"non_{self.currency.lower()}"
            elif not close:
                status = "no_previous_close" if security_id else "unlisted"
            elif not cash or cash <= 0 or cash >= close:
                status = "invalid"
            else:
                factor = 1 - cash / close
            rows.append(
                {
                    "event_id": record.get("id"),
                    "event_type": "cash_dividend",
                    "ticker": record["ticker"],
                    "security_id": security_id,
                    "cash_amount": cash,
                    "currency": currency,
                    "distribution_type": record.get("distribution_type"),
                    "previous_date": previous,
                    "previous_close": close,
                    "dividend_factor": factor,
                    "massive_historical_factor": record.get("historical_adjustment_factor"),
                    "status": status,
                }
            )
        return rows

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        import polars as pl

        plan = {**self.output.plan(context.date), "splits_key": self.splits.key(context.date), "dividends_key": self.dividends.key(context.date)}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self._will_build(context):
            return GenericResult(value={**plan, "status": "exists"})
        for model in self.upstream_models.values():
            model(self._dependency_context(model, context.date))
        rows = [{name: row.get(name) for name in FACTOR_SCHEMA} | {"date": context.date} for row in self._rows(context)]
        frame = pl.DataFrame(rows, schema=_polars_schema())
        status_counts = {str(status): count for status, count in frame.group_by("status").len().rows()}
        type_counts = {str(kind): count for kind, count in frame.group_by("event_type").len().rows()}
        lineage = {
            "sources": {
                **{
                    name: source_identity(dataset, context.date)
                    for name, dataset in (("splits", self.splits), ("dividends", self.dividends))
                    if dataset.exists(context.date)
                },
                "listings": source_identity(self.listings, context.date) if self.listings.exists(context.date) else self.listings.name,
                "bars": self.bars.name,
            },
            "quality": {"row_count": frame.height, "status_counts": status_counts, "event_type_counts": type_counts},
            "transform": transform_provenance(self),
        }
        writes = self.output.write_frame(context.date, frame, lineage=lineage, overwrite=self.overwrite_output)
        return GenericResult(value={**plan, "status": "written", **lineage["quality"], "output_writes": writes})


class AdjustedBars(BaseModel):
    """Split- or total-return-adjusted daily bars, computed when read from unadjusted bars and adjustment factors.

    A bar on date D is multiplied by the product of the factors of every event after D up to ``as_of`` (all known
    events when omitted), so earlier history moves when a new event arrives while stored bars never change. Volume is
    divided by the split factors only. Bars and events are matched by ``security_id`` from the listings, or by ticker
    when either side has none.
    """

    bars: DatasetDefinition
    factors: DatasetDefinition
    listings: DatasetDefinition
    price_columns: list[str] = Field(default_factory=lambda: ["open", "high", "low", "close", "vwap"])
    volume_column: str = "volume"

    def scan(self, start: date, end: date, mode: Literal["split", "total_return"] = "total_return", as_of: date | None = None) -> Any:
        import polars as pl

        as_of = as_of or date.today()
        key = pl.coalesce(pl.col("security_id"), pl.lit("ticker:") + pl.col("ticker")).alias("adjustment_key")
        listings = self.listings.scan(start, end).select("date", "ticker", "security_id")
        bars = self.bars.scan(start, end).join(listings, on=["date", "ticker"], how="left").with_columns(key).sort("date")
        factors = self.factors.scan(start + timedelta(days=1), as_of)
        if not factors.collect_schema().names():
            return bars.with_columns(pl.lit(1.0).alias("price_factor"), pl.lit(1.0).alias("volume_factor"))
        split = pl.col("split_factor").fill_null(1.0)
        price = split * pl.col("dividend_factor").fill_null(1.0) if mode == "total_return" else split
        events = (
            factors.filter(pl.col("date") <= as_of)
            .with_columns(key)
            .group_by("adjustment_key", "date")
            .agg(price.product().alias("event_price_factor"), split.product().alias("event_volume_factor"))
            .sort("adjustment_key", pl.col("date"), descending=[False, True])
            .with_columns(
                pl.col("event_price_factor").cum_prod().over("adjustment_key").alias("price_factor"),
                pl.col("event_volume_factor").cum_prod().over("adjustment_key").alias("volume_factor"),
            )
            .select("adjustment_key", pl.col("date").alias("event_date"), "price_factor", "volume_factor")
            .sort("event_date")
        )
        adjusted = bars.join_asof(
            events, left_on="date", right_on="event_date", by="adjustment_key", strategy="forward", allow_exact_matches=False, check_sortedness=False
        ).with_columns(pl.col("price_factor").fill_null(1.0), pl.col("volume_factor").fill_null(1.0))
        columns = [column for column in self.price_columns if column in adjusted.collect_schema().names()]
        return adjusted.with_columns(
            *[(pl.col(column) * pl.col("price_factor")).alias(column) for column in columns],
            (pl.col(self.volume_column) / pl.col("volume_factor")).alias(self.volume_column),
        ).drop("event_date")
