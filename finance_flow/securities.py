import gzip
import json
import logging
import re
from datetime import date, timedelta
from typing import Any

from ccflow import BaseModel, CallableModel, ContextType, DateContext, DateRangeContext, Flow, GenericResult, GraphDepList
from ccflow_etl import DatasetDefinition
from pydantic import Field

__all__ = (
    "ListingsCoverageModel",
    "ListingsResolutionCheckModel",
    "SecurityListingsModel",
    "SecurityMaster",
    "SymbolHistoryModel",
    "figi_fill_status",
    "name_tokens",
    "symbol_history",
)

# Words that describe the security or legal form rather than identify the issuer.
_GENERIC_NAME_WORDS = frozenset(
    [
        "A",
        "B",
        "C",
        "THE",
        "OF",
        "AND",
        "EACH",
        "ONE",
        "INC",
        "INCORPORATED",
        "CORP",
        "CORPORATION",
        "CO",
        "COMPANY",
        "LTD",
        "LIMITED",
        "PLC",
        "LLC",
        "LP",
        "SA",
        "NV",
        "AG",
        "SE",
        "CLASS",
        "ORDINARY",
        "COMMON",
        "STOCK",
        "SHARE",
        "SHARES",
        "SHS",
        "REG",
        "NY",
        "SPONSORED",
        "SPON",
        "UNIT",
        "UNITS",
        "WARRANT",
        "WARRANTS",
        "RIGHT",
        "RIGHTS",
        "REDEEMABLE",
        "DEPOSITARY",
        "RECEIPT",
        "RECEIPTS",
        "ADR",
        "ADS",
    ]
)
# Massive types whose OpenFIGI security type must match, and the OpenFIGI types other Massive types must not have.
_REQUIRED_FIGI_TYPES = {"WARRANT": "Warrant", "UNIT": "Unit", "RIGHT": "Right"}
_DERIVATIVE_FIGI_TYPES = frozenset({"Warrant", "Unit", "Right", "Option"})
# Listing name words that show a listing typed otherwise is really one of these derivatives.
_DERIVATIVE_NAME_WORDS = {"Warrant": ("WARRANT", "WARRANTS"), "Unit": ("UNIT", "UNITS"), "Right": ("RIGHT", "RIGHTS")}
_LISTING_FIELDS = ("ticker", "name", "type", "market", "locale", "primary_exchange", "currency_name", "cik", "active")


def name_tokens(name: str | None) -> list[str]:
    """Issuer-identifying words of a security name, uppercased, without punctuation or generic legal and type words."""
    words = re.split(r"[^A-Z0-9]+", (name or "").upper())
    # Warrant expiry codes such as CW27 and bare numbers do not identify the issuer.
    return [word for word in words if word and not word.isdigit() and not re.fullmatch(r"CW\d+", word) and word not in _GENERIC_NAME_WORDS]


def _tokens_match(left: str, right: str) -> bool:
    shorter, longer = sorted((left, right), key=len)
    return shorter == longer or (len(shorter) >= 3 and longer.startswith(shorter))


def figi_fill_status(listing: dict[str, Any], match: dict[str, Any], min_name_overlap: float = 0.5) -> str:
    """Whether a FIGI match fits the listing: ``matched``, ``rejected_type``, or ``rejected_name``.

    Warrants, units, and rights must match the same OpenFIGI security type, and other listings must not match a
    derivative type unless their own name says they are one (a listing typed as common stock named "... Units"). At least ``min_name_overlap`` of the match's issuer words must appear in the listing name, where
    a word matches another that it is a prefix of (``CORP`` matches ``CORPORATION``).
    """
    figi_type = match.get("securityType2")
    required = _REQUIRED_FIGI_TYPES.get(listing.get("type") or "")
    listing_words = set(re.split(r"[^A-Z]+", (listing.get("name") or "").upper()))
    named_derivative = any(word in listing_words for word in _DERIVATIVE_NAME_WORDS.get(figi_type or "", ()))
    if (required and figi_type != required) or (not required and figi_type in _DERIVATIVE_FIGI_TYPES and not named_derivative):
        return "rejected_type"
    match_words = name_tokens(match.get("name"))
    listing_words = name_tokens(listing.get("name"))
    if not match_words or not listing_words:
        return "rejected_name"
    found = sum(any(_tokens_match(word, other) for other in listing_words) for word in match_words)
    return "matched" if found and found / len(match_words) >= min_name_overlap else "rejected_name"


class SecurityListingsModel(CallableModel):
    """Build one session's listings from a listings snapshot, filling missing composite FIGIs from other sources.

    Each snapshot record becomes one row keyed by ticker. Records with a composite FIGI keep it. For the rest, the
    ``fills`` are tried in order, and the first match that passes ``figi_fill_status`` supplies the FIGIs. A listing no
    fill can identify keeps blank FIGIs. ``security_id`` is the resolved composite FIGI.

    Identifier fills arrive later than the listings, so a listing an identifier fill resolved on the previous session
    keeps those FIGIs, with ``fill_status`` ``carried``, while its name and type are unchanged and no fill resolves it.
    """

    snapshot: DatasetDefinition = Field(description="Listings snapshot partitions: JSON with records under records_key.")
    snapshot_model: CallableModel | None = Field(default=None, description="Model that produces the snapshot for a date, declared as a dependency.")
    records_key: str = "results"
    fills: dict[str, DatasetDefinition] = Field(
        default_factory=dict, description="FIGI fill bundles by source name, tried in order. Rows carry record.ticker, status, and matches."
    )
    fill_models: dict[str, CallableModel] = Field(default_factory=dict, description="Models that produce the fill bundles, declared as dependencies.")
    identifier_fills: list[str] = Field(
        default_factory=list,
        description="Fill sources matched by identifier (CUSIP or ISIN) rather than ticker, accepted without the type and name check.",
    )
    manual_figis: str | None = Field(
        default=None,
        description=(
            "CSV of manually resolved FIGIs (ticker, valid_from, valid_to, composite_figi, share_class_figi, source), as a file path or"
            " package:resource. Applied to listings that no fill identifies, within each row's date window."
        ),
    )
    output: DatasetDefinition
    min_name_overlap: float = Field(default=0.5, ge=0, le=1)
    carry_lookback_days: int = Field(default=10, ge=1, description="How far back to look for the previous session's listings to carry from.")
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

    def _carried(self, value: date) -> tuple[date | None, dict[str, dict[str, Any]]]:
        """The previous session's listings resolved by identifier fills, by ticker."""
        if not self.identifier_fills:
            return None, {}
        import polars as pl

        sessions = self.output.partitions(value - timedelta(days=self.carry_lookback_days), value - timedelta(days=1))
        previous = next((day for day in reversed(sessions) if self.output.exists(day)), None)
        if previous is None:
            return None, {}
        frame = self.output.read_frame(previous).filter(pl.col("figi_source").is_in(self.identifier_fills) & pl.col("security_id").is_not_null())
        rows = frame.select("ticker", "name", "type", "composite_figi", "share_class_figi", "figi_source").to_dicts()
        return previous, {row["ticker"]: row for row in rows}

    @Flow.deps
    def __deps__(self, context: DateContext) -> GraphDepList:
        if not self._will_build(context):
            return []
        models = ([self.snapshot_model] if self.snapshot_model is not None else []) + list(self.fill_models.values())
        return [(model, [self._dependency_context(model, context.date)]) for model in models]

    def _fill_matches(self, value: date) -> dict[str, dict[str, dict[str, Any]]]:
        fills = {}
        for source, dataset in self.fills.items():
            rows = {}
            if dataset.exists(value):
                payload = dataset.read_bytes(value)
                if dataset.format == "jsonl.gz":
                    payload = gzip.decompress(payload)
                for line in payload.splitlines():
                    row = json.loads(line)
                    rows[row["record"]["ticker"]] = row
            fills[source] = rows
        return fills

    def _manual_figis(self) -> dict[str, list[dict[str, Any]]]:
        if not self.manual_figis:
            return {}
        import csv
        import io
        from importlib.resources import files

        if ":" in self.manual_figis and not self.manual_figis.startswith("/"):
            package, resource = self.manual_figis.split(":", 1)
            text = files(package).joinpath(resource).read_text()
        else:
            with open(self.manual_figis) as handle:
                text = handle.read()
        rows: dict[str, list[dict[str, Any]]] = {}
        for row in csv.DictReader(io.StringIO(text)):
            row["valid_from"], row["valid_to"] = date.fromisoformat(row["valid_from"]), date.fromisoformat(row["valid_to"])
            rows.setdefault(row["ticker"], []).append(row)
        return rows

    def _listing_row(
        self,
        record: dict[str, Any],
        fills: dict[str, dict[str, dict[str, Any]]],
        manual: dict[str, list[dict[str, Any]]] | None = None,
        value: date | None = None,
        carried: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        row = {field: record.get(field) for field in _LISTING_FIELDS}
        row |= {"massive_composite_figi": record.get("composite_figi"), "massive_share_class_figi": record.get("share_class_figi")}
        if record.get("composite_figi"):
            return row | {
                "composite_figi": record["composite_figi"],
                "share_class_figi": record.get("share_class_figi"),
                "figi_source": "massive",
                "fill_status": None,
            }
        status = "no_fill"
        for source, rows in fills.items():
            fill = rows.get(record["ticker"])
            if fill is None:
                continue
            if fill.get("status") != "matched" or not fill.get("matches"):
                status = fill.get("status") or "no_match"
                continue
            match = fill["matches"][0]
            status = "matched" if source in self.identifier_fills else figi_fill_status(record, match, self.min_name_overlap)
            if status == "matched":
                return row | {
                    "composite_figi": match.get("compositeFIGI"),
                    "share_class_figi": match.get("shareClassFIGI"),
                    "figi_source": source,
                    "fill_status": status,
                }
        prior = (carried or {}).get(record["ticker"])
        if prior and (prior["name"], prior["type"]) == (record.get("name"), record.get("type")):
            return row | {
                "composite_figi": prior["composite_figi"],
                "share_class_figi": prior["share_class_figi"],
                "figi_source": prior["figi_source"],
                "fill_status": "carried",
            }
        for entry in (manual or {}).get(record["ticker"], []):
            if value is not None and entry["valid_from"] <= value <= entry["valid_to"] and entry.get("composite_figi"):
                return row | {
                    "composite_figi": entry["composite_figi"],
                    "share_class_figi": entry.get("share_class_figi") or None,
                    "figi_source": entry.get("source") or "manual",
                    "fill_status": "manual",
                }
        return row | {"composite_figi": None, "share_class_figi": None, "figi_source": None, "fill_status": status}

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        import polars as pl

        plan = {
            **self.output.plan(context.date),
            "snapshot_key": self.snapshot.key(context.date),
            "fill_keys": {s: d.key(context.date) for s, d in self.fills.items()},
        }
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self._will_build(context):
            return GenericResult(value={**plan, "status": "exists"})
        for model in ([self.snapshot_model] if self.snapshot_model is not None else []) + list(self.fill_models.values()):
            model(self._dependency_context(model, context.date))
        if not self.snapshot.exists(context.date):
            return GenericResult(value={**plan, "status": "no_snapshot"})
        payload = json.loads(self.snapshot.read_bytes(context.date))
        records = [record for record in payload.get(self.records_key, []) if isinstance(record, dict) and record.get("ticker")]
        fills = self._fill_matches(context.date)
        manual = self._manual_figis()
        carried_from, carried = self._carried(context.date)
        rows = [self._listing_row(record, fills, manual, context.date, carried) for record in records]
        frame = pl.DataFrame(rows, infer_schema_length=None).with_columns(
            pl.lit(context.date).alias("date"), pl.col("composite_figi").alias("security_id")
        )
        figi_sources = frame.group_by("figi_source").len().rows()
        fill_statuses = frame.filter(pl.col("fill_status").is_not_null()).group_by("fill_status").len().rows()
        lineage = {
            "sources": {
                "snapshot": self.snapshot.uri(context.date),
                **{source: dataset.uri(context.date) for source, dataset in self.fills.items()},
                **({"carried_from": self.output.uri(carried_from)} if carried_from else {}),
            },
            "quality": {
                "row_count": frame.height,
                "missing_security_id_count": frame["security_id"].null_count(),
                "figi_source_counts": {str(source): count for source, count in figi_sources},
                "fill_status_counts": {str(status): count for status, count in fill_statuses},
            },
            "transform": {"model": f"{type(self).__module__}.{type(self).__qualname__}", "min_name_overlap": self.min_name_overlap},
        }
        writes = self.output.write_frame(context.date, frame, lineage=lineage, overwrite=self.overwrite_output)
        return GenericResult(value={**plan, "status": "written", **lineage["quality"], "output_writes": writes})


_SYMBOL_ATTRIBUTES = ("name", "type", "primary_exchange", "composite_figi", "share_class_figi")


def symbol_history(listings: Any, as_of: date, attributes: Any = None) -> Any:
    """Collapse daily listings into ``(security_id, ticker, valid_from, valid_to)`` runs as of a date.

    A run ends when a security's ticker changes or when it is missing from a session's listings. ``listed`` is true when
    the run reaches the last session. Runs are found from the identifier columns alone, dictionary encoded. When
    ``attributes(day)`` is given, it returns that day's listings, and each run gets the name, type, primary exchange,
    and FIGIs listed on its last day, read one day at a time so memory stays small over long histories.
    """
    import polars as pl

    keys = (
        listings.filter(pl.col("security_id").is_not_null() & (pl.col("date") <= as_of))
        .select(pl.col("security_id").cast(pl.Categorical), pl.col("ticker").cast(pl.Categorical), "date")
        .collect(engine="streaming")
    )
    if keys.is_empty():
        return pl.LazyFrame(schema={"security_id": pl.String, "ticker": pl.String, "valid_from": pl.Date, "valid_to": pl.Date, "listed": pl.Boolean})
    last_session = keys["date"].max()
    keys = keys.with_columns(pl.col("date").rank("dense").cast(pl.UInt32).alias("session")).sort("security_id", "date", "ticker")
    new_run = (pl.col("ticker") != pl.col("ticker").shift(1).over("security_id")) | (
        pl.col("session") - pl.col("session").shift(1).over("security_id") > 1
    )
    runs = (
        keys.with_columns(new_run.fill_null(True).cum_sum().over("security_id").alias("run"))
        .group_by("security_id", "run")
        .agg(pl.col("ticker").first(), pl.col("date").min().alias("valid_from"), pl.col("date").max().alias("valid_to"))
        .select(pl.col("security_id").cast(pl.String), pl.col("ticker").cast(pl.String), "valid_from", "valid_to")
    )
    del keys
    if attributes is not None:
        rows = []
        for day, ends in runs.select("security_id", "ticker", "valid_to").partition_by("valid_to", as_dict=True).items():
            frame = attributes(day[0]).select("security_id", "ticker", *_SYMBOL_ATTRIBUTES)
            rows.append(
                frame.join(ends.select("security_id", "ticker"), on=["security_id", "ticker"], how="semi")
                .unique(["security_id", "ticker"])
                .with_columns(pl.lit(day[0]).alias("valid_to"))
            )
        runs = runs.join(pl.concat(rows, how="diagonal_relaxed"), on=["security_id", "ticker", "valid_to"], how="left")
    return (
        runs.with_columns((pl.col("valid_to") == last_session).alias("listed"), pl.lit(as_of).alias("date")).sort("security_id", "valid_from").lazy()
    )


class SymbolHistoryModel(CallableModel):
    """Write each security's ticker history, as known on a date, from all listings up to that date."""

    listings: DatasetDefinition
    listings_model: CallableModel | None = Field(default=None, description="Model that produces the listings for a date, declared as a dependency.")
    output: DatasetDefinition
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

    @Flow.deps
    def __deps__(self, context: DateContext) -> GraphDepList:
        if self.listings_model is None or not self._will_build(context):
            return []
        return [(self.listings_model, [self.listings_model.context_type.model_validate({"date": context.date})])]

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        plan = {**self.output.plan(context.date), "listings": self.listings.name}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self._will_build(context):
            return GenericResult(value={**plan, "status": "exists"})
        if self.listings_model is not None:
            self.listings_model(self.listings_model.context_type.model_validate({"date": context.date}))
        if not self.listings.exists(context.date):
            return GenericResult(value={**plan, "status": "no_listings"})
        start = self.listings.first_date or context.date
        history = symbol_history(self.listings.scan(start, context.date), context.date, attributes=self.listings.read_frame).collect()
        quality = {
            "row_count": history.height,
            "security_count": history["security_id"].n_unique(),
            "listed_count": int(history["listed"].sum()),
            "multi_ticker_security_count": int((history.group_by("security_id").len()["len"] > 1).sum()),
        }
        lineage = {"sources": {"listings": self.listings.name, "start": start.isoformat(), "end": context.date.isoformat()}, "quality": quality}
        writes = self.output.write_frame(context.date, history, lineage=lineage, overwrite=self.overwrite_output)
        return GenericResult(value={**plan, "status": "written", **quality, "output_writes": writes})


class SecurityMaster(BaseModel):
    """Point-in-time security lookups over the listings and symbol history datasets, returned as lazy frames."""

    listings: DatasetDefinition
    symbol_history: DatasetDefinition
    lookback_days: int = Field(default=10, ge=1, description="How far back to look for a stored symbol history on or before a date.")

    def listings_as_of(self, as_of: date) -> Any:
        return self.listings.scan(as_of, as_of)

    def lookup(self, tickers: list[str], as_of: date) -> Any:
        """Securities listed under ``tickers`` on ``as_of``."""
        import polars as pl

        return self.listings_as_of(as_of).filter(pl.col("ticker").is_in(tickers))

    def history(self, security_ids: list[str] | None = None, as_of: date | None = None) -> Any:
        """Ticker history of ``security_ids`` (all securities when omitted) as known on ``as_of``.

        Uses the latest stored history within ``lookback_days`` of ``as_of``, or computes it from the listings.
        """
        import polars as pl

        as_of = as_of or date.today()
        stored = [
            value
            for value in reversed(self.symbol_history.partitions(as_of - timedelta(days=self.lookback_days), as_of))
            if self.symbol_history.exists(value)
        ]
        if stored:
            frame = self.symbol_history.scan(stored[0], stored[0])
        else:
            frame = symbol_history(self.listings.scan(self.listings.first_date or as_of, as_of), as_of, attributes=self.listings.read_frame)
        return frame.filter(pl.col("security_id").is_in(security_ids)) if security_ids is not None else frame

    def attach_security_id(self, frame: Any, start: date, end: date, date_col: str = "date", ticker_col: str = "ticker") -> Any:
        """Join ticker-keyed rows to the listings of their own date, adding ``security_id`` and FIGIs."""
        import polars as pl

        listings = self.listings.scan(start, end).select(
            pl.col("date").alias(date_col), pl.col("ticker").alias(ticker_col), "security_id", "composite_figi", "share_class_figi"
        )
        return frame.join(listings, on=[date_col, ticker_col], how="left")


class ListingsCoverageModel(CallableModel):
    """Report how many listings have a security ID on each session in a date range, and where their IDs came from."""

    listings: DatasetDefinition
    explain: bool = False
    logger_name: str = __name__

    @property
    def context_type(self) -> type[DateRangeContext]:
        return DateRangeContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    @Flow.call
    def __call__(self, context: DateRangeContext) -> GenericResult:
        import polars as pl

        plan = {"start": context.start_date.isoformat(), "end": context.end_date.isoformat(), "listings": self.listings.name}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        listings = self.listings.scan(context.start_date, context.end_date)
        if not listings.collect_schema().names():
            return GenericResult(value={**plan, "status": "empty", "session_count": 0})
        sessions = (
            listings.group_by("date")
            .agg(pl.len().alias("listings"), pl.col("security_id").null_count().alias("unresolved"))
            .with_columns((1 - pl.col("unresolved") / pl.col("listings")).alias("identified"))
            .sort("date")
            .collect(engine="streaming")
        )
        columns = listings.collect_schema().names()
        sources = listings.group_by("figi_source").len().collect(engine="streaming") if "figi_source" in columns else pl.DataFrame()
        last = sessions["date"].max()
        unresolved = (
            listings.filter((pl.col("date") == last) & pl.col("security_id").is_null())
            .select([column for column in ("ticker", "name", "type", "fill_status") if column in columns])
            .sort("ticker")
            .collect()
        )
        identified = sessions["identified"]
        value = {
            **plan,
            "status": "ok",
            "session_count": sessions.height,
            "identified": {"min": round(identified.min(), 4), "mean": round(identified.mean(), 4), "max": round(identified.max(), 4)},
            "listing_days": int(sessions["listings"].sum()),
            "unresolved_listing_days": int(sessions["unresolved"].sum()),
            "figi_source_counts": {str(source): count for source, count in sources.rows()},
            "sessions": [{"date": day.isoformat(), "listings": rows, "unresolved": missing} for day, rows, missing, _ in sessions.rows()],
            "last_session": last.isoformat(),
            "last_session_unresolved": unresolved.to_dicts(),
        }
        logging.getLogger(self.logger_name).info(
            "listings coverage %s..%s: %d sessions, identified min %.4f mean %.4f max %.4f, %d unresolved on %s",
            plan["start"],
            plan["end"],
            sessions.height,
            identified.min(),
            identified.mean(),
            identified.max(),
            unresolved.height,
            value["last_session"],
        )
        return GenericResult(value=value)


class ListingsResolutionCheckModel(CallableModel):
    """Check that a session's listings stay resolved to security IDs.

    The check fails when a ticker first listed on or after ``baseline_date`` is still unresolved after
    ``grace_sessions`` sessions, or when the session's unresolved count exceeds ``max_unresolved``. Tickers listed
    before ``baseline_date`` that were never resolved are the known backlog and only count toward ``max_unresolved``.
    """

    listings: DatasetDefinition
    baseline_date: date = Field(description="Listings first seen on or after this date must resolve within the grace period.")
    grace_sessions: int = Field(default=3, ge=1, description="Sessions a new listing may stay unresolved, counting its first.")
    grace_sessions_by_type: dict[str, int] = Field(
        default_factory=dict, description="Grace periods for listing types that resolve later, such as preferreds waiting for a weekly CUSIP source."
    )
    max_unresolved: int | None = Field(default=None, ge=0, description="Largest unresolved count allowed on a session.")
    ignore_types: list[str] = Field(default_factory=list, description="Listing types excluded from the check.")
    fail_on_unresolved: bool = False
    lookback_days: int = Field(default=10, ge=1, description="How far back to look for the latest session on or before the date.")
    explain: bool = False
    logger_name: str = __name__

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        import polars as pl

        if self.explain:
            return GenericResult(value={"date": context.date.isoformat(), "listings": self.listings.name, "status": "planned"})
        logger = logging.getLogger(self.logger_name)
        # Check the latest session on or before the date, so weekends and holidays check the session before them.
        available = [
            value
            for value in reversed(self.listings.partitions(context.date - timedelta(days=self.lookback_days), context.date))
            if self.listings.exists(value)
        ]
        if not available:
            raise RuntimeError(f"listings check {context.date}: no listings partition in the previous {self.lookback_days} days")
        context = context.model_copy(update={"date": available[0]})
        today = self.listings.read_frame(context.date).filter(~pl.col("type").is_in(self.ignore_types) | pl.col("type").is_null())
        unresolved = today.filter(pl.col("security_id").is_null()).select(
            [column for column in ("ticker", "name", "type", "fill_status") if column in today.columns]
        )
        start = self.listings.first_date or self.baseline_date
        first_seen = (
            self.listings.scan(start, context.date)
            .filter(pl.col("ticker").is_in(unresolved["ticker"].to_list()))
            .group_by("ticker")
            .agg(pl.col("date").min().alias("first_seen"))
            .collect(engine="streaming")
        )
        sessions = self.listings.partitions(start, context.date)
        index = {value: position for position, value in enumerate(sessions)}
        new = (
            unresolved.join(first_seen, on="ticker", how="left")
            .filter(pl.col("first_seen") >= self.baseline_date)
            .with_columns(
                pl.col("first_seen")
                .map_elements(lambda value: len(sessions) - index.get(value, len(sessions)), return_dtype=pl.Int64)
                .alias("sessions")
            )
            .sort("first_seen", "ticker")
        )
        grace = (
            pl.col("type").replace_strict(self.grace_sessions_by_type, default=self.grace_sessions, return_dtype=pl.Int64)
            if "type" in new.columns
            else pl.lit(self.grace_sessions)
        )
        overdue = new.filter(pl.col("sessions") >= grace)
        too_many = self.max_unresolved is not None and unresolved.height > self.max_unresolved
        value = {
            "date": context.date.isoformat(),
            "listing_count": today.height,
            "unresolved_count": unresolved.height,
            "max_unresolved": self.max_unresolved,
            "new_unresolved": new.to_dicts(),
            "overdue": overdue.to_dicts(),
            "status": "failed" if (overdue.height or too_many) else "ok",
        }
        logger.info(
            "listings check %s: %s listings, %s unresolved (max %s), %s new unresolved, %s overdue",
            context.date,
            today.height,
            unresolved.height,
            self.max_unresolved,
            new.height,
            overdue.height,
        )
        for row in overdue.to_dicts():
            logger.warning(
                "unresolved %s (%s, %s) first listed %s, %s sessions: %s",
                row["ticker"],
                row.get("type"),
                row.get("fill_status"),
                row["first_seen"],
                row["sessions"],
                row.get("name"),
            )
        if self.fail_on_unresolved and value["status"] == "failed":
            reasons = []
            if overdue.height:
                reasons.append(
                    f"{overdue.height} new listings unresolved after {self.grace_sessions} sessions: {', '.join(overdue['ticker'].to_list()[:20])}"
                )
            if too_many:
                reasons.append(f"{unresolved.height} unresolved listings exceed {self.max_unresolved}")
            raise RuntimeError(f"listings check {context.date}: " + "; ".join(reasons))
        return GenericResult(value=value)
