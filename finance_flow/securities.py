import gzip
import json
import re
from datetime import date, timedelta
from typing import Any

from ccflow import BaseModel, CallableModel, ContextType, DateContext, Flow, GenericResult, GraphDepList
from ccflow_etl import DatasetDefinition
from pydantic import Field

__all__ = (
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
    """

    snapshot: DatasetDefinition = Field(description="Listings snapshot partitions: JSON with records under records_key.")
    snapshot_model: CallableModel | None = Field(default=None, description="Model that produces the snapshot for a date, declared as a dependency.")
    records_key: str = "results"
    fills: dict[str, DatasetDefinition] = Field(
        default_factory=dict, description="FIGI fill bundles by source name, tried in order. Rows carry record.ticker, status, and matches."
    )
    fill_models: dict[str, CallableModel] = Field(default_factory=dict, description="Models that produce the fill bundles, declared as dependencies.")
    manual_figis: str | None = Field(
        default=None,
        description=(
            "CSV of manually resolved FIGIs (ticker, valid_from, valid_to, composite_figi, share_class_figi, source), as a file path or"
            " package:resource. Applied to listings that no fill identifies, within each row's date window."
        ),
    )
    output: DatasetDefinition
    min_name_overlap: float = Field(default=0.5, ge=0, le=1)
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
            status = figi_fill_status(record, match, self.min_name_overlap)
            if status == "matched":
                return row | {
                    "composite_figi": match.get("compositeFIGI"),
                    "share_class_figi": match.get("shareClassFIGI"),
                    "figi_source": source,
                    "fill_status": status,
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
        rows = [self._listing_row(record, fills, manual, context.date) for record in records]
        frame = pl.DataFrame(rows, infer_schema_length=None).with_columns(
            pl.lit(context.date).alias("date"), pl.col("composite_figi").alias("security_id")
        )
        figi_sources = frame.group_by("figi_source").len().rows()
        fill_statuses = frame.filter(pl.col("fill_status").is_not_null()).group_by("fill_status").len().rows()
        lineage = {
            "sources": {"snapshot": self.snapshot.uri(context.date), **{source: dataset.uri(context.date) for source, dataset in self.fills.items()}},
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


def symbol_history(listings: Any, as_of: date) -> Any:
    """Collapse daily listings into ``(security_id, ticker, valid_from, valid_to)`` runs as of a date.

    A run ends when a security's ticker changes or when it is missing from a session's listings. Each run carries the
    latest name, type, and primary exchange seen in it, and ``listed`` is true when the run reaches ``as_of``.
    """
    import polars as pl

    frame = listings.filter(pl.col("security_id").is_not_null() & (pl.col("date") <= as_of))
    sessions = frame.select(pl.col("date").unique().sort()).with_row_index("session")
    ordered = frame.join(sessions, on="date").sort("security_id", "date", "ticker")
    runs = ordered.with_columns(
        (
            (pl.col("ticker") != pl.col("ticker").shift(1).over("security_id"))
            | (pl.col("session") - pl.col("session").shift(1).over("security_id") > 1)
        )
        .fill_null(True)
        .cum_sum()
        .over("security_id")
        .alias("run")
    )
    last_session = sessions.select(pl.col("date").max()).collect().item()
    return (
        runs.group_by("security_id", "run")
        .agg(
            pl.col("ticker").first(),
            pl.col("date").min().alias("valid_from"),
            pl.col("date").max().alias("valid_to"),
            pl.col("name").last(),
            pl.col("type").last(),
            pl.col("primary_exchange").last(),
            pl.col("composite_figi").last(),
            pl.col("share_class_figi").last(),
        )
        .with_columns((pl.col("valid_to") == last_session).alias("listed"), pl.lit(as_of).alias("date"))
        .drop("run")
        .sort("security_id", "valid_from")
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
        history = symbol_history(self.listings.scan(start, context.date), context.date).collect()
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
            frame = symbol_history(self.listings.scan(self.listings.first_date or as_of, as_of), as_of)
        return frame.filter(pl.col("security_id").is_in(security_ids)) if security_ids is not None else frame

    def attach_security_id(self, frame: Any, start: date, end: date, date_col: str = "date", ticker_col: str = "ticker") -> Any:
        """Join ticker-keyed rows to the listings of their own date, adding ``security_id`` and FIGIs."""
        import polars as pl

        listings = self.listings.scan(start, end).select(
            pl.col("date").alias(date_col), pl.col("ticker").alias(ticker_col), "security_id", "composite_figi", "share_class_figi"
        )
        return frame.join(listings, on=[date_col, ticker_col], how="left")
