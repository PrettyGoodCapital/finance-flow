import gzip
import json
import re
from datetime import date, timedelta
from typing import Any

from ccflow import CallableModel, DateContext, Flow, GenericResult
from ccflow_etl import DatasetDefinition
from pydantic import Field

from .lineage import transform_provenance

__all__ = (
    "RIC_EXCHANGE_SUFFIXES",
    "SecurityIdentifiersModel",
    "derive_bloomberg_ticker",
    "derive_ric",
    "share_class",
)

# Primary-listing MIC to the RIC exchange suffix of US equities.
RIC_EXCHANGE_SUFFIXES = {
    "XNYS": ".N",
    "XNAS": ".O",
    "XASE": ".A",
    "ARCX": ".P",
    "BATS": ".Z",
}

_PREFERRED = re.compile(r"^(?P<root>[A-Z]+)p(?P<series>[A-Z]?)$")
# Suffixes after a dot in a listing ticker, mapped to RIC instrument codes.
_RIC_SUFFIXES = {"WS": "_t", "U": "_u", "RT": "_r", "W": "_t"}
_CLASS_NAME = re.compile(r"\bClass ([A-Z])\b", re.IGNORECASE)


def derive_ric(ticker: str | None, mic: str | None) -> str | None:
    """Build the conventional US equity RIC for a listing ticker on its primary exchange.

    Class shares take a lowercase class letter (``BRK.B`` on NYSE is ``BRKb.N``), preferreds take ``_p`` and the
    series letter (``ABRpD`` is ``ABR_pd.N``), and warrants, units, and rights take ``_t``, ``_u``, and ``_r``. Returns
    None for exchanges without a known suffix. The result follows the published convention, not LSEG's own symbology,
    so unusual instruments can differ.
    """
    suffix = RIC_EXCHANGE_SUFFIXES.get(mic or "")
    if not ticker or suffix is None:
        return None
    preferred = _PREFERRED.match(ticker)
    if preferred:
        return f"{preferred['root']}_p{preferred['series'].lower()}{suffix}"
    if "." in ticker:
        root, _, rest = ticker.partition(".")
        parts = rest.split(".")
        code = _RIC_SUFFIXES.get(parts[0])
        if code is not None:
            return f"{root}{code}{''.join(part.lower() for part in parts[1:])}{suffix}"
        if len(rest) == 1:
            return f"{root}{rest.lower()}{suffix}"
        return None
    return f"{ticker}{suffix}"


def derive_bloomberg_ticker(ticker: str | None, exchange_code: str = "US", market_sector: str = "Equity") -> str | None:
    """Build a Bloomberg composite ticker such as ``BRK/B US Equity`` from a listing ticker.

    Class separators become ``/`` and preferreds take ``/P`` and the series letter. Prefer the ticker OpenFIGI reports
    for the security's FIGI where it is known; this covers dates before a ticker change.
    """
    if not ticker:
        return None
    preferred = _PREFERRED.match(ticker)
    base = f"{preferred['root']}/P{preferred['series']}" if preferred else ticker.replace(".", "/")
    return f"{base} {exchange_code} {market_sector}"


def share_class(ticker: str | None, name: str | None) -> str | None:
    """The share class letter from a listing's name (``... Class A Common Stock``) or its ticker (``BRK.B``)."""
    match = _CLASS_NAME.search(name or "")
    if match:
        return match.group(1).upper()
    if ticker and "." in ticker:
        rest = ticker.partition(".")[2]
        if len(rest) == 1 and rest.isalpha():
            return rest.upper()
    return None


_IDENTIFIER_SCHEMA = {
    "date": "date",
    "ticker": "str",
    "security_id": "str",
    "composite_figi": "str",
    "share_class_figi": "str",
    "name": "str",
    "type": "str",
    "mic": "str",
    "share_class": "str",
    "ric": "str",
    "ric_source": "str",
    "bloomberg_ticker": "str",
    "openfigi_ticker": "str",
    "openfigi_exchange": "str",
    "openfigi_name": "str",
    "security_type": "str",
    "cusip": "str",
    "cusip_date": "date",
    "isin": "str",
    "cik": "str",
    "company_name": "str",
}
_CARRIED = ("openfigi_ticker", "openfigi_exchange", "openfigi_name", "security_type", "cusip", "cusip_date")
# Parsed OpenFIGI captures by partition URI, so a run over many sessions reads each capture once.
_CAPTURES: dict[str, dict[str, dict[str, Any]]] = {}


def _schema() -> dict:
    import polars as pl

    return {name: pl.Date if kind == "date" else pl.String for name, kind in _IDENTIFIER_SCHEMA.items()}


class SecurityIdentifiersModel(CallableModel):
    """Write every listing's identifiers for a session: RIC, Bloomberg ticker, CUSIP, ISIN, CIK, company name, and more.

    Sources, by column:

    - ``ric``: an override dataset where one covers the listing, otherwise derived from the ticker and primary exchange
      (``ric_source`` says which).
    - ``bloomberg_ticker``: OpenFIGI's ticker for the security's FIGI while the listing still trades under it, otherwise
      derived from the listing ticker, which covers dates before a ticker change.
    - ``cusip``: the latest symbol master on or before the session (such as the WRDS NYSE TAQ master); ``isin`` is computed for US CUSIPs.
    - ``company_name``: the SEC name of the listing's CIK that was in effect on the session.

    Each session is built from its own inputs, so sessions can be built in any order: OpenFIGI attributes come from
    every capture up to the session, and CUSIPs from the latest symbol master within ``cusip_lookback_days``.
    """

    listings: DatasetDefinition
    figi_attributes: DatasetDefinition = Field(description="OpenFIGI captures by FIGI: JSONL rows with figi, status, and matches.")
    cusips: DatasetDefinition | None = Field(default=None, description="Symbol master partitions with symbol and CUSIP columns.")
    company_names: DatasetDefinition | None = Field(default=None, description="SEC name windows by CIK, one current table (a key without a date).")
    ric_overrides: DatasetDefinition | None = Field(
        default=None, description="Licensed RICs by security_id with valid_from and valid_to, one current table (a key without a date)."
    )
    output: DatasetDefinition
    symbol_column: str = "CTA_SYMBOL"
    cusip_column: str = "CUSIP"
    cusip_lookback_days: int = Field(default=30, ge=1, description="How far back to look for a symbol master; masters arrive about weekly.")
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def _latest(self, dataset: DatasetDefinition, value: date, days: int) -> date | None:
        sessions = dataset.partitions(value - timedelta(days=days), value)
        return next((day for day in reversed(sessions) if dataset.exists(day)), None)

    def _capture(self, value: date) -> dict[str, dict[str, Any]]:
        uri = self.figi_attributes.uri(value)
        if uri not in _CAPTURES:
            attributes = {}
            for line in gzip.decompress(self.figi_attributes.read_bytes(value)).splitlines():
                row = json.loads(line)
                if row.get("matches"):
                    match = row["matches"][0]
                    attributes[row["figi"]] = {
                        "openfigi_ticker": match.get("ticker"),
                        "openfigi_exchange": match.get("exchCode"),
                        "openfigi_name": match.get("name"),
                        "security_type": match.get("securityType2") or match.get("securityType"),
                    }
            _CAPTURES[uri] = attributes
        return _CAPTURES[uri]

    def _figi_attributes(self, value: date) -> dict[str, dict[str, Any]]:
        """OpenFIGI attributes by FIGI from every capture up to ``value``; later captures win."""
        start = self.figi_attributes.first_date or self.listings.first_date or value
        local = self.figi_attributes.local_store
        attributes: dict[str, dict[str, Any]] = {}
        for day in self.figi_attributes.partitions(start, value):
            key = self.figi_attributes.key(day)
            cached = local is not None and hasattr(local, "file_path") and local.file_path(key).exists()
            if self.figi_attributes.uri(day) in _CAPTURES or cached or self.figi_attributes.exists(day):
                attributes.update(self._capture(day))
        return attributes

    def _company_names(self, value: date) -> Any:
        import polars as pl

        # Name windows carry their own dates, so one current table gives the name in effect on any session.
        if self.company_names is None or not self.company_names.exists(value):
            return None
        names = self.company_names.read_frame(value)
        in_effect = (pl.col("valid_from").is_null() | (pl.col("valid_from") <= value)) & (
            pl.col("valid_to").is_null() | (pl.col("valid_to") >= value)
        )
        return names.filter(in_effect).group_by("cik").agg(pl.col("name").last().alias("company_name"))

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        import polars as pl
        from finance_etl import isin_from_cusip

        value = context.date
        plan = {**self.output.plan(value), "listings": self.listings.name}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self.overwrite_output and self.output.exists(value):
            return GenericResult(value={**plan, "status": "exists"})
        if not self.listings.exists(value):
            return GenericResult(value={**plan, "status": "no_listings"})
        listings = self.listings.read_frame(value)
        attributes = self._figi_attributes(value)
        cusips, cusip_date = {}, None
        if self.cusips is not None:
            cusip_date = self._latest(self.cusips, value, self.cusip_lookback_days)
            if cusip_date is not None:
                master = self.cusips.read_frame(cusip_date).select(self.symbol_column, self.cusip_column).drop_nulls()
                cusips = dict(zip(master[self.symbol_column].to_list(), master[self.cusip_column].to_list(), strict=True))
        # Key CUSIPs by security through the master date's listings, so they follow a ticker change after that date.
        cusips_by_security = {}
        if cusips and cusip_date != value and self.listings.exists(cusip_date):
            then = self.listings.read_frame(cusip_date).select("ticker", "security_id").drop_nulls()
            cusips_by_security = {figi: cusips[ticker] for ticker, figi in then.iter_rows() if ticker in cusips}
        overrides = {}
        if self.ric_overrides is not None and self.ric_overrides.exists(value):
            frame = self.ric_overrides.read_frame(value).filter((pl.col("valid_from") <= value) & (pl.col("valid_to") >= value))
            overrides = dict(zip(frame["security_id"].to_list(), frame["ric"].to_list(), strict=True))
        rows = []
        for listing in listings.to_dicts():
            security_id = listing.get("security_id")
            row = dict(attributes.get(security_id, {})) if security_id else {}
            ticker, mic = listing.get("ticker"), listing.get("primary_exchange")
            cusip = cusips_by_security.get(security_id) if security_id else None
            cusip = cusip or cusips.get(ticker)
            if cusip:
                row["cusip"], row["cusip_date"] = cusip, cusip_date
            cusip = row.get("cusip")
            derived = derive_bloomberg_ticker(ticker)
            openfigi = row.get("openfigi_ticker")
            if openfigi and derived and derived.split(" ")[0] == openfigi:
                bloomberg = f"{openfigi} {row.get('openfigi_exchange') or 'US'} Equity"
            else:
                bloomberg = derived
            ric = overrides.get(security_id)
            cik = listing.get("cik")
            rows.append(
                {
                    **{name: row.get(name) for name in _CARRIED},
                    "date": value,
                    "ticker": ticker,
                    "security_id": security_id,
                    "composite_figi": listing.get("composite_figi"),
                    "share_class_figi": listing.get("share_class_figi"),
                    "name": listing.get("name"),
                    "type": listing.get("type"),
                    "mic": mic,
                    "share_class": share_class(ticker, listing.get("name")),
                    "ric": ric or derive_ric(ticker, mic),
                    "ric_source": "override" if ric else ("derived" if derive_ric(ticker, mic) else None),
                    "bloomberg_ticker": bloomberg,
                    "isin": isin_from_cusip(cusip) if cusip and cusip[0].isdigit() and len(cusip) == 9 else None,
                    "cik": f"{int(cik):010d}" if cik else None,
                }
            )
        frame = pl.DataFrame(rows, schema={name: dtype for name, dtype in _schema().items() if name != "company_name"})
        names = self._company_names(value)
        frame = frame.join(names, on="cik", how="left") if names is not None else frame.with_columns(pl.lit(None, pl.String).alias("company_name"))
        frame = frame.select(list(_IDENTIFIER_SCHEMA))
        quality = {
            "row_count": frame.height,
            **{
                f"{column}_count": frame.height - frame[column].null_count()
                for column in ("ric", "bloomberg_ticker", "cusip", "isin", "cik", "company_name", "openfigi_ticker")
            },
        }
        lineage = {
            "sources": {
                "listings": self.listings.uri(value),
                "figi_attributes": self.figi_attributes.name,
                "cusips": self.cusips.uri(cusip_date) if self.cusips is not None and cusip_date else None,
            },
            "quality": quality,
            "transform": transform_provenance(self),
        }
        writes = self.output.write_frame(value, frame, lineage=lineage, overwrite=self.overwrite_output)
        return GenericResult(value={**plan, "status": "written", **quality, "output_writes": writes})
