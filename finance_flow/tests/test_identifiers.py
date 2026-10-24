import pytest

from finance_flow.identifiers import derive_bloomberg_ticker, derive_ric, share_class


@pytest.mark.parametrize(
    ("ticker", "mic", "ric"),
    [
        ("AAPL", "XNAS", "AAPL.O"),
        ("IBM", "XNYS", "IBM.N"),
        ("BRK.B", "XNYS", "BRKb.N"),
        ("ABRpD", "XNYS", "ABR_pd.N"),
        ("XYZp", "XNYS", "XYZ_p.N"),
        ("AAC.WS", "XNYS", "AAC_t.N"),
        ("AAC.U", "XNYS", "AAC_u.N"),
        ("SPY", "ARCX", "SPY.P"),
        ("IMO", "XASE", "IMO.A"),
        ("AAPL", "OTCM", None),
        (None, "XNAS", None),
        ("ODD.ABC", "XNYS", None),
    ],
)
def test_derive_ric(ticker, mic, ric):
    assert derive_ric(ticker, mic) == ric


def test_derive_bloomberg_ticker():
    assert derive_bloomberg_ticker("META") == "META US Equity"
    assert derive_bloomberg_ticker("BRK.B") == "BRK/B US Equity"
    assert derive_bloomberg_ticker("ABRpD") == "ABR/PD US Equity"
    assert derive_bloomberg_ticker(None) is None


def test_share_class():
    assert share_class("GOOGL", "Alphabet Inc. Class A Common Stock") == "A"
    assert share_class("BRK.B", "Berkshire Hathaway Inc.") == "B"
    assert share_class("AAPL", "Apple Inc.") is None


def test_security_identifiers_follow_a_ticker_change(tmp_path):
    import gzip
    import json
    from datetime import date

    import polars as pl
    from ccflow_etl import DatasetDefinition, LocalFileOutput

    from finance_flow.identifiers import SecurityIdentifiersModel

    store = LocalFileOutput(path=tmp_path)
    listings = DatasetDefinition(name="listings", key_template="curated/listings/{date}.parquet", store=store)
    figis = DatasetDefinition(name="figis", key_template="raw/figi/{date}.jsonl.gz", store=store, format="jsonl.gz")
    cusips = DatasetDefinition(name="taq", key_template="raw/taq/{ymd}.parquet", store=store)
    names = DatasetDefinition(name="names", key_template="curated/names/latest.parquet", store=store)
    output = DatasetDefinition(name="identifiers", key_template="curated/identifiers/{date}.parquet", store=store)
    day1, day2 = date(2025, 1, 6), date(2025, 1, 7)

    def listing(ticker, figi, mic, name, cik):
        return {
            "ticker": ticker,
            "security_id": figi,
            "composite_figi": figi,
            "share_class_figi": None,
            "name": name,
            "type": "CS",
            "primary_exchange": mic,
            "cik": cik,
        }

    listings.write_frame(
        day1,
        pl.DataFrame(
            [listing("FB", "BBG2", "XNAS", "Facebook, Inc. Class A", "1326801"), listing("BRK.B", "BBG4", "XNYS", "Berkshire Hathaway", "1067983")]
        ),
    )
    listings.write_frame(
        day2,
        pl.DataFrame(
            [
                listing("META", "BBG2", "XNAS", "Meta Platforms, Inc. Class A", "1326801"),
                listing("BRK.B", "BBG4", "XNYS", "Berkshire Hathaway", "1067983"),
            ]
        ),
    )
    rows = [
        {
            "figi": "BBG2",
            "status": "matched",
            "matches": [{"ticker": "META", "exchCode": "US", "name": "META PLATFORMS INC-CLASS A", "securityType2": "Common Stock"}],
        },
        {
            "figi": "BBG4",
            "status": "matched",
            "matches": [{"ticker": "BRK/B", "exchCode": "US", "name": "BERKSHIRE HATHAWAY INC-CL B", "securityType2": "Common Stock"}],
        },
    ]
    figis.write_bytes(day1, gzip.compress(b"".join(json.dumps(row).encode() + b"\n" for row in rows)))
    cusips.write_frame(day1, pl.DataFrame({"CTA_SYMBOL": ["FB"], "CUSIP": ["30303M102"]}))
    names.write_frame(
        day1,
        pl.DataFrame(
            {
                "cik": ["0001326801", "0001326801"],
                "name": ["FACEBOOK INC", "Meta Platforms, Inc."],
                "valid_from": [None, day2],
                "valid_to": [day1, None],
            },
            schema_overrides={"valid_from": pl.Date, "valid_to": pl.Date},
        ),
    )
    model = SecurityIdentifiersModel(listings=listings, figi_attributes=figis, cusips=cusips, company_names=names, output=output)

    model(day1)
    model(day2)
    first = {row["ticker"]: row for row in output.read_frame(day1).to_dicts()}
    second = {row["ticker"]: row for row in output.read_frame(day2).to_dicts()}

    assert (first["FB"]["ric"], first["FB"]["bloomberg_ticker"], first["FB"]["company_name"]) == ("FB.O", "FB US Equity", "FACEBOOK INC")
    assert (first["FB"]["cusip"], first["FB"]["isin"][:11], first["FB"]["cik"]) == ("30303M102", "US30303M102", "0001326801")
    assert (second["META"]["ric"], second["META"]["bloomberg_ticker"], second["META"]["company_name"]) == (
        "META.O",
        "META US Equity",
        "Meta Platforms, Inc.",
    )
    assert (second["META"]["cusip"], second["META"]["cusip_date"], second["META"]["openfigi_name"]) == (
        "30303M102",
        day1,
        "META PLATFORMS INC-CLASS A",
    )
    assert (second["BRK.B"]["ric"], second["BRK.B"]["bloomberg_ticker"], second["BRK.B"]["share_class"], second["BRK.B"]["ric_source"]) == (
        "BRKb.N",
        "BRK/B US Equity",
        "B",
        "derived",
    )
    assert second["BRK.B"]["company_name"] is None
