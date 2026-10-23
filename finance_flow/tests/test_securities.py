from __future__ import annotations

import gzip
import json
from datetime import date

import polars as pl
from ccflow_etl import DatasetDefinition, LocalFileOutput

from finance_flow.securities import SecurityListingsModel, figi_fill_status, name_tokens

DAY = date(2025, 1, 6)
SNAPSHOT = [
    {"ticker": "AAPL", "name": "Apple Inc.", "type": "CS", "composite_figi": "BBG000B9XRY4", "share_class_figi": "BBG001S5N8V8"},
    {"ticker": "AAC.WS", "name": "Ares Acquisition Corporation III Redeemable warrants", "type": "WARRANT"},
    {"ticker": "ALPXW", "name": "Alpex Acquisition Corporation Warrants", "type": "WARRANT"},
    {"ticker": "AACI", "name": "Armada Acquisition Corp. III Class A Ordinary Share", "type": "CS"},
    {"ticker": "ABRpD", "name": "Arbor Realty Trust 6.375% Series D Preferred", "type": "PFD"},
    {"ticker": "NEWW", "name": "Brand New Corp", "type": "CS"},
]
FILLS = [
    {
        "record": SNAPSHOT[1],
        "status": "matched",
        "matches": [
            {"compositeFIGI": "BBG01AAAAAA1", "shareClassFIGI": "BBG01AAAAAB1", "name": "ARES ACQUISITION CORP III", "securityType2": "Warrant"}
        ],
    },
    {"record": SNAPSHOT[2], "status": "matched", "matches": [{"compositeFIGI": "BBG01BBBBBB2", "name": "ALOPEXX INC", "securityType2": "Warrant"}]},
    {
        "record": SNAPSHOT[3],
        "status": "matched",
        "matches": [{"compositeFIGI": "BBG01CCCCCC3", "name": "ARMADA ACQUISITION CORP III", "securityType2": "Warrant"}],
    },
    {"record": SNAPSHOT[4], "status": "skipped", "matches": []},
]


def test_name_tokens_drop_generic_words_and_punctuation():
    assert name_tokens("Alussa Energy Acquisition Corp. II Redeemable Warrants") == ["ALUSSA", "ENERGY", "ACQUISITION", "II"]
    assert name_tokens("BLUE WATER ACQUISITION - 30") == ["BLUE", "WATER", "ACQUISITION"]
    assert name_tokens(None) == []


def test_figi_fill_status_checks_type_and_name():
    warrant = {"type": "WARRANT", "name": "Ares Acquisition Corporation III Redeemable warrants"}
    assert figi_fill_status(warrant, {"name": "ARES ACQUISITION CORP III", "securityType2": "Warrant"}) == "matched"
    assert figi_fill_status(warrant, {"name": "ARES ACQUISITION CORP III", "securityType2": "Common Stock"}) == "rejected_type"
    assert figi_fill_status({"type": "CS", "name": "Ares Acquisition"}, {"name": "ARES ACQUISITION", "securityType2": "Unit"}) == "rejected_type"
    assert (
        figi_fill_status({"type": "WARRANT", "name": "Alpex Acquisition Corporation Warrants"}, {"name": "ALOPEXX INC", "securityType2": "Warrant"})
        == "rejected_name"
    )
    assert figi_fill_status({"type": "CS", "name": "Brand New Corp"}, {"name": "BRAND NEW CORPORATION", "securityType2": "Common Stock"}) == "matched"
    assert (
        figi_fill_status({"type": "ADRC", "name": "ASML Holding NV"}, {"name": "ASML HOLDING NV-NY REG SHS", "securityType2": "Depositary Receipt"})
        == "matched"
    )
    assert (
        figi_fill_status({"type": "CS", "name": "OneIM Acquisition Corp. Units"}, {"name": "ONEIM ACQUISITION CORP", "securityType2": "Unit"})
        == "matched"
    )
    assert (
        figi_fill_status({"type": "WARRANT", "name": "Robo.ai Inc. Warrant"}, {"name": "NWTN INC -CW27", "securityType2": "Warrant"})
        == "rejected_name"
    )


def _datasets(tmp_path):
    store = LocalFileOutput(path=tmp_path / "store")
    snapshot = DatasetDefinition(name="all-tickers", key_template="raw/all-tickers/{date}.json", store=store, format="json")
    fill = DatasetDefinition(name="openfigi", key_template="raw/openfigi/{date}.jsonl.gz", store=store, format="jsonl.gz")
    output = DatasetDefinition(
        name="listings", key_template="curated/listings/{date}.parquet", store=store, schema_name="listing", schema_version="1"
    )
    snapshot.write_bytes(DAY, json.dumps({"results": SNAPSHOT}).encode())
    fill.write_bytes(DAY, gzip.compress(b"".join(json.dumps(row).encode() + b"\n" for row in FILLS)))
    return snapshot, fill, output


def test_security_listings_keep_massive_figis_and_fill_validated_matches(tmp_path):
    snapshot, fill, output = _datasets(tmp_path)
    model = SecurityListingsModel(snapshot=snapshot, fills={"openfigi": fill}, output=output)

    result = model(DAY).value
    frame = output.read_frame(DAY)
    rows = {row["ticker"]: row for row in frame.to_dicts()}
    sidecar = json.loads((tmp_path / "store" / output.sidecar_key(DAY)).read_text())

    assert result["status"] == "written"
    assert rows["AAPL"]["security_id"] == "BBG000B9XRY4" and rows["AAPL"]["figi_source"] == "massive"
    assert rows["AAC.WS"]["security_id"] == "BBG01AAAAAA1" and rows["AAC.WS"]["share_class_figi"] == "BBG01AAAAAB1"
    assert rows["AAC.WS"]["figi_source"] == "openfigi" and rows["AAC.WS"]["massive_composite_figi"] is None
    assert (rows["ALPXW"]["security_id"], rows["ALPXW"]["fill_status"]) == (None, "rejected_name")
    assert (rows["AACI"]["security_id"], rows["AACI"]["fill_status"]) == (None, "rejected_type")
    assert (rows["ABRpD"]["security_id"], rows["ABRpD"]["fill_status"]) == (None, "skipped")
    assert (rows["NEWW"]["security_id"], rows["NEWW"]["fill_status"]) == (None, "no_fill")
    assert frame["date"].unique().to_list() == [DAY]
    assert sidecar["quality"]["missing_security_id_count"] == 4
    assert sidecar["quality"]["figi_source_counts"] == {"massive": 1, "openfigi": 1, "None": 4}
    assert sidecar["sources"]["openfigi"].endswith("raw/openfigi/2025-01-06.jsonl.gz")


def test_security_listings_explain_exists_and_missing_snapshot(tmp_path):
    snapshot, fill, output = _datasets(tmp_path)

    assert SecurityListingsModel(snapshot=snapshot, fills={"openfigi": fill}, output=output, explain=True)(DAY).value["status"] == "planned"
    model = SecurityListingsModel(snapshot=snapshot, fills={"openfigi": fill}, output=output)
    model(DAY)
    assert model(DAY).value["status"] == "exists"
    assert model(date(2025, 1, 7)).value["status"] == "no_snapshot"
    assert isinstance(output.scan(DAY, DAY).collect(), pl.DataFrame)


def _listings_dataset(tmp_path):
    from ccflow_etl import WeekdayCalendar

    dataset = DatasetDefinition(
        name="listings",
        key_template="curated/listings/{date}.parquet",
        store=LocalFileOutput(path=tmp_path / "store"),
        local_store=LocalFileOutput(path=tmp_path / "local"),
        calendar=WeekdayCalendar(),
        first_date=date(2025, 1, 6),
    )
    days = {
        date(2025, 1, 6): [("OLD", "FIGI1"), ("STAY", "FIGI2"), ("GONE", "FIGI3"), ("NOFIGI.WS", None)],
        date(2025, 1, 7): [("NEW", "FIGI1"), ("GONE", "FIGI3")],
        date(2025, 1, 8): [("NEW", "FIGI1"), ("STAY", "FIGI2")],
    }
    for day, rows in days.items():
        frame = pl.DataFrame(
            {
                "ticker": [ticker for ticker, _ in rows],
                "name": [f"{ticker} Inc" for ticker, _ in rows],
                "type": ["CS"] * len(rows),
                "primary_exchange": ["XNYS"] * len(rows),
                "composite_figi": [figi for _, figi in rows],
                "share_class_figi": [figi for _, figi in rows],
                "security_id": [figi for _, figi in rows],
                "date": [day] * len(rows),
            }
        )
        dataset.write_frame(day, frame)
    return dataset


def test_symbol_history_splits_runs_on_ticker_changes_and_gaps(tmp_path):
    from finance_flow.securities import symbol_history

    listings = _listings_dataset(tmp_path)

    history = symbol_history(listings.scan(date(2025, 1, 6), date(2025, 1, 8)), date(2025, 1, 8)).collect()

    assert history.select("security_id", "ticker", "valid_from", "valid_to", "listed").rows() == [
        ("FIGI1", "OLD", date(2025, 1, 6), date(2025, 1, 6), False),
        ("FIGI1", "NEW", date(2025, 1, 7), date(2025, 1, 8), True),
        ("FIGI2", "STAY", date(2025, 1, 6), date(2025, 1, 6), False),
        ("FIGI2", "STAY", date(2025, 1, 8), date(2025, 1, 8), True),
        ("FIGI3", "GONE", date(2025, 1, 6), date(2025, 1, 7), False),
    ]
    with_attributes = symbol_history(listings.scan(date(2025, 1, 6), date(2025, 1, 8)), date(2025, 1, 8), attributes=listings.read_frame).collect()
    assert with_attributes.select("ticker", "name").rows()[:2] == [("OLD", "OLD Inc"), ("NEW", "NEW Inc")]
    as_of_first = symbol_history(listings.scan(date(2025, 1, 6), date(2025, 1, 8)), date(2025, 1, 6)).collect()
    assert as_of_first.select("ticker", "listed").sort("ticker").rows() == [("GONE", True), ("OLD", True), ("STAY", True)]


def test_symbol_history_model_writes_history_and_skips_existing(tmp_path):
    from finance_flow.securities import SymbolHistoryModel

    listings = _listings_dataset(tmp_path)
    output = DatasetDefinition(
        name="symbol-history", key_template="curated/symbol-history/{date}.parquet", store=LocalFileOutput(path=tmp_path / "store")
    )
    model = SymbolHistoryModel(listings=listings, output=output)

    result = model(date(2025, 1, 8)).value

    assert (result["status"], result["row_count"], result["security_count"], result["multi_ticker_security_count"]) == ("written", 5, 3, 2)
    assert output.read_frame(date(2025, 1, 8)).height == 5
    assert model(date(2025, 1, 8)).value["status"] == "exists"
    assert model(date(2025, 1, 9)).value["status"] == "no_listings"
    assert SymbolHistoryModel(listings=listings, output=output, explain=True)(date(2025, 1, 9)).value["status"] == "planned"


def test_security_master_lookups_return_point_in_time_frames(tmp_path):
    from finance_flow.securities import SecurityMaster, SymbolHistoryModel

    listings = _listings_dataset(tmp_path)
    history = DatasetDefinition(
        name="symbol-history", key_template="curated/symbol-history/{date}.parquet", store=LocalFileOutput(path=tmp_path / "store")
    )
    SymbolHistoryModel(listings=listings, output=history)(date(2025, 1, 8))
    master = SecurityMaster(listings=listings, symbol_history=history)

    assert master.lookup(["OLD", "NEW"], date(2025, 1, 6)).collect()["security_id"].to_list() == ["FIGI1"]
    assert master.lookup(["OLD", "NEW"], date(2025, 1, 8)).collect().select("ticker", "security_id").rows() == [("NEW", "FIGI1")]
    assert master.history(["FIGI1"], date(2025, 1, 9)).collect()["ticker"].to_list() == ["OLD", "NEW"]
    computed = SecurityMaster(listings=listings, symbol_history=history.model_copy(update={"key_template": "elsewhere/{date}.parquet"}))
    assert computed.history(["FIGI1"], date(2025, 1, 7)).collect().select("ticker", "listed").rows() == [("OLD", False), ("NEW", True)]
    bars = pl.LazyFrame(
        {"date": [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 7)], "ticker": ["OLD", "NEW", "MISSING"], "close": [1.0, 2.0, 3.0]}
    )
    joined = master.attach_security_id(bars, date(2025, 1, 6), date(2025, 1, 7)).collect().sort("date", "ticker")
    assert joined.select("ticker", "security_id").rows() == [("OLD", "FIGI1"), ("MISSING", None), ("NEW", "FIGI1")]


def test_security_listings_apply_manual_figis_within_their_windows(tmp_path):
    snapshot, fill, output = _datasets(tmp_path)
    manual = tmp_path / "manual_figis.csv"
    manual.write_text(
        "ticker,valid_from,valid_to,composite_figi,share_class_figi,source\n"
        "ABRpD,2025-01-01,2025-01-31,BBG00PREF001,,taq_cusip\n"
        "NEWW,2024-01-01,2024-12-31,BBG00OLDNEW1,,taq_cusip\n"
        "ALPXW,2025-01-01,2025-01-31,BBG00ALPX001,BBG00ALPX002,openfigi_search\n"
    )
    model = SecurityListingsModel(snapshot=snapshot, fills={"openfigi": fill}, output=output, manual_figis=str(manual))

    model(DAY)
    rows = {row["ticker"]: row for row in output.read_frame(DAY).to_dicts()}

    assert (rows["ABRpD"]["security_id"], rows["ABRpD"]["figi_source"], rows["ABRpD"]["fill_status"]) == ("BBG00PREF001", "taq_cusip", "manual")
    assert rows["ABRpD"]["share_class_figi"] is None
    assert rows["NEWW"]["security_id"] is None
    assert (rows["ALPXW"]["security_id"], rows["ALPXW"]["share_class_figi"]) == ("BBG00ALPX001", "BBG00ALPX002")
    assert rows["AAC.WS"]["figi_source"] == "openfigi"


def test_listings_resolution_check_flags_new_unresolved_listings_after_grace(tmp_path):
    import pytest

    from finance_flow.securities import ListingsResolutionCheckModel

    listings = _listings_dataset(tmp_path)
    # A new unresolved listing appears on the last two sessions; NOFIGI.WS is backlog from before the baseline.
    for day in (date(2025, 1, 7), date(2025, 1, 8)):
        frame = listings.read_frame(day)
        extra = pl.DataFrame(
            {
                "ticker": ["FRESH"],
                "name": ["Fresh Co"],
                "type": ["CS"],
                "primary_exchange": ["XNYS"],
                "composite_figi": [None],
                "share_class_figi": [None],
                "security_id": [None],
                "date": [day],
            }
        )
        listings.write_frame(
            day, pl.concat([frame, extra], how="diagonal_relaxed").with_columns(pl.lit(None, pl.String).alias("fill_status")), overwrite=True
        )
    check = ListingsResolutionCheckModel(listings=listings, baseline_date=date(2025, 1, 7), grace_sessions=2)

    result = check(date(2025, 1, 8)).value

    assert (result["unresolved_count"], result["status"]) == (1, "failed")
    assert [row["ticker"] for row in result["overdue"]] == ["FRESH"]
    assert check.model_copy(update={"grace_sessions": 3})(date(2025, 1, 8)).value["status"] == "ok"
    assert check.model_copy(update={"grace_sessions_by_type": {"CS": 5}})(date(2025, 1, 8)).value["status"] == "ok"
    assert check(date(2025, 1, 11)).value["date"] == "2025-01-08"
    assert check.model_copy(update={"explain": True})(date(2025, 1, 8)).value["status"] == "planned"
    assert check.model_copy(update={"ignore_types": ["CS"]})(date(2025, 1, 8)).value["unresolved_count"] == 0
    backlog = ListingsResolutionCheckModel(listings=listings, baseline_date=date(2025, 1, 7), max_unresolved=0)
    assert backlog(date(2025, 1, 6)).value["unresolved_count"] == 1
    with pytest.raises(RuntimeError, match="FRESH"):
        check.model_copy(update={"fail_on_unresolved": True})(date(2025, 1, 8))
    with pytest.raises(RuntimeError, match="exceed 0"):
        backlog.model_copy(update={"fail_on_unresolved": True})(date(2025, 1, 6))


def test_security_listings_accept_identifier_fills_without_name_validation(tmp_path):
    snapshot, fill, output = _datasets(tmp_path)
    cusip = DatasetDefinition(
        name="cusip", key_template="raw/cusip/{date}.jsonl.gz", store=LocalFileOutput(path=tmp_path / "store"), format="jsonl.gz"
    )
    rows = [
        {
            "record": SNAPSHOT[4],
            "status": "matched",
            "matches": [{"compositeFIGI": "BBG00PREF001", "name": "ABR 6.375 PERP D", "securityType2": "Preferred Stock"}],
        }
    ]
    cusip.write_bytes(DAY, gzip.compress(b"".join(json.dumps(row).encode() + b"\n" for row in rows)))

    SecurityListingsModel(snapshot=snapshot, fills={"openfigi": fill, "taq_cusip": cusip}, output=output, identifier_fills=["taq_cusip"])(DAY)
    validated = output.model_copy(update={"key_template": "curated/validated/{date}.parquet"})
    SecurityListingsModel(snapshot=snapshot, fills={"openfigi": fill, "taq_cusip": cusip}, output=validated)(DAY)

    accepted = {row["ticker"]: row for row in output.read_frame(DAY).to_dicts()}["ABRpD"]
    rejected = {row["ticker"]: row for row in validated.read_frame(DAY).to_dicts()}["ABRpD"]
    assert (accepted["security_id"], accepted["figi_source"]) == ("BBG00PREF001", "taq_cusip")
    assert (rejected["security_id"], rejected["fill_status"]) == (None, "rejected_name")
