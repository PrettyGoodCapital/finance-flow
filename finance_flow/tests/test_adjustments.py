from __future__ import annotations

import json
from datetime import date
from typing import Any

import polars as pl
import pytest
from ccflow_etl import DatasetDefinition, LocalFileOutput, WeekdayCalendar

from finance_flow.adjustments import AdjustedBars, AdjustmentFactorsModel

DAYS = [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8), date(2025, 1, 9)]


def _datasets(tmp_path):
    store = LocalFileOutput(path=tmp_path / "store")
    common: dict[str, Any] = {"store": store, "calendar": WeekdayCalendar(), "first_date": DAYS[0]}
    return {
        "splits": DatasetDefinition(name="splits", key_template="raw/splits/{date}.json", format="json", **common),
        "dividends": DatasetDefinition(name="dividends", key_template="raw/dividends/{date}.json", format="json", **common),
        "listings": DatasetDefinition(name="listings", key_template="curated/listings/{date}.parquet", **common),
        "bars": DatasetDefinition(name="bars", key_template="curated/bars/{date}.parquet", **common),
        "factors": DatasetDefinition(name="factors", key_template="curated/factors/{date}.parquet", **common),
    }


def _write_inputs(datasets):
    closes = {DAYS[0]: 100.0, DAYS[1]: 102.0, DAYS[2]: 51.0, DAYS[3]: 50.0}
    for day in DAYS:
        # AAA renames to AAB on the dividend day; the listings keep the same security ID.
        ticker = "AAB" if day == DAYS[3] else "AAA"
        datasets["listings"].write_frame(day, pl.DataFrame({"ticker": [ticker, "OTCX"], "security_id": ["FIGI1", None], "date": [day, day]}))
        datasets["bars"].write_frame(
            day,
            pl.DataFrame(
                {
                    "ticker": [ticker],
                    "date": [day],
                    "open": [closes[day]],
                    "high": [closes[day]],
                    "low": [closes[day]],
                    "close": [closes[day]],
                    "vwap": [None],
                    "volume": [1000.0],
                }
            ),
        )
        datasets["splits"].write_bytes(day, json.dumps({"results": []}).encode())
        datasets["dividends"].write_bytes(day, json.dumps({"results": []}).encode())
    datasets["splits"].write_bytes(
        DAYS[2],
        json.dumps(
            {
                "results": [
                    {
                        "id": "S1",
                        "ticker": "AAA",
                        "split_from": 1,
                        "split_to": 2,
                        "adjustment_type": "forward_split",
                        "historical_adjustment_factor": 0.5,
                    }
                ]
            }
        ).encode(),
        overwrite=True,
    )
    datasets["dividends"].write_bytes(
        DAYS[3],
        json.dumps(
            {
                "results": [
                    {"id": "D1", "ticker": "AAB", "cash_amount": 1.02, "currency": "USD", "distribution_type": "special"},
                    {"id": "D2", "ticker": "OTCX", "cash_amount": 0.5, "currency": "CAD"},
                ]
            }
        ).encode(),
        overwrite=True,
    )


def test_adjustment_factors_compute_split_and_dividend_factors(tmp_path):
    datasets = _datasets(tmp_path)
    _write_inputs(datasets)
    model = AdjustmentFactorsModel(**{name: datasets[name] for name in ("splits", "dividends", "listings", "bars")}, output=datasets["factors"])

    for day in DAYS:
        model(day)
    split = datasets["factors"].read_frame(DAYS[2]).to_dicts()
    dividends = {row["event_id"]: row for row in datasets["factors"].read_frame(DAYS[3]).to_dicts()}

    assert datasets["factors"].read_frame(DAYS[0]).height == 0
    assert (split[0]["security_id"], split[0]["split_factor"], split[0]["status"]) == ("FIGI1", 0.5, "ok")
    assert dividends["D1"]["security_id"] == "FIGI1"
    assert dividends["D1"]["previous_close"] == 51.0 and dividends["D1"]["previous_date"] == DAYS[2]
    assert dividends["D1"]["dividend_factor"] == pytest.approx(1 - 1.02 / 51.0)
    assert (dividends["D2"]["dividend_factor"], dividends["D2"]["status"]) == (None, "non_usd")
    assert model(DAYS[3]).value["status"] == "exists"


def test_adjusted_bars_apply_later_factors_by_security(tmp_path):
    datasets = _datasets(tmp_path)
    _write_inputs(datasets)
    model = AdjustmentFactorsModel(**{name: datasets[name] for name in ("splits", "dividends", "listings", "bars")}, output=datasets["factors"])
    for day in DAYS:
        model(day)
    adjusted = AdjustedBars(bars=datasets["bars"], factors=datasets["factors"], listings=datasets["listings"])
    dividend_factor = 1 - 1.02 / 51.0

    total = adjusted.scan(DAYS[0], DAYS[3], as_of=DAYS[3]).collect().sort("date")
    split_only = adjusted.scan(DAYS[0], DAYS[3], mode="split", as_of=DAYS[3]).collect().sort("date")
    before_dividend = adjusted.scan(DAYS[0], DAYS[3], as_of=DAYS[2]).collect().sort("date")

    assert total["close"].to_list() == pytest.approx([100.0 * 0.5 * dividend_factor, 102.0 * 0.5 * dividend_factor, 51.0 * dividend_factor, 50.0])
    assert total["volume"].to_list() == [2000.0, 2000.0, 1000.0, 1000.0]
    assert split_only["close"].to_list() == pytest.approx([50.0, 51.0, 51.0, 50.0])
    assert before_dividend["close"].to_list() == pytest.approx([50.0, 51.0, 51.0, 50.0])
    assert total["security_id"].to_list() == ["FIGI1"] * 4
