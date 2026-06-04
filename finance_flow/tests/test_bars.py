from finance_flow import *  # noqa
from finance_flow import DailyBar, MassiveDailyBarsNormalizeContext, MassiveDailyBarsNormalizeModel, normalize_massive_daily_bars
import pytest


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
    with pytest.raises(ValueError, match="Missing numeric field"):
        normalize_massive_daily_bars({"results": [{"T": "AAA", "o": 102.5}]}, ticker="AAA", session_date="2024-01-03")
