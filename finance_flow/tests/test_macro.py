from datetime import date

import polars as pl

from finance_flow.macro import FRED_OBSERVATIONS_PATH, load_fred


def test_load_fred_returns_values_as_published_on_a_date(tmp_path):
    path = tmp_path / FRED_OBSERVATIONS_PATH
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "series_id": ["PAYEMS", "PAYEMS", "PAYEMS", "UNRATE"],
            "date": [date(2026, 9, 1), date(2026, 9, 1), date(2026, 10, 1), date(2026, 9, 1)],
            "value": [100.0, 101.0, 102.0, 4.1],
            "valid_from": [date(2026, 10, 3), date(2026, 11, 7), date(2026, 11, 7), date(2026, 10, 3)],
            "valid_to": [date(2026, 11, 6), None, None, None],
        }
    ).write_parquet(path)

    def values(**kwargs):
        return load_fred(tmp_path, **kwargs).collect().select("series_id", "date", "value").rows()

    # Before the November release: September's first print, and no October value yet.
    assert values(series=["PAYEMS"], as_of=date(2026, 11, 6)) == [("PAYEMS", date(2026, 9, 1), 100.0)]
    # On the release day: September revised and October published.
    assert values(series=["PAYEMS"], as_of=date(2026, 11, 7)) == [("PAYEMS", date(2026, 9, 1), 101.0), ("PAYEMS", date(2026, 10, 1), 102.0)]
    assert values() == [("PAYEMS", date(2026, 9, 1), 101.0), ("PAYEMS", date(2026, 10, 1), 102.0), ("UNRATE", date(2026, 9, 1), 4.1)]
    assert values(as_of=date(2026, 10, 2)) == []
    # Any fsspec URL reads the same table.
    assert load_fred(f"file://{tmp_path}", ["UNRATE"]).collect()["value"].to_list() == [4.1]
