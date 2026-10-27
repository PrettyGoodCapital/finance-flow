from datetime import date
from pathlib import Path
from typing import Any

__all__ = (
    "FRED_OBSERVATIONS_PATH",
    "fred_as_of",
    "load_fred",
)

FRED_OBSERVATIONS_PATH = "curated/fred/observations/v1/latest.parquet"


def fred_as_of(observations: Any, as_of: date | None = None, series: list[str] | None = None) -> Any:
    """Each series' value for every observation date as published on ``as_of``, from a FRED vintage table.

    ``observations`` has ``series_id``, ``date``, ``value``, ``valid_from``, and ``valid_to`` (null while current). A
    value published on ``as_of`` counts, so use the session date to get what was known by its close. With ``as_of``
    omitted, returns current values. Observation dates whose first release came after ``as_of`` are absent.
    """
    import polars as pl

    frame = observations.lazy() if hasattr(observations, "lazy") else observations
    if series is not None:
        frame = frame.filter(pl.col("series_id").is_in(series))
    if as_of is None:
        known = frame.filter(pl.col("valid_to").is_null())
    else:
        known = frame.filter((pl.col("valid_from") <= as_of) & (pl.col("valid_to").is_null() | (pl.col("valid_to") >= as_of)))
    return known.select("series_id", "date", "value", pl.col("valid_from").alias("published")).sort("series_id", "date")


def load_fred(root: str | Path, series: list[str] | None = None, as_of: date | None = None) -> Any:
    """Load FRED observations as published on ``as_of`` from the curated vintage table under ``root``, as a LazyFrame.

    For example ``load_fred("/var/lib/airflow/data", ["PAYEMS", "UNRATE"], as_of=date(2024, 6, 3))`` gives payrolls and
    unemployment as they stood on that day, before later revisions.
    """
    import polars as pl

    return fred_as_of(pl.scan_parquet(Path(root) / FRED_OBSERVATIONS_PATH), as_of=as_of, series=series)
