from datetime import date
from typing import Any, Dict, List, Optional, Type, Union

from ccflow import CallableModel, ContextBase, ContextType, Flow, GenericResult, ResultType
from pydantic import BaseModel, field_serializer

__all__ = (
    "DailyBar",
    "MassiveDailyBarsNormalizeContext",
    "MassiveDailyBarsNormalizeModel",
    "normalize_massive_daily_bars",
)


class DailyBar(BaseModel):
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None
    transactions: Optional[int] = None

    @field_serializer("date")
    def serialize_date(self, value: date) -> str:
        return value.isoformat()


class MassiveDailyBarsNormalizeContext(ContextBase):
    payload: Union[Dict[str, Any], List[Dict[str, Any]]]
    ticker: str
    session_date: date


class MassiveDailyBarsNormalizeModel(CallableModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return MassiveDailyBarsNormalizeContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: MassiveDailyBarsNormalizeContext) -> GenericResult:
        return GenericResult(value=normalize_massive_daily_bars(context.payload, ticker=context.ticker, session_date=context.session_date))


def _items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get("results", payload.get("rows", []))
        return list(value or [])
    return list(payload or [])


def _value(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def _float(row: Dict[str, Any], *names: str) -> float:
    value = _value(row, *names)
    if value is None:
        raise ValueError(f"Missing numeric field; expected one of {names}.")
    return float(value)


def _int(row: Dict[str, Any], *names: str) -> int:
    value = _value(row, *names)
    if value is None:
        raise ValueError(f"Missing integer field; expected one of {names}.")
    return int(value)


def _optional_float(row: Dict[str, Any], *names: str) -> Optional[float]:
    value = _value(row, *names)
    return None if value is None else float(value)


def _optional_int(row: Dict[str, Any], *names: str) -> Optional[int]:
    value = _value(row, *names)
    return None if value is None else int(value)


def normalize_massive_daily_bars(payload: Union[Dict[str, Any], List[Dict[str, Any]]], ticker: str, session_date: Union[str, date]) -> List[DailyBar]:
    business_date = date.fromisoformat(session_date) if isinstance(session_date, str) else session_date
    bars = []
    for row in _items(payload):
        bars.append(
            DailyBar(
                ticker=str(_value(row, "ticker", "T") or ticker),
                date=_value(row, "date") or business_date,
                open=_float(row, "open", "o"),
                high=_float(row, "high", "h"),
                low=_float(row, "low", "l"),
                close=_float(row, "close", "c"),
                volume=_int(row, "volume", "v"),
                vwap=_optional_float(row, "vwap", "vw"),
                transactions=_optional_int(row, "transactions", "n"),
            )
        )
    return bars
