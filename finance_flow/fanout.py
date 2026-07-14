import json
import posixpath
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Type

from ccflow import CallableModel, ContextType, Flow, GenericResult, ResultType
from finance_etl import SymbolUniverseResult
from pydantic import Field, PrivateAttr

__all__ = ("SymbolFanoutModel",)


class SymbolFanoutModel(CallableModel):
    universe_model: CallableModel
    model: CallableModel
    symbol_field: str = "symbol"
    date_field: str = "date"
    context_values: Dict[str, Any] = Field(default_factory=dict)
    max_symbols: Optional[int] = Field(default=None, ge=1)
    include_outputs: bool = True
    skip_existing: bool = False
    explain: bool = False

    _contexts_by_parent: Dict[str, List[ContextType]] = PrivateAttr(default_factory=dict)
    _inventory_by_parent: Dict[str, Dict[str, int]] = PrivateAttr(default_factory=dict)

    @property
    def context_type(self) -> Type[ContextType]:
        return self.universe_model.context_type

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    def _symbols(self, context: ContextType) -> List[str]:
        result = self.universe_model(context=context)
        value = result.value if isinstance(result, GenericResult) else result
        if isinstance(value, SymbolUniverseResult):
            symbols = value.symbols
        elif isinstance(value, dict):
            symbols = value.get("symbols", [])
        else:
            symbols = getattr(value, "symbols", value)
        symbols = sorted({str(symbol).strip().upper() for symbol in symbols or [] if str(symbol).strip()})
        return symbols[: self.max_symbols] if self.max_symbols is not None else symbols

    def _context_date(self, context: ContextType) -> Optional[date]:
        for field in ("date", "as_of_date", "session_date"):
            value = getattr(context, field, None)
            if value is not None:
                return value
        return None

    def _child_contexts(self, context: ContextType) -> List[ContextType]:
        base = context.model_dump(mode="python")
        base.pop("type_", None)
        context_date = self._context_date(context)
        contexts = []
        for symbol in self._symbols(context):
            values = {**base, **self.context_values, self.symbol_field: symbol}
            if context_date is not None:
                values[self.date_field] = context_date
            contexts.append(self.model.context_type.model_validate(values))
        return contexts

    def _context_key(self, context: ContextType) -> str:
        return json.dumps(context.model_dump(mode="json"), sort_keys=True)

    def _missing_contexts(self, contexts: List[ContextType]) -> List[ContextType]:
        if not self.skip_existing or self.explain:
            return contexts
        output = getattr(self.model, "output", None)
        output_key = getattr(self.model, "output_key", None)
        if output is None or not hasattr(output, "list_keys") or not callable(output_key):
            raise ValueError("skip_existing requires a child model with output_key(context) and an output store with list_keys(prefix).")
        keys = [output_key(context) for context in contexts]
        if not keys:
            return []
        prefix = posixpath.commonpath(keys)
        existing = set(output.list_keys(prefix))
        return [context for context, key in zip(contexts, keys) if key not in existing]

    @Flow.deps
    def __deps__(self, context: ContextType) -> List[Tuple[CallableModel, List[ContextType]]]:
        context_key = self._context_key(context)
        planned_contexts = self._child_contexts(context)
        self._contexts_by_parent[context_key] = self._missing_contexts(planned_contexts)
        self._inventory_by_parent[context_key] = {
            "planned": len(planned_contexts),
            "existing": len(planned_contexts) - len(self._contexts_by_parent[context_key]),
        }
        if self.explain:
            return [(self.universe_model, [context])]
        return [(self.universe_model, [context]), (self.model, self._contexts_by_parent[context_key])]

    @Flow.call
    def __call__(self, context: ContextType) -> GenericResult:
        context_key = self._context_key(context)
        contexts = self._contexts_by_parent.get(context_key)
        if contexts is None:
            planned_contexts = self._child_contexts(context)
            contexts = self._missing_contexts(planned_contexts)
            self._inventory_by_parent[context_key] = {
                "planned": len(planned_contexts),
                "existing": len(planned_contexts) - len(contexts),
            }
        if self.explain:
            return GenericResult(
                value={
                    "status": "planned",
                    "symbols": [getattr(child_context, self.symbol_field) for child_context in contexts],
                    "contexts": [child_context.model_dump(mode="json") for child_context in contexts],
                    "child_model": f"{self.model.__class__.__module__}.{self.model.__class__.__name__}",
                }
            )
        outputs = []
        status_counts: Dict[str, int] = {}
        for child_context in contexts:
            result = self.model(context=child_context)
            value = result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")
            if isinstance(value, dict):
                status = value.get("status")
                if status:
                    status_counts[str(status)] = status_counts.get(str(status), 0) + 1
            if self.include_outputs:
                outputs.append({"context": child_context.model_dump(mode="json"), "value": value})
        payload = {"symbols": len(contexts), "status_counts": status_counts}
        if self.skip_existing:
            inventory = self._inventory_by_parent[context_key]
            payload.update({"symbols": inventory["planned"], "missing": len(contexts), "existing": inventory["existing"]})
        if self.include_outputs:
            payload["outputs"] = outputs
        return GenericResult(value=payload)
