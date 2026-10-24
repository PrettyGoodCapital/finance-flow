import json
from datetime import date
from hashlib import sha256
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from ccflow import ModelRegistry
from pydantic import BaseModel as PydanticBaseModel

__all__ = (
    "source_identity",
    "transform_provenance",
)

_IDENTITY_FIELDS = ("etag", "version_id", "size", "last_modified")
_RUNTIME_FIELDS = {"meta", "explain", "overwrite_output"}


def source_identity(dataset: Any, value: date) -> dict[str, Any]:
    """A partition's URI plus the object identity its store reports (ETag, version, size, modification time)."""
    identity: dict[str, Any] = {"uri": dataset.uri(value)}
    store = ModelRegistry.root()[dataset.store] if isinstance(dataset.store, str) else dataset.store
    head = getattr(store, "head", None)
    if head is not None:
        reported = head(dataset.key(value))
        identity.update({name: reported[name] for name in _IDENTITY_FIELDS if reported.get(name) is not None})
    return identity


def _is_model_field(value: Any) -> bool:
    if isinstance(value, PydanticBaseModel):
        return True
    return isinstance(value, dict) and bool(value) and all(isinstance(item, PydanticBaseModel) for item in value.values())


def transform_provenance(model: PydanticBaseModel, packages: tuple[str, ...] = ("finance-flow", "polars")) -> dict[str, Any]:
    """The model that produced an output, a fingerprint of its own settings, and the package versions it ran with.

    Datasets and upstream models are left out of the fingerprint; sources record them. Runtime flags such as
    ``overwrite_output`` are left out, so rerunning the same transform keeps the same fingerprint.
    """
    settings = {
        name: value
        for name, value in model.model_dump(mode="json", exclude=_RUNTIME_FIELDS).items()
        if name in type(model).model_fields and not _is_model_field(getattr(model, name))
    }
    versions = {}
    for package in packages:
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = None
    return {
        "model": f"{type(model).__module__}.{type(model).__qualname__}",
        "settings": settings,
        "config_fingerprint": sha256(json.dumps(settings, sort_keys=True, default=str).encode()).hexdigest(),
        "versions": versions,
    }
