"""Infer Singer schema properties from normalized sample records."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from hotglue_singer_sdk import typing as th

ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")


def infer_schema_from_records(
    records: List[dict],
) -> Tuple[List[th.Property], List[th.Property]]:
    """Infer top-level properties from sample records.

    Returns ``(typed, null_only)``. ``typed`` has fields with at least one non-null
    value. ``null_only`` has fields that were null in every record, so callers can let
    a better source, like iMIS metadata, decide their type.
    """
    typed: List[th.Property] = []
    null_only: List[th.Property] = []
    for key, values in _values_by_key(records).items():
        prop = th.Property(key, _infer_type(values, nested=False))
        if any(value is not None for value in values):
            typed.append(prop)
        else:
            null_only.append(prop)
    return typed, null_only


def _values_by_key(objects: List[dict]) -> Dict[str, List[Any]]:
    """Group every value seen per key across a list of objects."""
    values: Dict[str, List[Any]] = {}
    for obj in objects:
        for key, value in obj.items():
            values.setdefault(key, []).append(value)
    return values


def _unknown_type(nested: bool) -> th.CustomType:
    """Return a catch-all type for values we couldn't infer a single type for."""
    types = ["string", "number", "object", "array"]
    # The SDK coerces top-level fields whose schema allows "boolean" into bools,
    # so only nested fallbacks may accept booleans.
    if nested:
        types.append("boolean")
    return th.CustomType({"type": types})


def _infer_type(values: List[Any], nested: bool) -> Any:
    """Infer a JSON schema type that accepts every sampled value."""
    present = [value for value in values if value is not None]
    if not present:
        return _unknown_type(nested)
    if all(isinstance(value, bool) for value in present):
        return th.BooleanType()
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in present):
        return th.NumberType()
    if all(isinstance(value, str) for value in present):
        if all(ISO_DATETIME.match(value) for value in present):
            return th.DateTimeType()
        return th.StringType()
    if all(isinstance(value, dict) for value in present):
        return th.ObjectType(
            *(
                th.Property(key, _infer_type(child_values, nested=True))
                for key, child_values in _values_by_key(present).items()
            )
        )
    if all(isinstance(value, list) for value in present):
        items = [item for value in present for item in value]
        return th.ArrayType(_infer_type(items, nested=True))
    return _unknown_type(nested)
