"""Build stream schemas from overrides, sample records, and iMIS metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_imis.metadata_schema import properties_from_metadata_body
from tap_imis.schema_inference import infer_schema_from_records

if TYPE_CHECKING:
    from tap_imis.client import IMISStream


def merge_schema_property_groups(*property_groups: List[th.Property]) -> dict:
    """Merge property lists into one JSON schema. The first list to define a name wins."""
    properties: List[th.Property] = []
    seen: set[str] = set()
    for group in property_groups:
        for prop in group:
            if prop.name in seen:
                continue
            seen.add(prop.name)
            properties.append(prop)
    if not properties:
        return {"type": "object", "properties": {}}
    return th.PropertiesList(*properties).to_dict()


def load_metadata_properties(stream: IMISStream) -> Optional[List[th.Property]]:
    """Return properties from ``GET /metadata{path}``, or None if the call fails."""
    url = f"{stream.url_base.rstrip('/')}/metadata{stream.path}"
    try:
        body = stream._request_with_backoff(url).json()
    except (FatalAPIError, RetriableAPIError) as exc:
        stream.logger.warning("Metadata fetch for %s failed: %s", stream.path, exc)
        return None
    return properties_from_metadata_body(body)


def load_sample_properties(
    stream: IMISStream,
) -> Optional[Tuple[List[th.Property], List[th.Property]]]:
    """Infer properties from the first page of normalized records, or None if the call fails.

    Returns ``(typed, null_only)`` as described in ``infer_schema_from_records``.
    """
    try:
        records = stream._fetch_sample_records()
    except (FatalAPIError, RetriableAPIError) as exc:
        stream.logger.warning("Sample fetch for %s failed: %s", stream.path, exc)
        return None
    if not records:
        stream.logger.warning(
            "No records found for %s during schema discovery; continuing with metadata and overrides.",
            stream.path,
        )
        return [], []
    normalized = [stream.normalize_record(record) for record in records]
    return infer_schema_from_records(normalized)


def discover_stream_schema(stream: IMISStream) -> dict:
    """Build the stream's JSON schema.

    When several sources define the same field, the first one here wins:

    1. Overrides (primary keys, replication keys, and other fixed types).
    2. Types inferred from sample values, so the schema matches what sync emits.
    3. Metadata, which also adds fields missing from the samples (for example
       tenant-specific custom Party fields).
    4. A catch-all type for sample fields that were null in every record.

    Raises if the sample request fails and metadata gives no fields either. A schema
    with only the overrides would make sync drop every other field without failing.
    """
    samples = load_sample_properties(stream)
    metadata = load_metadata_properties(stream)
    if samples is None and not metadata:
        raise RuntimeError(
            f"Could not build a schema for '{stream.name}': "
            "the sample request failed and metadata returned no fields."
        )
    typed_samples, null_only_samples = samples or ([], [])
    return merge_schema_property_groups(
        stream.schema_property_overrides,
        typed_samples,
        metadata or [],
        null_only_samples,
    )
