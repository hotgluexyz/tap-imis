"""Tests for combined metadata and sample schema discovery."""

from unittest.mock import MagicMock

import pytest
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_imis.schema_discover import (
    discover_stream_schema,
    load_metadata_properties,
    load_sample_properties,
    merge_schema_property_groups,
)


def test_merge_schema_property_groups_prefers_earlier_groups():
    schema = merge_schema_property_groups(
        [th.Property("Id", th.StringType)],
        [th.Property("Id", th.IntegerType), th.Property("Name", th.StringType)],
        [th.Property("Name", th.IntegerType), th.Property("Extra", th.BooleanType)],
    )
    props = schema["properties"]
    assert props["Id"]["type"] == ["string", "null"]
    assert props["Name"]["type"] == ["string", "null"]
    assert props["Extra"]["type"] == ["boolean", "null"]


def test_discover_stream_schema_merges_all_sources():
    stream = MagicMock()
    stream.path = "/Event"
    stream.url_base = "https://example.com/api/"
    stream.schema_property_overrides = [th.Property("EventId", th.StringType)]
    stream.logger = MagicMock()

    stream._request_with_backoff.return_value.json.return_value = {
        "Properties": {
            "$values": [
                {"Name": "EventId", "PropertyTypeName": "String"},
                {"Name": "FromMetadata", "PropertyTypeName": "String"},
            ]
        }
    }
    stream._fetch_sample_records.return_value = [
        {"EventId": "1", "Location": "Hall", "$type": "ignored"},
    ]
    stream.normalize_record.side_effect = lambda record: {
        k: v for k, v in record.items() if k != "$type"
    }

    schema = discover_stream_schema(stream)
    props = schema["properties"]
    assert set(props) == {"EventId", "FromMetadata", "Location"}


def test_load_metadata_properties_returns_none_on_http_error():
    stream = MagicMock()
    stream.path = "/Party"
    stream.url_base = "https://example.com/api/"
    stream.logger = MagicMock()
    stream._request_with_backoff.side_effect = FatalAPIError("501")

    assert load_metadata_properties(stream) is None


def test_discover_stream_schema_uses_metadata_when_samples_fail():
    stream = MagicMock()
    stream.path = "/Event"
    stream.url_base = "https://example.com/api/"
    stream.schema_property_overrides = [th.Property("EventId", th.StringType)]
    stream.logger = MagicMock()
    stream._fetch_sample_records.side_effect = RetriableAPIError("503")
    stream._request_with_backoff.return_value.json.return_value = {
        "Properties": {"$values": [{"Name": "Capacity", "PropertyTypeName": "Integer"}]}
    }

    assert set(discover_stream_schema(stream)["properties"]) == {"EventId", "Capacity"}


def test_discover_stream_schema_raises_when_samples_and_metadata_fail():
    stream = MagicMock()
    stream.name = "event"
    stream.path = "/Event"
    stream.url_base = "https://example.com/api/"
    stream.logger = MagicMock()
    stream._fetch_sample_records.side_effect = RetriableAPIError("503")
    stream._request_with_backoff.side_effect = RetriableAPIError("503")

    with pytest.raises(RuntimeError, match="event"):
        discover_stream_schema(stream)


def test_discover_stream_schema_prefers_sample_types_over_metadata():
    stream = MagicMock()
    stream.path = "/Activity"
    stream.url_base = "https://example.com/api/"
    stream.schema_property_overrides = [
        th.Property("PartyId", th.StringType),
        th.Property("ActivityId", th.StringType),
        th.Property("EFFECTIVE_DATE", th.DateTimeType),
    ]
    stream.logger = MagicMock()
    stream._request_with_backoff.return_value.json.return_value = {
        "Properties": {
            "$values": [
                {"Name": "AMOUNT", "PropertyTypeName": "String"},
                {"Name": "THRU_DATE", "PropertyTypeName": "Date"},
            ]
        }
    }
    stream._fetch_sample_records.return_value = [{"Properties": {"$values": []}}]
    stream.normalize_record.return_value = {
        "PartyId": "1",
        "ActivityId": "2",
        "AMOUNT": 0,
        "RECURRING_REQUEST": False,
        "THRU_DATE": None,
    }

    props = discover_stream_schema(stream)["properties"]
    assert props["AMOUNT"]["type"] == ["number", "null"]
    assert props["RECURRING_REQUEST"]["type"] == ["boolean", "null"]
    assert props["THRU_DATE"]["format"] == "date-time"


def test_load_sample_properties_returns_empty_when_no_records():
    stream = MagicMock()
    stream.path = "/Party"
    stream.logger = MagicMock()
    stream._fetch_sample_records.return_value = []

    assert load_sample_properties(stream) == ([], [])
