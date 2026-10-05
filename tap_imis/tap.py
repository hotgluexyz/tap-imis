"""iMIS tap class."""

from __future__ import annotations

from typing import List

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th

from tap_imis.client import IMISStream
from tap_imis.streams import (
    ActivitiesStream,
    ContactsStream,
    EventsStream,
    GroupMembersStream,
    GroupsStream,
)

STREAM_TYPES = [
    ContactsStream,
    ActivitiesStream,
    EventsStream,
    GroupsStream,
    GroupMembersStream,
]


class TapIMIS(Tap):
    """Singer tap for iMIS."""

    name = "tap-imis"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "site_url",
            th.StringType,
            required=True,
            description="Base URL of the iMIS site (for example https://example.imiscloud.com)",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="iMIS API username",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="iMIS API password",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Incremental streams only sync records on or after this date (ISO 8601)",
        ),
    ).to_dict()

    def discover_streams(self) -> List[IMISStream]:
        """Return all supported iMIS entity streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapIMIS.cli()
