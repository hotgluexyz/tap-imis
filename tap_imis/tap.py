"""ActionKit tap class."""

from __future__ import annotations
from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_imis.client import IMISStream
from tap_imis.streams import ContactsStream, ActivitiesStream

STREAM_TYPES = [
    ContactsStream,
    ActivitiesStream
]


class TapIMIS(Tap):
    """IMIS tap class."""

    name = "tap-imis"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "site_url",
            th.StringType,
            required=False,
            description="URL of the IMIS site",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="API username for authentication",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="API password for authentication",
        ),
    ).to_dict()

    def discover_streams(self) -> List[IMISStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
    
    def sync_all(self) -> None:
        raise NotImplementedError("Sync all is not implemented")


if __name__ == "__main__":
    TapIMIS.cli()
