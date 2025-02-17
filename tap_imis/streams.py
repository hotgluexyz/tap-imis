"""Stream type classes for tap-IMIS."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_imis.client import IMISStream


class ContactsStream(IMISStream):
    """Define custom stream for Contacts."""

    name = "contacts"
    path = "/Party"

class ActivitiesStream(IMISStream):
    """Define custom stream for Activities."""
    name = "activities"
    path = "/Activity"