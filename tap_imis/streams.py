"""Stream type classes for tap-imis."""

from __future__ import annotations

from hotglue_singer_sdk import typing as th

from tap_imis.client import IMISStream
from tap_imis.transforms import normalize_activity, normalize_group_member, normalize_party


class ContactsStream(IMISStream):
    """iMIS Party (contacts) stream."""

    name = "contacts"
    path = "/Party"
    primary_keys = ["PartyId"]
    replication_key = "UpdatedOn"
    record_normalizer = staticmethod(normalize_party)
    schema_property_overrides = [
        th.Property("PartyId", th.StringType),
        th.Property("UpdatedOn", th.DateTimeType),
    ]


class ActivitiesStream(IMISStream):
    """iMIS Activity stream."""

    name = "activities"
    path = "/Activity"
    primary_keys = ["PartyId", "ActivityId"]
    replication_key = "EFFECTIVE_DATE"
    record_normalizer = staticmethod(normalize_activity)
    schema_property_overrides = [
        th.Property("PartyId", th.StringType),
        th.Property("ActivityId", th.StringType),
        th.Property("EFFECTIVE_DATE", th.DateTimeType),
    ]


class EventsStream(IMISStream):
    """iMIS Event stream."""

    name = "event"
    path = "/Event"
    primary_keys = ["EventId"]
    schema_property_overrides = [
        th.Property("EventId", th.StringType),
    ]


class GroupsStream(IMISStream):
    """iMIS Group stream."""

    name = "group"
    path = "/Group"
    primary_keys = ["GroupId"]
    schema_property_overrides = [
        th.Property("GroupId", th.StringType),
    ]


class GroupMembersStream(IMISStream):
    """iMIS GroupMember stream."""

    name = "group_member"
    path = "/GroupMember"
    primary_keys = ["GroupMemberId"]
    record_normalizer = staticmethod(normalize_group_member)
    schema_property_overrides = [
        th.Property("GroupMemberId", th.StringType),
        th.Property("GroupId", th.StringType),
        th.Property("PartyId", th.StringType),
    ]
