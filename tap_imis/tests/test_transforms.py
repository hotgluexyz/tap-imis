"""Tests for iMIS record normalization."""

from tap_imis.transforms import (
    normalize_activity,
    normalize_group_member,
    normalize_party,
    unwrap_imis,
)

PROPERTY_BAG_TYPE = "Asi.Soa.Core.DataContracts.GenericPropertyDataCollection, Asi.Contracts"


def test_normalize_party_promotes_updated_on():
    record = {
        "$type": "Asi.Soa.Membership.DataContracts.PartyData, Asi.Contracts",
        "PartyId": "12345",
        "Name": "Sample",
        "UpdateInformation": {
            "UpdatedOn": "2025-01-15T10:00:00",
            "CreatedOn": "2020-01-01T00:00:00",
        },
    }
    out = normalize_party(record)
    assert out["PartyId"] == "12345"
    assert out["UpdatedOn"] == "2025-01-15T10:00:00"
    assert "UpdateInformation" not in out
    assert "$type" not in out


def test_normalize_party_unwraps_addresses_collection():
    record = {
        "PartyId": "1001",
        "Addresses": {
            "$type": "Asi.Soa.Membership.DataContracts.FullAddressDataCollection, Asi.Contracts",
            "$values": [
                {
                    "$type": "Asi.Soa.Membership.DataContracts.FullAddressData, Asi.Contracts",
                    "Address": {
                        "$type": "Asi.Soa.Membership.DataContracts.AddressData, Asi.Contracts",
                        "AddressId": "2001",
                        "CountryCode": "CA",
                        "FullAddress": "ON  CANADA",
                    },
                    "AddresseeText": "Sample",
                    "AddressPurpose": "TwoPurpose",
                    "FullAddressId": "2001",
                }
            ],
        },
    }
    out = normalize_party(record)
    assert isinstance(out["Addresses"], list)
    assert out["Addresses"][0]["AddressId"] == "2001"
    assert out["Addresses"][0]["CountryCode"] == "CA"
    assert out["Addresses"][0]["AddresseeText"] == "Sample"
    assert "$type" not in out["Addresses"][0]


def test_normalize_group_member_promotes_join_ids():
    record = {
        "$type": "Asi.Soa.Membership.DataContracts.GroupMemberData, Asi.Contracts",
        "GroupMemberId": "GOLD:1001",
        "Group": {
            "$type": "Asi.Soa.Membership.DataContracts.GroupData, Asi.Contracts",
            "GroupId": "GOLD",
            "Name": "Gold Members",
        },
        "Party": {
            "$type": "Asi.Soa.Membership.DataContracts.PartySummaryData, Asi.Contracts",
            "PartyId": "1001",
            "Name": "Sample Person",
        },
    }
    out = normalize_group_member(record)
    assert out["GroupId"] == "GOLD"
    assert out["PartyId"] == "1001"
    assert out["PartyName"] == "Sample Person"
    assert out["Group"] == {"GroupId": "GOLD", "Name": "Gold Members"}


def test_normalize_activity_flattens_property_bag():
    record = {
        "$type": "Asi.Soa.Core.DataContracts.ActivityData, Asi.Contracts",
        "Identity": {
            "IdentityElements": {"$values": ["99"]},
        },
        "PrimaryParentIdentity": {
            "IdentityElements": {"$values": ["42"]},
        },
        "Properties": {
            "$type": PROPERTY_BAG_TYPE,
            "$values": [
                {"Name": "ACTIVITY_TYPE", "Value": "DUES"},
                {
                    "Name": "EFFECTIVE_DATE",
                    "Value": {"$type": "System.DateTime", "$value": "2024-06-01T00:00:00"},
                },
                {"Name": "AMOUNT", "Value": {"$type": "System.Decimal", "$value": 12.5}},
            ]
        },
    }
    out = normalize_activity(record)
    assert out == {
        "ActivityId": "99",
        "PartyId": "42",
        "ACTIVITY_TYPE": "DUES",
        "EFFECTIVE_DATE": "2024-06-01T00:00:00",
        "AMOUNT": 12.5,
    }


def test_unwrap_imis_cleans_nested_wrappers():
    record = {
        "$type": "Asi.Soa.Events.DataContracts.EventData, Asi.Contracts",
        "EventId": "ANNCONF",
        "Functions": {
            "$type": "Asi.Soa.Events.DataContracts.EventFunctionDataCollection, Asi.Contracts",
            "$values": [
                {
                    "$type": "Asi.Soa.Events.DataContracts.EventFunctionData, Asi.Contracts",
                    "Category": {"$type": "Category, Asi.Contracts", "Name": "REG"},
                    "Capacity": {"$type": "System.Int32", "$value": 50},
                    "AdditionalAttributes": {
                        "$type": PROPERTY_BAG_TYPE,
                        "$values": [
                            {
                                "$type": "Asi.Soa.Core.DataContracts.GenericPropertyData, Asi.Contracts",
                                "Name": "WebEnabled",
                                "Value": {"$type": "System.Boolean", "$value": True},
                            },
                            {"Name": "VAT_RULE", "Value": ""},
                        ],
                    },
                }
            ],
        },
    }
    assert unwrap_imis(record) == {
        "EventId": "ANNCONF",
        "Functions": [
            {
                "Category": {"Name": "REG"},
                "Capacity": 50,
                "AdditionalAttributes": {"WebEnabled": True, "VAT_RULE": None},
            }
        ],
    }
