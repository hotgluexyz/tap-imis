"""Tests for metadata-based schema types."""

from hotglue_singer_sdk import typing as th

from tap_imis.metadata_schema import properties_from_metadata_body


def test_metadata_types_match_unwrapped_records():
    body = {
        "Properties": {
            "$values": [
                {"Name": "QUANTITY", "PropertyTypeName": "Decimal"},
                {"Name": "CreditLimit", "PropertyTypeName": "Monetary"},
                {
                    "Name": "Emails",
                    "PropertyTypeName": "EntityDefinitionData",
                    "ItemEntityPropertyDefinition": {
                        "Name": "item",
                        "PropertyTypeName": "EntityDefinitionData",
                        "EntityDefinition": {
                            "Properties": {
                                "$values": [{"Name": "Address", "PropertyTypeName": "String"}]
                            }
                        },
                    },
                },
                {
                    "Name": "AdditionalAttributes",
                    "PropertyTypeName": "GenericPropertyDataCollection",
                    "GenericPropertyDefinitions": {
                        "$values": [{"Name": "JoinDate", "PropertyTypeName": "Date"}]
                    },
                },
            ]
        }
    }
    props = th.PropertiesList(*properties_from_metadata_body(body)).to_dict()["properties"]

    assert props["QUANTITY"]["type"] == ["number", "null"]
    assert props["CreditLimit"]["type"] == ["number", "null"]
    assert props["Emails"]["type"] == ["array", "null"]
    assert "Address" in props["Emails"]["items"]["properties"]
    assert props["AdditionalAttributes"]["properties"]["JoinDate"]["format"] == "date-time"
