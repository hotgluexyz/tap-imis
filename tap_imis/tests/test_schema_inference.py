"""Tests for sample-based schema inference."""

from hotglue_singer_sdk import typing as th

from tap_imis.schema_inference import infer_schema_from_records


def _schema(properties):
    return th.PropertiesList(*properties).to_dict()["properties"]


def test_infer_schema_types_scalars_from_all_samples():
    records = [
        {"AMOUNT": 0, "JoinDate": "2012-02-04T00:00:00", "Code": "M", "IsActive": True},
        {"AMOUNT": 12.5, "JoinDate": "2020-01-01T00:00:00", "Code": "2012", "IsActive": False},
    ]
    typed, null_only = infer_schema_from_records(records)
    props = _schema(typed)

    assert null_only == []
    assert props["AMOUNT"]["type"] == ["number", "null"]
    assert props["JoinDate"]["format"] == "date-time"
    assert props["Code"]["type"] == ["string", "null"]
    assert props["IsActive"]["type"] == ["boolean", "null"]


def test_infer_schema_recurses_into_objects_and_arrays():
    records = [
        {"Roles": [{"RoleId": "R1", "Stage": {"Name": "Active"}}], "Tags": []},
        {"Roles": [{"RoleId": "R2", "Weight": 3}], "Tags": []},
    ]
    typed, _ = infer_schema_from_records(records)
    roles = _schema(typed)["Roles"]

    assert roles["type"] == ["array", "null"]
    item_props = roles["items"]["properties"]
    assert set(item_props) == {"RoleId", "Stage", "Weight"}
    assert item_props["Stage"]["properties"]["Name"]["type"] == ["string", "null"]
    assert item_props["Weight"]["type"] == ["number", "null"]


def test_infer_schema_splits_null_only_fields_with_permissive_type():
    records = [{"PartyId": "1", "Note": None}, {"PartyId": "2", "Note": None}]
    typed, null_only = infer_schema_from_records(records)

    assert [prop.name for prop in typed] == ["PartyId"]
    note_type = _schema(null_only)["Note"]["type"]
    assert "boolean" not in note_type
    assert {"string", "number", "object", "array"} <= set(note_type)
