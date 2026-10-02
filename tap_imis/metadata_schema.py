"""Build Singer schemas from iMIS /metadata responses.

Types describe records after ``transforms.unwrap_imis``: collections are arrays and
generic property bags are objects.
"""

from __future__ import annotations

from typing import Any, List

from hotglue_singer_sdk import typing as th

SCALAR_TYPES = {
    "String": th.StringType,
    "Boolean": th.BooleanType,
    "Date": th.DateTimeType,
    "Integer": th.IntegerType,
    "Decimal": th.NumberType,
    "Monetary": th.NumberType,
}


def _object_type(property_defs: List[dict]) -> th.ObjectType:
    """Build an object type from a list of metadata property definitions."""
    return th.ObjectType(
        *(
            th.Property(property_def["Name"], jsonschema_type_for_metadata_property(property_def))
            for property_def in property_defs
            if property_def.get("Name")
        )
    )


def jsonschema_type_for_metadata_property(property_def: dict) -> Any:
    """Map an iMIS metadata property definition to a JSON Schema type."""
    type_name = property_def.get("PropertyTypeName", "String")
    if type_name in SCALAR_TYPES:
        return SCALAR_TYPES[type_name]()
    if type_name == "EntityDefinitionData":
        item_property = property_def.get("ItemEntityPropertyDefinition")
        if item_property:
            return th.ArrayType(jsonschema_type_for_metadata_property(item_property))
        entity_definition = property_def.get("EntityDefinition") or {}
        return _object_type((entity_definition.get("Properties") or {}).get("$values") or [])
    if type_name == "GenericPropertyDataCollection":
        return _object_type((property_def.get("GenericPropertyDefinitions") or {}).get("$values") or [])
    return th.StringType()


def properties_from_metadata_body(body: dict) -> List[th.Property]:
    """Parse a metadata response into Singer schema properties."""
    property_defs = (body.get("Properties") or {}).get("$values") or []
    return [
        th.Property(property_def["Name"], jsonschema_type_for_metadata_property(property_def))
        for property_def in property_defs
        if property_def.get("Name")
    ]
