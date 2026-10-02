"""Convert iMIS API records into plain JSON."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

RecordNormalizer = Callable[[Dict[str, Any]], Dict[str, Any]]


def unwrap_imis(value: Any) -> Any:
    """Convert an iMIS API value into plain JSON, at every nesting level.

    Drops ``$type`` keys, turns ``{"$values": [...]}`` into a list and
    ``{"$value": x}`` into ``x``, and turns property bags (lists of
    ``Name``/``Value`` pairs) into dicts.
    """
    if isinstance(value, list):
        return [unwrap_imis(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "$value" in value:
        return unwrap_imis(value["$value"])
    if "$values" in value:
        if "GenericPropertyDataCollection" in value.get("$type", ""):
            return property_bag_to_dict(value["$values"])
        return unwrap_imis(value["$values"])
    return {key: unwrap_imis(item) for key, item in value.items() if key != "$type"}


def property_bag_to_dict(properties: list) -> Dict[str, Any]:
    """Convert iMIS ``Name``/``Value`` pairs into a dict, mapping empty strings to null."""
    result: Dict[str, Any] = {}
    for prop in properties:
        if not isinstance(prop, dict) or not prop.get("Name"):
            continue
        value = unwrap_imis(prop.get("Value"))
        result[prop["Name"]] = None if value == "" else value
    return result


def identity_element(identity: Optional[dict]) -> Optional[str]:
    """Return the ID from an unwrapped iMIS identity object, or None if it has none."""
    elements = (identity or {}).get("IdentityElements") or []
    return str(elements[0]) if elements else None


def flatten_full_address_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Move the nested ``Address`` fields up onto the address item, keeping existing keys."""
    out = dict(item)
    nested_address = out.pop("Address", None)
    if isinstance(nested_address, dict):
        for key, value in nested_address.items():
            out.setdefault(key, value)
    return out


def normalize_party(record: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap a Party, promote ``UpdatedOn``, and flatten address items."""
    out = unwrap_imis(record)
    update_info = out.pop("UpdateInformation", None)
    if isinstance(update_info, dict) and update_info.get("UpdatedOn") is not None:
        out["UpdatedOn"] = update_info["UpdatedOn"]

    addresses = out.get("Addresses")
    if isinstance(addresses, list):
        out["Addresses"] = [
            flatten_full_address_item(item) for item in addresses if isinstance(item, dict)
        ]
    return out


def normalize_group_member(record: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap a GroupMember and copy its group and party IDs to the top level for joins."""
    out = unwrap_imis(record)
    group = out.get("Group") or {}
    party = out.get("Party") or {}
    promoted = {
        "GroupId": group.get("GroupId"),
        "PartyId": party.get("PartyId"),
        "PartyName": party.get("Name"),
    }
    for key, value in promoted.items():
        if out.get(key) is None and value is not None:
            out[key] = value
    return out


def normalize_activity(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return an Activity's properties as one flat object, plus its ActivityId and PartyId."""
    unwrapped = unwrap_imis(record)
    out = dict(unwrapped.get("Properties") or {})
    activity_id = identity_element(unwrapped.get("Identity"))
    party_id = identity_element(unwrapped.get("PrimaryParentIdentity"))
    if activity_id is not None:
        out["ActivityId"] = activity_id
    if party_id is not None:
        out["PartyId"] = party_id
    return out
