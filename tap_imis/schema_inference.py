import re
from pendulum import parse
from singer_sdk import typing as th


def infer_schema_from_records(records):
    """Fetch sample records and infer schema from them."""

    # Check if we have IMIS GenericPropertyDataCollection structure
    if records and len(records) > 0 and "Properties" in records[0]:
        # IMIS specific property structure handling
        properties_dict = _analyze_imis_property_records(records)
    else:
        # Standard records analysis
        properties_dict = _analyze_records(records)
    return [th.Property(key, value) for key, value in properties_dict.items()]

def _analyze_imis_property_records(records):
    """Analyze IMIS specific property-based records structure."""
    if not records:
        return {}
    
    # Collect all property names and their values across records
    all_properties = {}
    
    for record in records:
        properties = record.get("Properties", {})
        
        # Handle IMIS GenericPropertyDataCollection structure
        if isinstance(properties, dict) and "$type" in properties and "$values" in properties:
            if "GenericPropertyDataCollection" in properties.get("$type", ""):
                property_values = properties.get("$values", [])
                
                for prop in property_values:
                    if not isinstance(prop, dict) or "Name" not in prop:
                        continue
                        
                    name = prop.get("Name")
                    if name not in all_properties:
                        all_properties[name] = {"values": []}
                    
                    # Extract the value, which might be a simple value or a complex $value structure
                    value = None
                    if "Value" in prop:
                        raw_value = prop.get("Value")
                        if isinstance(raw_value, dict) and "$type" in raw_value and "$value" in raw_value:
                            # Complex IMIS value format
                            value_type = raw_value.get("$type", "")
                            value = raw_value.get("$value")
                            
                            # Type conversion based on $type
                            if "System.Int" in value_type:
                                try:
                                    value = int(value)
                                except (ValueError, TypeError):
                                    pass
                            elif "System.Decimal" in value_type or "System.Double" in value_type:
                                try:
                                    value = float(value)
                                except (ValueError, TypeError):
                                    pass
                            elif "System.Boolean" in value_type:
                                if isinstance(value, str):
                                    value = value.lower() == "true"
                                else:
                                    value = bool(value)
                        else:
                            # Simple value
                            value = raw_value
                    
                    # Track values for this property
                    if value is not None and len(all_properties[name]["values"]) < 5:
                        all_properties[name]["values"].append(value)
    
    # Generate schema properties from collected values
    property_schema = {}
    for key, info in all_properties.items():
        property_schema[key] = _infer_property_schema(key, info["values"])
    
    return property_schema

def _analyze_records(records):
    """Analyze records to create schema that resembles the metadata schema."""
    if not records or not isinstance(records[0], dict):
        return {}
        
    # Collect top-level properties and their values
    top_level_properties = {}
    
    for record in records:
        for key, value in record.items():
            if key == "$type":
                continue
                
            if key not in top_level_properties:
                top_level_properties[key] = []
            
            if len(top_level_properties[key]) < 5:
                top_level_properties[key].append(value)
    
    # Generate schema properties from collected values
    property_schema = {}
    for key, values in top_level_properties.items():
        property_schema[key] = _infer_property_schema(key, values)
    
    return property_schema

def _infer_property_schema(property_name, values):
    """Infer JSON schema for a property based on collected values."""
    if not values:
        # No values to analyze, use string as default
        return th.StringType()
        
    # Analyze types of collected values
    types = [type(v) for v in values if v is not None]
    
    # Handle case where all values are None
    if not types:
        return th.StringType()
        
    # Check for known field patterns first
    if _is_known_id_field(property_name):
        return th.StringType()
    
    if _is_known_boolean_field(property_name):
        return th.BooleanType()
        
    if _is_known_date_field(property_name):
        return th.DateTimeType()
        
    # Handle different value types
    if all(isinstance(v, bool) for v in values if v is not None):
        return th.BooleanType()
        
    if all(isinstance(v, int) for v in values if v is not None):
        return th.IntegerType()
        
    if all(isinstance(v, (int, float)) for v in values if v is not None):
        return th.NumberType()
        
    if all(isinstance(v, str) for v in values if v is not None):
        # Check if all strings are dates
        date_strings = [v for v in values if isinstance(v, str)]
        if date_strings and all(_is_date_string(s) for s in date_strings):
            return th.DateTimeType()
        return th.StringType()
        
    if all(isinstance(v, dict) for v in values if v is not None):
        return _infer_object_schema(property_name, values)
        
    if all(isinstance(v, list) for v in values if v is not None):
        return _infer_array_schema(property_name, values)
        
    # Mixed types - handle more flexibly
    return th.ObjectType()

def _infer_object_schema(property_name, obj_values):
    """Infer schema for an object property."""
    # Filter out None values
    valid_objects = [v for v in obj_values if isinstance(v, dict)]
    
    if not valid_objects:
        return th.ObjectType()
    
    # Special handling for AdditionalAttributes
    if property_name == "AdditionalAttributes":
        return _infer_additional_attributes_schema(valid_objects)
        
    # Detect if this is a collection-like field that should use 'item' pattern
    is_collection = _is_collection_pattern(property_name, valid_objects)
    
    if is_collection:
        return _infer_collection_object_schema(property_name, valid_objects)
        
    # Handle regular objects by analyzing all properties
    all_properties = {}
    for obj in valid_objects:
        for field, value in obj.items():
            if field == "$type":
                continue
                
            if field not in all_properties:
                all_properties[field] = []
                
            if len(all_properties[field]) < 5:
                all_properties[field].append(value)
        
    # Generate schema for each property
    property_schema = {}
    for field_name, field_values in all_properties.items():
        property_schema[field_name] = _infer_property_schema(field_name, field_values)
    properties = [th.Property(key, value) for key, value in property_schema.items()]
        
    return th.ObjectType(*properties)

def _infer_additional_attributes_schema(obj_values):
    """Dynamically infer schema for AdditionalAttributes by analyzing its structure."""
    # Collect all attribute names and values
    attributes = {}
    
    for obj in obj_values:

        
        if "$values" in obj:
            values = obj.get("$values", [])
            for item in values:
                if isinstance(item, dict) and "Name" in item and "Value" in item:
                    name = item.get("Name")
                    if not name:
                        continue
                        
                    value = item.get("Value")
                    # Handle complex value structures
                    if isinstance(value, dict) and "$type" in value and "$value" in value:
                        value = value.get("$value")
                        
                    if name not in attributes:
                        attributes[name] = []
                        
                    if len(attributes[name]) < 5:
                        attributes[name].append(value)
    
    # Generate schema for each attribute
    property_schema = {}
    for name, values in attributes.items():
        property_schema[name] = _infer_property_schema(name, values)
    properties = [th.Property(key, value) for key, value in property_schema.items()]
    return th.ObjectType(*properties)

def _is_collection_pattern(property_name, objects):
    """Dynamically determine if a field follows a collection pattern."""
    # Check for common collection field names
    common_collections = ["Addresses", "Emails", "Phones", "AlternateIds", 
                            "SocialNetworks", "CommunicationTypePreferences",
                            "Salutations"]
                            
    if property_name in common_collections:
        return True
            
    # Check structure patterns that indicate collections
    for obj in objects:
        # Pattern 1: Object has 'item' key
        if 'item' in obj:
            return True
            
        # Pattern 2: Object has '$values' key
        if '$values' in obj:
            return True
            
        # Pattern 3: Object has '$type' with Collection in name
        if '$type' in obj and ('Collection' in obj['$type'] or 'List' in obj['$type']):
            return True
    
    return False

def _infer_collection_object_schema(property_name, obj_values):
    """Infer schema for collection fields that need the 'item' pattern."""
    # For known collection types, use templates
    return _get_template_for_collection(property_name)

def _get_template_for_collection(collection_name):
    """Get predefined template schema for common collection types."""
    if collection_name == "Emails":
        email_type = th.ObjectType(
            th.Property("Address", th.StringType()),
            th.Property("EmailType", th.StringType())
        )
        return th.ArrayType(email_type)
    elif collection_name == "Phones":
        phone_type = th.ObjectType(
            th.Property("Number", th.StringType()),
            th.Property("PhoneType", th.StringType())
        )
        return th.ArrayType(phone_type)

    elif collection_name == "Addresses":
        address_type = th.ObjectType(
            th.Property("AddressId", th.StringType()),
            th.Property("CountryCode", th.StringType()),
            th.Property("CountryName", th.StringType()),
            th.Property("CountrySubEntityCode", th.StringType()),
            th.Property("CountrySubEntityName", th.StringType()),
            th.Property("FullAddress", th.StringType()),
            th.Property("AddresseeText", th.StringType()),
            th.Property("AddressPurpose", th.StringType()),
            th.Property("FullAddressId", th.StringType()),
            th.Property("DisplayOrganizationName", th.StringType())
        )
        return th.ArrayType(address_type)
      
    elif collection_name == "AlternateIds":
        alternate_id_type = th.ObjectType(
            th.Property("Id", th.StringType()),
            th.Property("IdType", th.StringType())
        )
        return th.ArrayType(alternate_id_type)
    elif collection_name == "SocialNetworks":
        social_network_type = th.ObjectType(
            th.Property("PartySocialNetworkId", th.StringType()),
            th.Property("SocialNetwork", th.ObjectType(
                th.Property("SocialNetworkId", th.StringType()),
                th.Property("SocialNetworkName", th.StringType()),
                th.Property("BaseURL", th.StringType())
            )),
            th.Property("SocialNetworkUserName", th.StringType()),
            th.Property("SocialNetworkUserId", th.StringType()),
            th.Property("SocialNetworkProfileLinkURL", th.StringType()),
            th.Property("SocialNetworkToken", th.StringType()),
            th.Property("UseSocialNetworkProfilePhoto", th.BooleanType())
        )
        return th.ArrayType(social_network_type)
    elif collection_name == "CommunicationTypePreferences":
        communication_type_preference_type = th.ObjectType(
            th.Property("PartyCommunicationTypePreferenceId", th.StringType()),
            th.Property("CommunicationTypeId", th.StringType()),
            th.Property("OptInFlag", th.BooleanType())
        )
        return th.ArrayType(communication_type_preference_type)
    elif collection_name == "Salutations":
        salutation_type = th.ObjectType(
            th.Property("IsOverridden", th.BooleanType()),
            th.Property("SalutationId", th.StringType()),
            th.Property("SalutationMethod", th.ObjectType(
                    th.Property("PartySalutationMethodId", th.StringType())
            )),
            th.Property("Text", th.StringType())
        )
        return th.ArrayType(salutation_type)
    else:
        # Generic item structure for other collections
        return th.ArrayType(th.StringType())

def _process_name_value_collection(property_name, values):
    """Process a collection with Name-Value paired objects."""
    # For AdditionalAttributes and similar collections
    return th.ObjectType()

def _infer_array_schema(property_name, array_values):
    """Infer schema for an array property."""
    # Filter out None values and flatten items
    flat_items = []
    for arr in array_values:
        if isinstance(arr, list):
            flat_items.extend(arr[:min(5, len(arr))])
            
    if not flat_items:
        return th.ArrayType(th.StringType())
        
    # Check for Name-Value pairs which should be converted to flat objects
    if all(isinstance(item, dict) for item in flat_items) and \
        all("Name" in item and "Value" in item for item in flat_items):
        # This is a Name-Value pair collection, process specially
        return _process_name_value_collection(property_name, flat_items)
        
    # Determine item type based on samples
    item_types = set(type(item) for item in flat_items if item is not None)
    
    if bool in item_types:
        item_schema = th.BooleanType()
    elif int in item_types and not any(t for t in item_types if t not in (int, type(None))):
        item_schema = th.IntegerType()
    elif any(t in item_types for t in (int, float)) and not any(t for t in item_types if t not in (int, float, type(None))):
        item_schema = th.NumberType()
    elif str in item_types and not any(t for t in item_types if t not in (str, type(None))):
        # Check if all strings are dates
        date_strings = [item for item in flat_items if isinstance(item, str)]
        if date_strings and all(_is_date_string(s) for s in date_strings):
            item_schema =th.DateTimeType()
        else:
            item_schema = th.StringType()
    elif dict in item_types:
        # For objects, we won't recurse to avoid deeply nested schemas
        item_schema = th.ObjectType()
    else:
        # Default to string for mixed types
        item_schema = th.StringType()
    
    return th.ArrayType(item_schema)

def _is_date_string(string_val):
    """Check if a string is a date/datetime."""
    if not isinstance(string_val, str):
        return False
        
    try:
        parse(string_val)
        return True
    except:
        return False

def _is_known_id_field(field_name):
    """Check if field is a known ID field that should be a string."""
    id_patterns = [
        r"Id$", r"ID$", 
        r"PartyId", r"UniformId", 
        r"^PartySalutationMethodId$", r"^SalutationId$", 
        r"^AddressId$", r"^FullAddressId$"
    ]
    
    return any(re.search(pattern, field_name) for pattern in id_patterns)

def _is_known_boolean_field(field_name):
    """Check if field is a known boolean field."""
    boolean_patterns = [
        r"Is[A-Z]", r"^SortIsOverridden$", r"^IsMarkedForDelete$", 
        r"^IsDefault$", r"^IsOverridden$", r"^IsOverrideable$",
        r"^IsAnonymous$", r"^IsExempt$", r"^IsOngoing$", r"^IsPast$",
        r"RECURRING_REQUEST$"
    ]
    
    return any(re.search(pattern, field_name) for pattern in boolean_patterns)

def _is_known_date_field(field_name):
    """Check if field is a known date field."""
    date_patterns = [
        r"Date$", r"^BirthDate$", r"^CreatedOn$", r"^UpdatedOn$",
        r"^EFFECTIVE_DATE$", r"^TRANSACTION_DATE$", r"^NEXT_INSTALL_DATE$",
        r"^THRU_DATE$", r"^TICKLER_DATE$", r"^UF_6$", r"^UF_7$",
        r"^JOINDATE$", r"^DeclarationReceived$", r"^Cancelled$", 
        r"^ConfirmationLetterSent$"
    ]
    
    return any(re.search(pattern, field_name) for pattern in date_patterns)
