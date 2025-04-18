import re
from pendulum import parse
from singer_sdk import typing as th
import requests

PARAM_LIMIT = 500

class SchemaInference:
    """Schema inference utilities for IMIS API responses."""
    
    def __init__(self, logger=None):
        self.logger = logger
    
    def infer_schema_from_records(self, api_url, auth_token, path, logger=None):
        """Fetch sample records and infer schema from them."""
        if logger:
            self.logger = logger
            
        url = f"{api_url}{path}"
        headers = {"Authorization": f"Bearer {auth_token}"}
        
        params = {"limit": PARAM_LIMIT}  
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        json_response = response.json()
        items = json_response.get("Items", {})
        
        if isinstance(items, dict) and "$values" in items:
            records = items.get("$values", [])
        else:
            records = items if isinstance(items, list) else []
            
        if not records:
            if self.logger:
                self.logger.warning(f"No records found for {path}. Using empty schema.")
            return {}

        # Check if we have IMIS GenericPropertyDataCollection structure
        if records and len(records) > 0 and "Properties" in records[0]:
            # IMIS specific property structure handling
            return self._analyze_imis_property_records(records)
        else:
            # Standard records analysis
            return self._analyze_records(records)

    def _analyze_imis_property_records(self, records):
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
            property_schema[key] = self._infer_property_schema(key, info["values"])
        
        return {"properties": property_schema, "type": "object"}

    def _analyze_records(self, records):
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
            property_schema[key] = self._infer_property_schema(key, values)
        
        return {"properties": property_schema, "type": "object"}
    
    def _infer_property_schema(self, property_name, values):
        """Infer JSON schema for a property based on collected values."""
        if not values:
            # No values to analyze, use string as default
            return {"type": ["string", "null"]}
            
        # Analyze types of collected values
        types = [type(v) for v in values if v is not None]
        
        # Handle case where all values are None
        if not types:
            return {"type": ["string", "null"]}
            
        # Check for known field patterns first
        if self._is_known_id_field(property_name):
            return {"type": ["string", "null"]}
        
        if self._is_known_boolean_field(property_name):
            return {"type": ["boolean", "null"]}
            
        if self._is_known_date_field(property_name):
            return {"format": "date-time", "type": ["string", "null"]}
            
        # Handle different value types
        if all(isinstance(v, bool) for v in values if v is not None):
            return {"type": ["boolean", "null"]}
            
        if all(isinstance(v, int) for v in values if v is not None):
            return {"type": ["integer", "null"]}
            
        if all(isinstance(v, (int, float)) for v in values if v is not None):
            return {"type": ["number", "null"]}
            
        if all(isinstance(v, str) for v in values if v is not None):
            # Check if all strings are dates
            date_strings = [v for v in values if isinstance(v, str)]
            if date_strings and all(self._is_date_string(s) for s in date_strings):
                return {"format": "date-time", "type": ["string", "null"]}
            return {"type": ["string", "null"]}
            
        if all(isinstance(v, dict) for v in values if v is not None):
            return self._infer_object_schema(property_name, values)
            
        if all(isinstance(v, list) for v in values if v is not None):
            return self._infer_array_schema(property_name, values)
            
        # Mixed types - handle more flexibly
        return {"type": ["number", "string", "object", "null"]}
    
    def _infer_object_schema(self, property_name, obj_values):
        """Infer schema for an object property."""
        # Filter out None values
        valid_objects = [v for v in obj_values if isinstance(v, dict)]
        
        if not valid_objects:
            return {"type": ["object", "null"]}
        
        # Special handling for AdditionalAttributes
        if property_name == "AdditionalAttributes":
            return self._infer_additional_attributes_schema(valid_objects)
            
        # Detect if this is a collection-like field that should use 'item' pattern
        is_collection = self._is_collection_pattern(property_name, valid_objects)
        
        if is_collection:
            return self._infer_collection_object_schema(property_name, valid_objects)
            
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
            property_schema[field_name] = self._infer_property_schema(field_name, field_values)
            
        return {"properties": property_schema, "type": ["object", "null"]}
    
    def _infer_additional_attributes_schema(self, obj_values):
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
            property_schema[name] = self._infer_property_schema(name, values)
            
        return {"properties": property_schema, "type": ["object", "null"]}
    
    def _is_collection_pattern(self, property_name, objects):
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
    
    def _infer_collection_object_schema(self, property_name, obj_values):
        """Infer schema for collection fields that need the 'item' pattern."""
        # For known collection types, use templates
        return self._get_template_for_collection(property_name)
    
    def _get_template_for_collection(self, collection_name):
        """Get predefined template schema for common collection types."""
        if collection_name == "Emails":
            return {
                "properties": {
                    "item": {
                        "properties": {
                            "Address": {"type": ["string", "null"]},
                            "EmailType": {"type": ["string", "null"]}
                        },
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        elif collection_name == "Phones":
            return {
                "properties": {
                    "item": {
                        "properties": {
                            "Number": {"type": ["string", "null"]}
                        },
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        elif collection_name == "Addresses":
            return {
                "properties": {
                    "item": {
                        "properties": {
                            "Address": {
                                "properties": {
                                    "AddressId": {"type": ["string", "null"]},
                                    "CountryCode": {"type": ["string", "null"]},
                                    "CountryName": {"type": ["string", "null"]},
                                    "CountrySubEntityCode": {"type": ["string", "null"]},
                                    "CountrySubEntityName": {"type": ["string", "null"]},
                                    "FullAddress": {"type": ["string", "null"]}
                                },
                                "type": ["object", "null"]
                            },
                            "AddresseeText": {"type": ["string", "null"]},
                            "AddressPurpose": {"type": ["string", "null"]},
                            "FullAddressId": {"type": ["string", "null"]},
                            "DisplayOrganizationName": {"type": ["string", "null"]}
                        },
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        elif collection_name == "AlternateIds":
            return {
                "properties": {
                    "item": {
                        "properties": {
                            "Id": {"type": ["string", "null"]},
                            "IdType": {"type": ["string", "null"]}
                        },
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        elif collection_name == "SocialNetworks":
            return {
                "properties": {
                    "item": {
                        "properties": {
                            "PartySocialNetworkId": {"type": ["string", "null"]},
                            "SocialNetwork": {
                                "properties": {
                                    "SocialNetworkId": {"type": ["string", "null"]},
                                    "SocialNetworkName": {"type": ["string", "null"]},
                                    "BaseURL": {"type": ["string", "null"]}
                                },
                                "type": ["object", "null"]
                            },
                            "SocialNetworkUserName": {"type": ["string", "null"]},
                            "SocialNetworkUserId": {"type": ["string", "null"]},
                            "SocialNetworkProfileLinkURL": {"type": ["string", "null"]},
                            "SocialNetworkToken": {"type": ["string", "null"]},
                            "UseSocialNetworkProfilePhoto": {"type": ["boolean", "null"]}
                        },
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        elif collection_name == "CommunicationTypePreferences":
            return {
                "properties": {
                    "item": {
                        "type": ["string", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        elif collection_name == "Salutations":
            return {
                "properties": {
                    "item": {
                        "properties": {
                            "IsOverridden": {"type": ["boolean", "null"]},
                            "SalutationId": {"type": ["string", "null"]},
                            "SalutationMethod": {
                                "properties": {
                                    "PartySalutationMethodId": {"type": ["string", "null"]}
                                },
                                "type": ["object", "null"]
                            },
                            "Text": {"type": ["string", "null"]}
                        },
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
        else:
            # Generic item structure for other collections
            return {
                "properties": {
                    "item": {
                        "type": ["object", "null"]
                    }
                },
                "type": ["object", "null"]
            }
    
    def _process_name_value_collection(self, property_name, values):
        """Process a collection with Name-Value paired objects."""
        # For AdditionalAttributes and similar collections
        return {
            "properties": {},
            "type": ["object", "null"]
        }
    
    def _infer_array_schema(self, property_name, array_values):
        """Infer schema for an array property."""
        # Filter out None values and flatten items
        flat_items = []
        for arr in array_values:
            if isinstance(arr, list):
                flat_items.extend(arr[:min(5, len(arr))])
                
        if not flat_items:
            return {"type": ["array", "null"], "items": {"type": ["string", "null"]}}
            
        # Check for Name-Value pairs which should be converted to flat objects
        if all(isinstance(item, dict) for item in flat_items) and \
           all("Name" in item and "Value" in item for item in flat_items):
            # This is a Name-Value pair collection, process specially
            return self._process_name_value_collection(property_name, flat_items)
            
        # Determine item type based on samples
        item_types = set(type(item) for item in flat_items if item is not None)
        
        if bool in item_types:
            item_schema = {"type": ["boolean", "null"]}
        elif int in item_types and not any(t for t in item_types if t not in (int, type(None))):
            item_schema = {"type": ["integer", "null"]}
        elif any(t in item_types for t in (int, float)) and not any(t for t in item_types if t not in (int, float, type(None))):
            item_schema = {"type": ["number", "null"]}
        elif str in item_types and not any(t for t in item_types if t not in (str, type(None))):
            # Check if all strings are dates
            date_strings = [item for item in flat_items if isinstance(item, str)]
            if date_strings and all(self._is_date_string(s) for s in date_strings):
                item_schema = {"format": "date-time", "type": ["string", "null"]}
            else:
                item_schema = {"type": ["string", "null"]}
        elif dict in item_types:
            # For objects, we won't recurse to avoid deeply nested schemas
            item_schema = {"type": ["object", "null"]}
        else:
            # Default to string for mixed types
            item_schema = {"type": ["string", "null"]}
        
        return {
            "items": item_schema,
            "type": ["array", "null"]
        }
    
    def _is_date_string(self, string_val):
        """Check if a string is a date/datetime."""
        if not isinstance(string_val, str):
            return False
            
        try:
            parse(string_val)
            return True
        except:
            return False
    
    def _is_known_id_field(self, field_name):
        """Check if field is a known ID field that should be a string."""
        id_patterns = [
            r"Id$", r"ID$", 
            r"PartyId", r"UniformId", 
            r"^PartySalutationMethodId$", r"^SalutationId$", 
            r"^AddressId$", r"^FullAddressId$"
        ]
        
        return any(re.search(pattern, field_name) for pattern in id_patterns)
    
    def _is_known_boolean_field(self, field_name):
        """Check if field is a known boolean field."""
        boolean_patterns = [
            r"Is[A-Z]", r"^SortIsOverridden$", r"^IsMarkedForDelete$", 
            r"^IsDefault$", r"^IsOverridden$", r"^IsOverrideable$",
            r"^IsAnonymous$", r"^IsExempt$", r"^IsOngoing$", r"^IsPast$",
            r"RECURRING_REQUEST$"
        ]
        
        return any(re.search(pattern, field_name) for pattern in boolean_patterns)
    
    def _is_known_date_field(self, field_name):
        """Check if field is a known date field."""
        date_patterns = [
            r"Date$", r"^BirthDate$", r"^CreatedOn$", r"^UpdatedOn$",
            r"^EFFECTIVE_DATE$", r"^TRANSACTION_DATE$", r"^NEXT_INSTALL_DATE$",
            r"^THRU_DATE$", r"^TICKLER_DATE$", r"^UF_6$", r"^UF_7$",
            r"^JOINDATE$", r"^DeclarationReceived$", r"^Cancelled$", 
            r"^ConfirmationLetterSent$"
        ]
        
        return any(re.search(pattern, field_name) for pattern in date_patterns) 