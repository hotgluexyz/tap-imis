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

        schema = self._analyze_records(records)
        return schema

    def _analyze_records(self, records):
        """Analyze records to create schema that resembles the metadata schema."""
        if not records or not isinstance(records[0], dict):
            return {}
            
        all_properties = {}
        
        # First pass: Collect all unique keys and their potential types
        for record in records:
            for key, value in record.items():
                if key == "$type":
                    continue
                    
                if key not in all_properties:
                    all_properties[key] = {"types": set(), "examples": []}
                
                # Track the type (even if None)
                all_properties[key]["types"].add(type(value))
                
                # Store examples for type inference
                if value is not None and len(all_properties[key]["examples"]) < 5:
                    all_properties[key]["examples"].append(value)
        
        # Second pass: Generate schema properties based on observed values
        properties = []
        for key, info in all_properties.items():
            # Special case for known IMIS ID fields (ensure consistent type)
            if key in ("Id", "PartyId", "UniformId") or key.endswith("Id"):
                properties.append(th.Property(key, th.StringType()))
            else:
                properties.append(th.Property(key, self._infer_property_type(key, info)))
        
        return th.PropertiesList(*properties).to_dict()
        
    def _infer_property_type(self, property_name, property_info):
        """Infer property type from collected examples."""
        types = property_info["types"]
        examples = property_info["examples"]
        
        # Handle the case where we only have None values
        if len(types) == 1 and None.__class__ in types:
            return th.StringType()
            
        # Remove None type for further analysis
        actual_types = types - {None.__class__} if None.__class__ in types else types
        
        if not actual_types:
            return th.StringType()
        
        # Special case handling for known field patterns
        if self._is_known_id_field(property_name):
            return th.StringType()
        
        if self._is_known_boolean_field(property_name):
            return th.BooleanType()
            
        if self._is_known_date_field(property_name):
            return th.DateTimeType()
            
        # Check for datetime strings
        if str in actual_types and examples:
            for example in examples:
                if isinstance(example, str):
                    try:
                        parse(example)
                        return th.DateTimeType()
                    except:
                        # Not all string examples need to be dates
                        pass
        
        # Handle IMIS special object structure with $values for arrays
        if dict in actual_types:
            # Check if this is a common IMIS pattern with 'item' property
            for example in examples:
                if isinstance(example, dict):
                    # IMIS-specific pattern: object with single 'item' key containing the actual data
                    if len(example) == 1 and 'item' in example:
                        item_value = example['item']
                        if isinstance(item_value, dict):
                            # Recursively analyze the item's properties
                            nested_props = {}
                            for k, v in item_value.items():
                                if k != "$type":
                                    nested_props[k] = {"types": {type(v)}, "examples": [v] if v is not None else []}
                            
                            obj_properties = []
                            for k, v_info in nested_props.items():
                                obj_properties.append(th.Property(k, self._infer_property_type(k, v_info)))
                            
                            if obj_properties:
                                # Structure: { "item": { ... properties ... } }
                                item_prop = th.Property("item", th.ObjectType(*obj_properties))
                                return th.ObjectType(item_prop)
                        elif isinstance(item_value, list):
                            # Structure: { "item": [ ... array items ... ] }
                            item_type = self._determine_array_item_type(item_value)
                            item_prop = th.Property("item", th.ArrayType(item_type))
                            return th.ObjectType(item_prop)
                    
                    # Check for $values - represents an array in IMIS
                    if "$values" in example:
                        values = example.get("$values", [])
                        if values and isinstance(values, list) and values:
                            # Analyze the first few items to determine array item type
                            array_items = values[:min(5, len(values))]
                            item_type = self._determine_array_item_type(array_items)
                            return th.ArrayType(item_type)
                    
                    # Handle regular object with properties
                    if example and any(k != "$type" for k in example.keys()):
                        nested_props = {}
                        for k, v in example.items():
                            if k != "$type":
                                nested_props[k] = {"types": {type(v)}, "examples": [v] if v is not None else []}
                        
                        obj_properties = []
                        for k, v_info in nested_props.items():
                            # Apply special case detection for nested properties too
                            if self._is_known_id_field(k):
                                obj_properties.append(th.Property(k, th.StringType()))
                            elif self._is_known_boolean_field(k):
                                obj_properties.append(th.Property(k, th.BooleanType()))
                            elif self._is_known_date_field(k):
                                obj_properties.append(th.Property(k, th.DateTimeType()))
                            else:
                                obj_properties.append(th.Property(k, self._infer_property_type(k, v_info)))
                        
                        # If we have nested properties, create an object with those properties
                        if obj_properties:
                            return th.ObjectType(*obj_properties)
            
            # IMIS often represents mixed types with a custom type
            if self._has_mixed_value_types(examples):
                return th.CustomType({"type": ["number", "string", "object"]})
                
            # Default object with no determined properties
            return th.ObjectType()
            
        # Handle arrays (non-IMIS format)
        if list in actual_types:
            for example in examples:
                if isinstance(example, list) and example:
                    # Sample up to 5 items to determine type
                    array_items = example[:min(5, len(example))]
                    item_type = self._determine_array_item_type(array_items)
                    return th.ArrayType(item_type)
            
            # Empty arrays or no examples - default to array of strings
            return th.ArrayType(th.StringType())
            
        # Handle primitive types
        if bool in actual_types:
            return th.BooleanType()
        if int in actual_types:
            return th.IntegerType()
        if float in actual_types:
            return th.NumberType()
        if str in actual_types:
            return th.StringType()
            
        # Fallback to custom type for complex cases
        return th.CustomType({"type": ["number", "string", "object"]})
        
    def _determine_array_item_type(self, items):
        """Determine the type of elements in an array."""
        if not items:
            return th.StringType()
            
        # Analyze item types
        item_types = set(type(item) for item in items)
        
        # If all items are the same type, use that type
        if len(item_types) == 1:
            item_type = next(iter(item_types))
            if item_type == dict:
                # For objects, analyze the first few to determine common properties
                nested_props = {}
                for item in items[:3]:  # Sample first 3 items
                    for k, v in item.items():
                        if k != "$type":
                            if k not in nested_props:
                                nested_props[k] = {"types": set(), "examples": []}
                            nested_props[k]["types"].add(type(v))
                            if v is not None and len(nested_props[k]["examples"]) < 3:
                                nested_props[k]["examples"].append(v)
                                
                obj_properties = []
                for k, v_info in nested_props.items():
                    # Apply special case detection for nested properties too
                    if self._is_known_id_field(k):
                        obj_properties.append(th.Property(k, th.StringType()))
                    elif self._is_known_boolean_field(k):
                        obj_properties.append(th.Property(k, th.BooleanType()))
                    elif self._is_known_date_field(k):
                        obj_properties.append(th.Property(k, th.DateTimeType()))
                    else:
                        obj_properties.append(th.Property(k, self._infer_property_type(k, v_info)))
                
                return th.ObjectType(*obj_properties) if obj_properties else th.ObjectType()
            elif item_type == bool:
                return th.BooleanType()
            elif item_type == int:
                return th.IntegerType()
            elif item_type == float:
                return th.NumberType()
            elif item_type == str:
                # Check if all strings are dates
                all_dates = True
                for item in items[:3]:  # Check first 3 items
                    if isinstance(item, str):
                        try:
                            parse(item)
                        except:
                            all_dates = False
                            break
                
                return th.DateTimeType() if all_dates else th.StringType()
            else:
                return th.StringType()
        
        # Mixed types - analyze more carefully
        if dict in item_types:
            return th.ObjectType()  # If any items are objects, treat as array of objects
        if any(t in item_types for t in (int, float)):
            return th.NumberType()  # If any numbers, use number type
        
        # Default to string for most mixed cases
        return th.StringType()
    
    def _is_known_id_field(self, field_name):
        """Check if field is a known ID field that should be a string."""
        id_patterns = [
            "Id$", "ID$", 
            "PartyId", "UniformId", 
            "^PartySalutationMethodId$", "^SalutationId$", 
            "^AddressId$", "^FullAddressId$"
        ]
        
        return any(re.search(pattern, field_name) for pattern in id_patterns)
    
    def _is_known_boolean_field(self, field_name):
        """Check if field is a known boolean field."""
        boolean_patterns = [
            "Is[A-Z]", "^SortIsOverridden$", "^IsMarkedForDelete$", 
            "^IsDefault$", "^IsOverridden$", "^IsOverrideable$",
            "^IsAnonymous$", "^IsExempt$", "^IsOngoing$", "^IsPast$",
            "RECURRING_REQUEST$"
        ]
        
        return any(re.search(pattern, field_name) for pattern in boolean_patterns)
    
    def _is_known_date_field(self, field_name):
        """Check if field is a known date field."""
        date_patterns = [
            "Date$", "^BirthDate$", "^CreatedOn$", "^UpdatedOn$",
            "^EFFECTIVE_DATE$", "^TRANSACTION_DATE$", "^NEXT_INSTALL_DATE$",
            "^THRU_DATE$", "^TICKLER_DATE$", "^UF_6$", "^UF_7$",
            "^JOINDATE$", "^DeclarationReceived$", "^Cancelled$", 
            "^ConfirmationLetterSent$"
        ]
        
        return any(re.search(pattern, field_name) for pattern in date_patterns)
    
    def _has_mixed_value_types(self, examples):
        """Check if a field has mixed value types that would need a custom type."""
        value_types = set()
        for example in examples:
            if isinstance(example, dict) and "$value" in example:
                value_types.add(type(example["$value"]))
        
        # If we have multiple value types, this is likely a field that
        # IMIS represents with a mixed type
        return len(value_types) > 1 