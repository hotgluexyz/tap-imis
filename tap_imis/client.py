"""REST client handling, including ActionKitStream base class."""

from datetime import datetime

from functools import cached_property
import typing as t
import requests
from requests.auth import _basic_auth_str
from singer_sdk.streams import RESTStream
from singer_sdk import typing as th
from pendulum import parse
from tap_imis.auth import IMISAuth

class IMISStream(RESTStream):
    """IMIS stream class."""

    access_token = None

    records_jsonpath = "$.Items[*]"
    

    @property
    def url_base(self):
        return f"{self.config.get('site_url')}/api/"

    def get_new_access_token(self):
        auth = IMISAuth(self.config)
        return auth.get_token()

    def get_access_token(self):
        if self.access_token is None:
            self.access_token = self.get_new_access_token()
        return self.access_token
    
    def get_jsonschema_type(self, property):
        type_name = property["PropertyTypeName"]

        if type_name == "String":
            return th.StringType()
        if type_name == "Boolean":
            return th.BooleanType()
        if type_name == "Date":
            return th.DateTimeType()
        if type_name == "Integer":
            return th.IntegerType()
        if type_name == "EntityDefinitionData":
            if property.get("ItemEntityPropertyDefinition"):
                item_property = property.get("ItemEntityPropertyDefinition")
                obj_props = [
                    th.Property(item_property["Name"], self.get_jsonschema_type(item_property))
                ]
                return th.ObjectType(*obj_props)

            entity_properties = property.get("EntityDefinition").get("Properties").get("$values")
            obj_props = []
            for entity_property in entity_properties:
                obj_props.append(th.Property(entity_property["Name"], self.get_jsonschema_type(entity_property)))
            return th.ObjectType(*obj_props)
        if type_name == "GenericPropertyDataCollection":
            generic_properties = property.get("GenericPropertyDefinitions").get("$values")
            obj_props = []
            for generic_property in generic_properties:
                obj_props.append(th.Property(generic_property["Name"], self.get_jsonschema_type(generic_property)))
            return th.ObjectType(*obj_props)
        else:
            return th.CustomType({"type": ["number", "string", "object"]})

 
    def get_schema(self) -> dict:
        url = f"{self.url_base}/metadata{self.path}"
        headers = {"Authorization": f"Bearer {self.get_access_token()}"}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            schema = response.json().get("Properties").get("$values")
            properties = []
            for property in schema:
                schema_type = self.get_jsonschema_type(property)
                properties.append(th.Property(property["Name"], schema_type))
            return th.PropertiesList(*properties).to_dict()
        except requests.exceptions.HTTPError as e:
            # Check if we got a 501 Not Implemented error
            if e.response.status_code == 501:
                self.logger.warning(f"Metadata endpoint returned 501 for {self.path}. Falling back to record inference.")
                return self._infer_schema_from_records()
            # Re-raise if it's a different error
            raise

    def _infer_schema_from_records(self) -> dict:
        """Fetch sample records and infer schema from them."""
        url = f"{self.url_base}{self.path}"
        headers = {"Authorization": f"Bearer {self.get_access_token()}"}
        
        params = {"limit": 500}  
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        json_response = response.json()
        items = json_response.get("Items", {})
        
        if isinstance(items, dict) and "$values" in items:
            records = items.get("$values", [])
        else:
            records = items if isinstance(items, list) else []
            
        if not records:
            self.logger.warning(f"No records found for {self.path}. Using empty schema.")
            return {}

        properties_map = {}
        
        for record in records:
            if not isinstance(record, dict):
                continue
                
            for key, value in record.items():
                if key == "$type":
                    continue
                    
                if key not in properties_map:
                    properties_map[key] = {"types": set(), "examples": []}
                
                properties_map[key]["types"].add(type(value))
                
                if value is not None and len(properties_map[key]["examples"]) < 3:
                    properties_map[key]["examples"].append(value)

        properties = []
        for key, info in properties_map.items():
            if len(info["types"]) == 1 and None.__class__ in info["types"]:
                properties.append(th.Property(key, th.StringType()))
                continue
                
            types = info["types"] - {None.__class__} if None.__class__ in info["types"] else info["types"]
            
            if not types:
                properties.append(th.Property(key, th.StringType()))
                continue
                
            examples = info["examples"]
            
            # Check for dict with $values - represents array
            if dict in types and examples and any("$values" in ex for ex in examples if isinstance(ex, dict)):
                # Find an example with $values
                array_example = next((ex for ex in examples if isinstance(ex, dict) and "$values" in ex), None)
                if array_example and array_example["$values"]:
                    values = array_example["$values"]
                    if values and isinstance(values, list) and len(values) > 0:
                        first_item = values[0]
                        if isinstance(first_item, dict):
                            properties.append(th.Property(key, th.ArrayType(th.ObjectType())))
                        elif isinstance(first_item, str):
                            properties.append(th.Property(key, th.ArrayType(th.StringType())))
                        elif isinstance(first_item, int):
                            properties.append(th.Property(key, th.ArrayType(th.IntegerType())))
                        elif isinstance(first_item, bool):
                            properties.append(th.Property(key, th.ArrayType(th.BooleanType())))
                        elif isinstance(first_item, float):
                            properties.append(th.Property(key, th.ArrayType(th.NumberType())))
                        else:
                            properties.append(th.Property(key, th.ArrayType(th.StringType())))
                    else:
                        properties.append(th.Property(key, th.ArrayType(th.StringType())))
                else:
                    properties.append(th.Property(key, th.ObjectType()))
            # Regular dict - represents object
            elif dict in types:
                properties.append(th.Property(key, th.ObjectType()))
            # List - represents array
            elif list in types:
                # Try to determine array item type from examples
                if examples:
                    list_example = next((ex for ex in examples if isinstance(ex, list)), [])
                    if list_example and len(list_example) > 0:
                        first_item = list_example[0]
                        if isinstance(first_item, dict):
                            properties.append(th.Property(key, th.ArrayType(th.ObjectType())))
                        elif isinstance(first_item, str):
                            properties.append(th.Property(key, th.ArrayType(th.StringType())))
                        elif isinstance(first_item, int):
                            properties.append(th.Property(key, th.ArrayType(th.IntegerType())))
                        elif isinstance(first_item, bool):
                            properties.append(th.Property(key, th.ArrayType(th.BooleanType())))
                        elif isinstance(first_item, float):
                            properties.append(th.Property(key, th.ArrayType(th.NumberType())))
                        else:
                            properties.append(th.Property(key, th.ArrayType(th.StringType())))
                    else:
                        properties.append(th.Property(key, th.ArrayType(th.StringType())))
                else:
                    properties.append(th.Property(key, th.ArrayType(th.StringType())))
            # String - check if it's a date/datetime
            elif str in types:
                is_datetime = False
                for example in examples:
                    if isinstance(example, str):
                        try:
                            parse(example)
                            is_datetime = True
                            break
                        except:
                            pass
                if is_datetime:
                    properties.append(th.Property(key, th.DateTimeType()))
                else:
                    properties.append(th.Property(key, th.StringType()))
            # Other primitive types
            elif bool in types:
                properties.append(th.Property(key, th.BooleanType()))
            elif int in types:
                properties.append(th.Property(key, th.IntegerType()))
            elif float in types:
                properties.append(th.Property(key, th.NumberType()))
            else:
                # Fallback for unknown types
                properties.append(th.Property(key, th.StringType()))
        
        return th.PropertiesList(*properties).to_dict()

   
    @cached_property
    def schema(self) -> dict:
        return self.get_schema()

