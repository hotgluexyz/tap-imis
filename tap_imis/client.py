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
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        schema = response.json().get("Properties").get("$values")
        properties = []
        for property in schema:
            schema_type = self.get_jsonschema_type(property)
            properties.append(th.Property(property["Name"], schema_type))
        return th.PropertiesList(*properties).to_dict()


   
    @cached_property
    def schema(self) -> dict:
        return self.get_schema()

