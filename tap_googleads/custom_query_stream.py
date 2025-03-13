from functools import cached_property
from types import SimpleNamespace
from typing import Any, List, Mapping

import requests
import sqlparse

from tap_googleads.streams import ReportsStream

DATE_TYPES = ("segments.date", "segments.month", "segments.quarter", "segments.week")


class CustomQueryStream(ReportsStream):
    """Define custom stream."""

    records_jsonpath = "$.results[*]"
    primary_keys = []
    replication_key = None

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
        """
        self.custom_query = kwargs.pop("custom_query")
        self._query = self.custom_query["query"]
        self.name = self.custom_query["name"]
        super().__init__(*args, **kwargs)

    @property
    def gaql(self):
        return self._query

    @cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        local_json_schema = {
            "type": "object",
            "properties": {},
            "additionalProperties": True,
        }

        google_datatype_mapping = {
            "STRING": "string",
            "MESSAGE": "string",
            "BOOLEAN": "boolean",
            "DATE": "string",
            "ENUM": "string",
            "INT64": "integer",
            "INT32": "integer",
            "DOUBLE": "number",
        }
        try:
            query_object = sqlparse.parse(self.custom_query["query"])[0]
        except ValueError:
            message = f"The custom GAQL query {self.custom_query['name']} failed. Validate your GAQL query with the Google Ads query validator. https://developers.google.com/google-ads/api/fields/v19/query_validator"
            raise Exception(message)

        fields = []
        has_where_clause = False
        for token in query_object.tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                fields = [field.strip() for field in token.value.split(",")]
            if isinstance(token, sqlparse.sql.Where):
                has_where_clause = True

        date_filter = (
            f"segments.date >= {self.start_date} and segments.date <= {self.end_date}"
        )
        if "segments.date" not in fields:
            fields.append("segments.date")
        if has_where_clause:
            self._query = self._query + f" AND {date_filter}"
        else:
            self._query = self._query + f" WHERE {date_filter}"

        google_schema = self.get_fields_metadata(fields)

        for field in fields:
            node = google_schema.get(field)
            google_data_type = node.data_type
            field_value = {
                "type": [
                    google_datatype_mapping.get(google_data_type, "string"),
                    "null",
                ]
            }

            if google_data_type == "DATE" and field in DATE_TYPES:
                field_value["format"] = "date"

            if google_data_type == "ENUM":
                field_value = {"type": "string", "enum": list(node.enum_values)}

            if node.is_repeated:
                field_value = {"type": ["null", "array"], "items": field_value}
            local_json_schema["properties"][field] = field_value

        return local_json_schema

    def get_fields_metadata(self, fields: List[str]) -> Mapping[str, Any]:
        """
        Get field metadata for custom query columns.

        Issue Google API request to get detailed information on data type for custom query columns.
        Uses direct REST API calls.

        Args:
            fields: List of columns for user defined query.

        Returns:
            dict: Field metadata for custom query columns.
        """
        base_url = f"{self.url_base}/googleAdsFields:search"

        fields_sql = ",".join([f"'{field}'" for field in fields])
        query = f"""
        SELECT
          name,
          data_type,
          enum_values,
          is_repeated
        WHERE name in ({fields_sql})
        """

        payload = {"query": query, "pageSize": len(fields)}

        self.authenticator.update_access_token()
        headers = {
            "Authorization": f"Bearer {self.authenticator.access_token}",
            "Content-Type": "application/json",
            "developer-token": self.config["developer_token"],
        }
        response = requests.post(base_url, json=payload, headers=headers)
        response.raise_for_status()

        response_data = response.json()

        result = {}
        for item in response_data.get("results", []):
            field = SimpleNamespace()
            field.name = item.get("name")
            field.data_type = item.get("dataType", "")
            field.enum_values = item.get("enumValues", [])
            field.is_repeated = item.get("isRepeated", False)
            result[field.name] = field

        return result
