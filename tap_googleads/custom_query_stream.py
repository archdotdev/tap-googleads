from __future__ import annotations

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CustomQueryStream(DynamicQueryStream):
    """Define custom stream."""

    records_jsonpath = "$.results[*]"
    primary_keys = []
    replication_key = None
    add_date_filter_to_query = True

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
        """
        self._custom_query = kwargs.pop("custom_query")
        self._gaql = self._custom_query["query"]
        self.name = self._custom_query["name"]
        super().__init__(*args, **kwargs)

    def add_date_filter(self, fields, has_where_clause, query):
        date_filter = (
            f"segments.date >= {self.start_date} and segments.date <= {self.end_date}"
        )
        if "segments.date" not in fields:
            fields.append("segments.date")
        if has_where_clause:
            self.gaql = self.gaql + f" AND {date_filter}"
        else:
            self.gaql = self.gaql + f" WHERE {date_filter}"

    @property
    def gaql(self):
        return self._gaql

    @gaql.setter
    def gaql(self, value):
        self._gaql = value
