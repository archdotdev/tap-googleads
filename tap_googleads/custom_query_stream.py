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

    @property
    def gaql(self):
        return self._gaql

    @gaql.setter
    def gaql(self, value):
        self._gaql = value
