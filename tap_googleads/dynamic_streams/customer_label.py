"""CustomerLabelStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CustomerLabelStream(DynamicQueryStream):
    """Customer Label stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "customer.id",
            "customer_label.resource_name",
            "customer_label.customer",
            "customer_label.label",
            from_table="customer_label",
        )

    name = "customer_label"
    primary_keys = ("customerLabel__resourceName",)
    replication_key = None
