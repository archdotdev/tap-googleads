"""AudienceStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AudienceStream(DynamicQueryStream):
    """Audience stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "customer.id",
            "customer.resource_name",
            "audience.description",
            "audience.dimensions",
            "audience.exclusion_dimension",
            "audience.id",
            "audience.name",
            "audience.resource_name",
            "audience.status",
            from_table="audience",
        )

    name = "audience"
    primary_keys = ("customer__id", "audience__id")
    replication_key = None
