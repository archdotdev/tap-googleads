"""AdGroupLabelStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupLabelStream(DynamicQueryStream):
    """Ad Group Label stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "ad_group.id",
            "label.id",
            "ad_group.resource_name",
            "ad_group_label.resource_name",
            "label.name",
            "label.resource_name",
            from_table="ad_group_label",
        )

    name = "ad_group_label"
    primary_keys = ("adGroup__id", "label__id")
    replication_key = None
