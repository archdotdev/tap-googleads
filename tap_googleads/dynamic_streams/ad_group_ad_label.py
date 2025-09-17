"""AdGroupAdLabelStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupAdLabelStream(DynamicQueryStream):
    """Ad Group Ad Label stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "ad_group.id",
            "ad_group_ad.ad.id",
            "ad_group_ad.ad.resource_name",
            "ad_group_ad_label.resource_name",
            "label.name",
            "label.resource_name",
            "label.id",
            from_table="ad_group_ad_label",
        )

    name = "ad_group_ad_label"
    primary_keys = ("adGroup__id", "adGroupAd__ad__id", "label__id")
    replication_key = None
