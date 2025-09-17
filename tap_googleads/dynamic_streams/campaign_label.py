"""CampaignLabelStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignLabelStream(DynamicQueryStream):
    """Campaign Label stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "campaign.id",
            "label.id",
            "campaign.resource_name",
            "campaign_label.resource_name",
            "label.name",
            "label.resource_name",
            from_table="campaign_label",
        )

    name = "campaign_label"
    primary_keys = ("campaign__id", "label__id")
    replication_key = None
