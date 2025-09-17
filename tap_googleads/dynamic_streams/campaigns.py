"""CampaignsStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL, OrderBy
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignsStream(DynamicQueryStream):
    """Define custom stream."""

    @functools.cached_property
    def gaql(self) -> GAQL:
        """The GAQL query for the Campaigns stream."""
        return GAQL(
            "campaign.id",
            "campaign.name",
            "campaign.resource_name",
            from_table="campaign",
            order_by=[
                OrderBy("campaign.id"),
            ],
        )

    name = "campaign"
    primary_keys = ("campaign__id",)
    replication_key = None
