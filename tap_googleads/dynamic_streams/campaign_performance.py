"""CampaignPerformance for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformance(DynamicQueryStream):
    """Campaign Performance."""

    @functools.cached_property
    def gaql(self) -> GAQL:
        """The GAQL query for the CampaignPerformance stream."""
        return GAQL(
            "campaign.name",
            "campaign.status",
            "campaign.resource_name",
            "segments.device",
            "segments.date",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            from_table="campaign",
            where_clause=[
                f"segments.date >= {self.start_date}",
                f"segments.date <= {self.end_date}",
            ],
        )

    name = "campaign_performance"
    primary_keys = (
        "campaign__name",
        "campaign__status",
        "segments__date",
        "segments__device",
    )
    replication_key = None
