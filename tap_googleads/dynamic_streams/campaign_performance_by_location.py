"""CampaignPerformanceByLocation for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformanceByLocation(DynamicQueryStream):
    """Campaign Performance By Location"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "campaign_criterion.location.geo_target_constant",
            "campaign.name",
            "campaign_criterion.bid_modifier",
            "segments.date",
            "metrics.clicks",
            "metrics.impressions",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            from_table="location_view",
            where_clause=[
                f"segments.date >= {self.start_date}",
                f"segments.date <= {self.end_date}",
                "campaign_criterion.status != 'REMOVED'",
            ],
        )

    name = "campaign_performance_by_location"
    primary_keys = (
        "campaignCriterion__location__geoTargetConstant",
        "campaign__name",
        "segments__date",
    )
    replication_key = None
