"""CampaignPerformanceByGenderAndDevice for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class CampaignPerformanceByGenderAndDevice(DynamicQueryStream):
    """Campaign Performance By Gender and Device"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "ad_group_criterion.gender.type",
            "campaign.name",
            "campaign.status",
            "ad_group.name",
            "segments.date",
            "segments.device",
            "ad_group_criterion.system_serving_status",
            "ad_group_criterion.bid_modifier",
            "metrics.clicks",
            "metrics.impressions",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            "campaign.advertising_channel_type",
            from_table="gender_view",
            where_clause=[
                f"segments.date >= {self.start_date}",
                f"segments.date <= {self.end_date}",
            ],
        )

    name = "campaign_performance_by_gender_and_device"
    primary_keys = (
        "adGroupCriterion__gender__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    )
    replication_key = None
