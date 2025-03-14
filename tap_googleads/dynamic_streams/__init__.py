"""Dynamic streams package for Google Ads tap."""

from tap_googleads.dynamic_streams.ad_groups import AdGroupsStream
from tap_googleads.dynamic_streams.ad_groups_performance import AdGroupsPerformance
from tap_googleads.dynamic_streams.campaign_performance_by_age_range_and_device import (
    CampaignPerformanceByAgeRangeAndDevice,
)
from tap_googleads.dynamic_streams.campaign_performance_by_gender_and_device import (
    CampaignPerformanceByGenderAndDevice,
)
from tap_googleads.dynamic_streams.campaign_performance_by_location import (
    CampaignPerformanceByLocation,
)
from tap_googleads.dynamic_streams.campaigns import CampaignsStream
from tap_googleads.dynamic_streams.click_view_report import ClickViewReportStream
from tap_googleads.dynamic_streams.geo_performance import GeoPerformance
from tap_googleads.dynamic_streams.geotargets import GeotargetsStream
from tap_googleads.dynamic_streams.campaign_performance import CampaignPerformance

__all__ = [
    "AdGroupsPerformance",
    "AdGroupsStream", 
    "CampaignPerformance",
    "CampaignPerformanceByAgeRangeAndDevice",
    "CampaignPerformanceByGenderAndDevice",
    "CampaignPerformanceByLocation",
    "CampaignsStream",
    "ClickViewReportStream",
    "GeoPerformance",
    "GeotargetsStream",
]
