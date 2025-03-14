from __future__ import annotations

import datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Dict, Iterable

from tap_googleads.client import ResumableAPIError
from tap_googleads.dynamic_query_stream import DynamicQueryStream

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class GeotargetsStream(DynamicQueryStream):
    """Geotargets, worldwide, constant across all customers"""

    gaql = """
    SELECT 
        geo_target_constant.canonical_name,
        geo_target_constant.country_code,
        geo_target_constant.id,
        geo_target_constant.name,
        geo_target_constant.status,
        geo_target_constant.target_type
    FROM geo_target_constant
    """
    name = "geo_target_constant"
    primary_keys = ["geoTargetConstant__id"]
    replication_key = None

    def get_records(self, context: Context) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from super().get_records(context)
        self.selected = False  # sync once only


class ClickViewReportStream(DynamicQueryStream):
    date: datetime.date

    @property
    def gaql(self):
        return f"""
        SELECT
            click_view.gclid
            , customer.id
            , click_view.ad_group_ad
            , ad_group.id
            , ad_group.name
            , campaign.id
            , campaign.name
            , segments.ad_network_type
            , segments.device
            , segments.date
            , segments.slot
            , metrics.clicks
            , segments.click_type
            , click_view.keyword
            , click_view.keyword_info.match_type
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'
        """

    name = "click_view_report"
    primary_keys = [
        "clickView__gclid",
        "clickView__keyword",
        "clickView__keywordInfo__matchType",
        "customer__id",
        "adGroup__id",
        "campaign__id",
        "segments__device",
        "segments__adNetworkType",
        "segments__slot",
        "date",
    ]
    replication_key = "date"

    def post_process(self, row, context):
        row["date"] = row["segments"].pop("date")

        if row.get("clickView", {}).get("keyword") is None:
            row["clickView"]["keyword"] = "null"
            row["clickView"]["keywordInfo"] = {"matchType": "null"}

        return super().post_process(row, context)

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.

        """
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        return params

    def request_records(self, context):
        start_value = self.get_starting_replication_key_value(context)

        start_date = datetime.date.fromisoformat(start_value)
        end_date = datetime.date.fromisoformat(self.config["end_date"])

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            records = list(super().request_records(context))

            if not records:
                self._increment_stream_state(
                    {"date": self.date.isoformat()}, context=self.context
                )

            yield from records

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            error = response.json()["error"]["details"][0]["errors"][0]
            msg = (
                "Click view report not accessible to customer "
                f"'{self.context['customer_id']}': {error['message']}"
            )
            raise ResumableAPIError(msg, response)

        super().validate_response(response)


class CampaignsStream(DynamicQueryStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
        SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id
        """

    name = "campaign"
    primary_keys = ["campaign__id"]
    replication_key = None


class AdGroupsStream(DynamicQueryStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
       SELECT ad_group.url_custom_parameters, 
       ad_group.type, 
       ad_group.tracking_url_template, 
       ad_group.targeting_setting.target_restrictions,
       ad_group.target_roas,
       ad_group.target_cpm_micros,
       ad_group.status,
       ad_group.target_cpa_micros,
       ad_group.resource_name,
       ad_group.percent_cpc_bid_micros,
       ad_group.name,
       ad_group.labels,
       ad_group.id,
       ad_group.final_url_suffix,
       ad_group.excluded_parent_asset_field_types,
       ad_group.effective_target_roas_source,
       ad_group.effective_target_roas,
       ad_group.effective_target_cpa_source,
       ad_group.effective_target_cpa_micros,
       ad_group.display_custom_bid_dimension,
       ad_group.cpv_bid_micros,
       ad_group.cpm_bid_micros,
       ad_group.cpc_bid_micros,
       ad_group.campaign,
       ad_group.base_ad_group,
       ad_group.ad_rotation_mode
       FROM ad_group 
       """

    name = "adgroups"
    primary_keys = ["adGroup__id", "adGroup__campaign", "adGroup__status"]
    replication_key = None


class AdGroupsPerformance(DynamicQueryStream):
    """AdGroups Performance"""

    @property
    def gaql(self):
        return f"""
        SELECT campaign.id, ad_group.id, metrics.impressions, metrics.clicks,
               metrics.cost_micros
               FROM ad_group
               WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
        """

    name = "adgroupsperformance"
    primary_keys = ["campaign__id", "adGroup__id"]
    replication_key = None


class CampaignPerformance(DynamicQueryStream):
    """Campaign Performance"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign.name, campaign.status, segments.device, segments.date, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM campaign WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    name = "campaign_performance"
    primary_keys = [
        "campaign__name",
        "campaign__status",
        "segments__date",
        "segments__device",
    ]
    replication_key = None


class CampaignPerformanceByAgeRangeAndDevice(DynamicQueryStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT ad_group_criterion.age_range.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM age_range_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    name = "campaign_performance_by_age_range_and_device"
    primary_keys = [
        "adGroupCriterion__ageRange__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None


class CampaignPerformanceByGenderAndDevice(DynamicQueryStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT ad_group_criterion.gender.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM gender_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    name = "campaign_performance_by_gender_and_device"
    primary_keys = [
        "adGroupCriterion__gender__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    replication_key = None


class CampaignPerformanceByLocation(DynamicQueryStream):
    """Campaign Performance By Age Range and Device"""

    @property
    def gaql(self):
        return f"""
    SELECT campaign_criterion.location.geo_target_constant, campaign.name, campaign_criterion.bid_modifier, segments.date, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros FROM location_view WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date} AND campaign_criterion.status != 'REMOVED'
    """

    name = "campaign_performance_by_location"
    primary_keys = [
        "campaignCriterion__location__geoTargetConstant",
        "campaign__name",
        "segments__date",
    ]
    replication_key = None


class GeoPerformance(DynamicQueryStream):
    """Geo performance"""

    @property
    def gaql(self):
        return f"""
    SELECT 
        campaign.name, 
        campaign.status, 
        segments.date, 
        metrics.clicks, 
        metrics.cost_micros,
        metrics.impressions, 
        metrics.conversions,
        geographic_view.location_type,
        geographic_view.country_criterion_id
    FROM geographic_view 
    WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date} 
    """

    name = "geo_performance"
    primary_keys = [
        "geographicView__countryCriterionId",
        "customer_id",
        "campaign__name",
        "segments__date",
    ]
    replication_key = None
