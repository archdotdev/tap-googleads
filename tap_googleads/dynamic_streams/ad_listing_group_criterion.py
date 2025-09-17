"""AdListingGroupCriterionStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdListingGroupCriterionStream(DynamicQueryStream):
    """Ad Listing Group Criterion stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "ad_group_criterion.resource_name",
            "ad_group.id",
            "ad_group_criterion.criterion_id",
            "ad_group_criterion.listing_group.case_value.activity_country.value",
            "ad_group_criterion.listing_group.case_value.activity_id.value",
            "ad_group_criterion.listing_group.case_value.activity_rating.value",
            "ad_group_criterion.listing_group.case_value.hotel_city.city_criterion",
            "ad_group_criterion.listing_group.case_value.hotel_class.value",
            "ad_group_criterion.listing_group.case_value.hotel_country_region.country_region_criterion",
            "ad_group_criterion.listing_group.case_value.hotel_id.value",
            "ad_group_criterion.listing_group.case_value.hotel_state.state_criterion",
            "ad_group_criterion.listing_group.case_value.product_category.category_id",
            "ad_group_criterion.listing_group.case_value.product_category.level",
            "ad_group_criterion.listing_group.case_value.product_brand.value",
            "ad_group_criterion.listing_group.case_value.product_channel.channel",
            "ad_group_criterion.listing_group.case_value.product_channel_exclusivity.channel_exclusivity",
            "ad_group_criterion.listing_group.case_value.product_condition.condition",
            "ad_group_criterion.listing_group.case_value.product_custom_attribute.index",
            "ad_group_criterion.listing_group.case_value.product_custom_attribute.value",
            "ad_group_criterion.listing_group.case_value.product_item_id.value",
            "ad_group_criterion.listing_group.case_value.product_type.level",
            "ad_group_criterion.listing_group.case_value.product_type.value",
            "ad_group_criterion.listing_group.parent_ad_group_criterion",
            "ad_group_criterion.listing_group.type",
            from_table="ad_group_criterion",
            where_clause=[
                "ad_group_criterion.type = 'LISTING_GROUP'",
            ],
        )

    name = "ad_listing_group_criterion"
    primary_keys = ("adGroup__id", "adGroupCriterion__criterionId")
    replication_key = None
