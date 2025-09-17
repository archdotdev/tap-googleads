"""GeoPerformance for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class GeoPerformance(DynamicQueryStream):
    """Geo performance"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "campaign.name",
            "campaign.status",
            "segments.date",
            "metrics.clicks",
            "metrics.cost_micros",
            "metrics.impressions",
            "metrics.conversions",
            "geographic_view.location_type",
            "geographic_view.country_criterion_id",
            from_table="geographic_view",
            where_clause=[
                f"segments.date >= {self.start_date}",
                f"segments.date <= {self.end_date}",
            ],
        )

    name = "geo_performance"
    primary_keys = (
        "geographicView__countryCriterionId",
        "customer_id",
        "campaign__name",
        "segments__date",
    )
    replication_key = None
