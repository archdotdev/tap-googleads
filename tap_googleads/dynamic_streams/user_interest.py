"""UserInterestStream for Google Ads tap."""

import functools

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream


class UserInterestStream(DynamicQueryStream):
    """User Interest stream"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "user_interest.availabilities",
            "user_interest.launched_to_all",
            "user_interest.name",
            "user_interest.resource_name",
            "user_interest.taxonomy_type",
            "user_interest.user_interest_id",
            "user_interest.user_interest_parent",
            from_table="user_interest",
        )

    name = "user_interest"
    primary_keys = ("userInterest__userInterestId",)
    replication_key = None
