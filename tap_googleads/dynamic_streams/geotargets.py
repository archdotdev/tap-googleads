"""GeotargetsStream for Google Ads tap."""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Iterable

from tap_googleads._gaql import GAQL
from tap_googleads.dynamic_query_stream import DynamicQueryStream

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class GeotargetsStream(DynamicQueryStream):
    """Geotargets, worldwide, constant across all customers"""

    @functools.cached_property
    def gaql(self) -> GAQL:
        return GAQL(
            "geo_target_constant.canonical_name",
            "geo_target_constant.country_code",
            "geo_target_constant.id",
            "geo_target_constant.name",
            "geo_target_constant.status",
            "geo_target_constant.target_type",
            "geo_target_constant.resource_name",
            from_table="geo_target_constant",
        )

    name = "geo_target_constant"
    primary_keys = ("geoTargetConstant__id",)
    replication_key = None

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from super().get_records(context)
        self.selected = False  # sync once only
