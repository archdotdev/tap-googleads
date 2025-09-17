from tap_googleads._gaql import GAQL, OrderBy


def test_gaql():
    gaql = GAQL(
        "campaign.id",
        "campaign.name",
        from_table="campaign",
        order_by=[
            OrderBy("campaign.id"),
        ],
    )
    assert str(gaql) == "SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id"


def test_gaql_with_where_clause():
    gaql = GAQL(
        "campaign.name",
        "campaign.status",
        "segments.device",
        "segments.date",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.ctr",
        "metrics.average_cpc",
        "metrics.cost_micros",
        from_table="campaign",
        where_clause=[
            "segments.date >= '2025-01-01'",
            "segments.date <= '2025-02-01'",
        ],
    )
    assert str(gaql) == (
        "SELECT "
        "campaign.name, campaign.status, "
        "segments.device, segments.date, "
        "metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc, metrics.cost_micros "
        "FROM campaign "
        "WHERE segments.date >= '2025-01-01' AND segments.date <= '2025-02-01'"
    )


def test_gaql_from_string():
    gaql = GAQL.from_string("SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id")
    assert str(gaql) == "SELECT campaign.id, campaign.name FROM campaign ORDER BY campaign.id"
