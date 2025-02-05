from op_analytics.datasources.growthepie.dataaccess import GrowThePie


def test_properties():
    assert GrowThePie.FUNDAMENTALS_SUMMARY.db == "growthepie"
    assert GrowThePie.FUNDAMENTALS_SUMMARY.table == "chains_daily_fundamentals_v1"
    assert GrowThePie.FUNDAMENTALS_SUMMARY.root_path == "growthepie/chains_daily_fundamentals_v1"
