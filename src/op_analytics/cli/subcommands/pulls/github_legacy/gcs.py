from .allrepos import GithubAnalyticsData


def write_to_gcs(data: GithubAnalyticsData):
    """Write to BigQuery.

    This operation will be retired soon. Data will move to Clickhouse.
    """
    pass
