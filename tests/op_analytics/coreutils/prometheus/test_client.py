"""Tests for Prometheus client and data source functionality."""

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
from op_analytics.coreutils.prometheus.client import PrometheusClient
from op_analytics.datasources.eng.prometheus import PrometheusDataSource


@pytest.fixture
def mock_prometheus_response():
    """Fixture providing mock Prometheus API responses."""
    return {
        "instant": {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {"instance": "test-instance", "job": "test-job"},
                        "value": [1746694184, "13.255406195207481"],
                    }
                ],
            },
        },
        "range": {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"instance": "test-instance", "job": "test-job"},
                        "values": [[1746690584, "0"], [1746690884, "3.75"], [1746691184, "2.5"]],
                    }
                ],
            },
        },
    }


@pytest.fixture
def prometheus_client():
    """Fixture providing a PrometheusClient instance with test credentials."""
    return PrometheusClient(
        username="test_user",
        password="test_pass",
        base_url="https://test-prometheus.example.com/api/v1",
    )


@pytest.fixture
def prometheus_datasource(prometheus_client):
    """Fixture providing a PrometheusDataSource instance."""
    return PrometheusDataSource(client=prometheus_client)


class TestPrometheusClient:
    """Test suite for PrometheusClient."""

    def test_init_with_credentials(self):
        """Test client initialization with credentials."""
        client = PrometheusClient(
            username="test",
            password="test",
            base_url="https://test.example.com",
        )
        assert client.username == "test"
        assert client.password == "test"
        assert client.base_url == "https://test.example.com"

    @patch("requests.get")
    def test_query(self, mock_get, prometheus_client, mock_prometheus_response):
        """Test instant query functionality."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_prometheus_response["instant"]
        mock_get.return_value = mock_response

        test_time = datetime.now()
        result = prometheus_client.query("test_metric", time=test_time)

        assert result["status"] == "success"
        assert result["data"]["resultType"] == "vector"
        assert len(result["data"]["result"]) == 1
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_query_with_timestamp(self, mock_get, prometheus_client, mock_prometheus_response):
        """Test instant query functionality with Unix timestamp."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_prometheus_response["instant"]
        mock_get.return_value = mock_response

        test_time = int(datetime.now().timestamp())
        result = prometheus_client.query("test_metric", time=test_time)

        assert result["status"] == "success"
        assert result["data"]["resultType"] == "vector"
        assert len(result["data"]["result"]) == 1
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_query_range(self, mock_get, prometheus_client, mock_prometheus_response):
        """Test range query functionality."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_prometheus_response["range"]
        mock_get.return_value = mock_response

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        result = prometheus_client.query_range("test_metric", start_time, end_time)

        assert result["status"] == "success"
        assert result["data"]["resultType"] == "matrix"
        assert len(result["data"]["result"]) == 1
        mock_get.assert_called_once()


class TestPrometheusDataSource:
    """Test suite for PrometheusDataSource."""

    @patch("requests.get")
    def test_instant_query_to_table(
        self, mock_get, prometheus_datasource, mock_prometheus_response
    ):
        """Test converting instant query results to DataFrame."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_prometheus_response["instant"]
        mock_get.return_value = mock_response

        test_time = datetime.now()
        df = prometheus_datasource.instant_query_to_table("test_metric", time=test_time)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert list(df.columns) == ["timestamp", "metric_name", "value", "labels"]
        assert df["metric_name"].iloc[0] == "test_metric"
        assert df["value"].iloc[0] == 13.255406195207481

    @patch("requests.get")
    def test_range_query_to_table(self, mock_get, prometheus_datasource, mock_prometheus_response):
        """Test converting range query results to DataFrame."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_prometheus_response["range"]
        mock_get.return_value = mock_response

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        df = prometheus_datasource.range_query_to_table("test_metric", start_time, end_time)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["timestamp", "metric_name", "value", "labels"]
        assert df["metric_name"].iloc[0] == "test_metric"

    def test_save_query_results(self, prometheus_datasource, tmp_path):
        """Test saving query results to different formats."""
        df = pd.DataFrame(
            {
                "timestamp": [datetime.now()],
                "metric_name": ["test_metric"],
                "value": [1.0],
                "labels": [{"test": "label"}],
            }
        )

        parquet_path = tmp_path / "test.parquet"
        prometheus_datasource.save_query_results(df, str(parquet_path), "parquet")
        assert parquet_path.exists()

        csv_path = tmp_path / "test.csv"
        prometheus_datasource.save_query_results(df, str(csv_path), "csv")
        assert csv_path.exists()

        json_path = tmp_path / "test.json"
        prometheus_datasource.save_query_results(df, str(json_path), "json")
        assert json_path.exists()

        with pytest.raises(ValueError, match="Unsupported format"):
            prometheus_datasource.save_query_results(df, "test.txt", "txt")

    @patch("requests.get")
    def test_query_error_handling(self, mock_get, prometheus_datasource):
        """Test error handling for failed queries."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "error", "error": "Test error"}
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Query failed"):
            prometheus_datasource.instant_query_to_table("test_metric")
