"""Tests for Prometheus client and data source functionality."""

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import pytest
from op_analytics.coreutils.prometheus.client import PrometheusClient


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

        test_time = datetime.fromtimestamp(int(datetime.now().timestamp()))
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
