"""Tests for Prometheus client and data source functionality."""

from unittest.mock import patch
import pytest
import requests  # Keep for type hinting if Session is used explicitly in tests
from op_analytics.coreutils.prometheus.client import PrometheusClient
from op_analytics.coreutils.time import datetime, timedelta


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
    client = PrometheusClient(
        username="test_user",
        password="test_pass",
        base_url="https://test-prometheus.example.com/api/v1",
    )
    # __post_init__ should have created the session
    return client


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
        assert client.retry_attempts == 3  # Default value
        assert client.session is not None
        assert isinstance(client.session, requests.Session)
        assert client.session.auth == ("test", "test")

    @patch("op_analytics.coreutils.prometheus.client.get_data")
    def test_query(self, mock_get_data, prometheus_client, mock_prometheus_response):
        """Test instant query functionality."""
        mock_get_data.return_value = mock_prometheus_response["instant"]

        test_time = datetime.now()  # Using standard datetime, client uses op_datetime
        expected_params = {"query": "test_metric", "time": int(test_time.timestamp())}
        expected_url = f"{prometheus_client.base_url}/query"
        default_timeout = 10

        result = prometheus_client.query("test_metric", time=test_time, timeout=default_timeout)

        assert result["status"] == "success"
        assert result["data"]["resultType"] == "vector"
        assert len(result["data"]["result"]) == 1

        mock_get_data.assert_called_once_with(
            session=prometheus_client.session,
            url=expected_url,
            params=expected_params,
            retry_attempts=prometheus_client.retry_attempts,
            timeout=default_timeout,
        )

    @patch("op_analytics.coreutils.prometheus.client.get_data")
    def test_query_with_timestamp(self, mock_get_data, prometheus_client, mock_prometheus_response):
        """Test instant query functionality with Unix timestamp."""
        mock_get_data.return_value = mock_prometheus_response["instant"]

        test_time = datetime.fromtimestamp(int(datetime.now().timestamp()))
        expected_params = {"query": "test_metric", "time": int(test_time.timestamp())}
        expected_url = f"{prometheus_client.base_url}/query"
        default_timeout = 10  # Default timeout for query method

        result = prometheus_client.query("test_metric", time=test_time)

        assert result["status"] == "success"
        assert result["data"]["resultType"] == "vector"
        assert len(result["data"]["result"]) == 1
        mock_get_data.assert_called_once_with(
            session=prometheus_client.session,
            url=expected_url,
            params=expected_params,
            retry_attempts=prometheus_client.retry_attempts,
            timeout=default_timeout,
        )

    @patch("op_analytics.coreutils.prometheus.client.get_data")
    def test_query_range(self, mock_get_data, prometheus_client, mock_prometheus_response):
        """Test range query functionality."""
        mock_get_data.return_value = mock_prometheus_response["range"]

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        expected_params = {
            "query": "test_metric",
            "start": int(start_time.timestamp()),
            "end": int(end_time.timestamp()),
            "step": "1m",  # Default step
        }
        expected_url = f"{prometheus_client.base_url}/query_range"
        default_timeout = 10  # Default timeout for query_range
        default_step = "1m"

        result = prometheus_client.query_range(
            "test_metric", start_time, end_time, step=default_step, timeout=default_timeout
        )

        assert result["status"] == "success"
        assert result["data"]["resultType"] == "matrix"
        assert len(result["data"]["result"]) == 1
        mock_get_data.assert_called_once_with(
            session=prometheus_client.session,
            url=expected_url,
            params=expected_params,
            retry_attempts=prometheus_client.retry_attempts,
            timeout=default_timeout,
        )
