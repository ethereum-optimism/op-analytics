"""Prometheus client utilities for querying metrics."""

import requests
from datetime import datetime
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class PrometheusClient:
    """A simple client for querying Prometheus metrics."""

    username: str
    password: str
    base_url: str

    def query(self, query: str, time: datetime, timeout: int = 10) -> Dict[str, Any]:
        """Execute a PromQL query.

        Args:
            query: The PromQL query string.
            time: The evaluation timestamp.
            timeout: Request timeout in seconds.

        Returns:
            Dict containing the query results.
        """
        params = {"query": query, "time": int(time.timestamp())}

        response = requests.get(
            f"{self.base_url}/query",
            params=params,
            auth=(self.username, self.password),
            timeout=timeout,
        )

        response.raise_for_status()
        return response.json()

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step: str = "1m",
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """Execute a PromQL range query.

        Args:
            query: The PromQL query string.
            start: Start time for the range query.
            end: End time for the range query.
            step: Query resolution step width (e.g. "1m", "5m", "1h").
            timeout: Request timeout in seconds.

        Returns:
            Dict containing the query results.
        """
        params = {
            "query": query,
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "step": step,
        }

        response = requests.get(
            f"{self.base_url}/query_range",
            params=params,
            auth=(self.username, self.password),
            timeout=timeout,
        )

        response.raise_for_status()
        return response.json()
