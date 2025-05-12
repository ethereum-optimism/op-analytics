"""Prometheus client utilities for querying metrics."""

from typing import Dict, Any, Union, Optional
from dataclasses import dataclass
import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import datetime, now

log = structlog.get_logger()


@dataclass
class PrometheusClient:
    """A simple client for querying Prometheus metrics."""

    username: str
    password: str
    base_url: str
    retry_attempts: int = 3
    session: Optional[requests.Session] = None

    def __post_init__(self):
        """Initialize the HTTP session if not provided."""
        if self.session is None:
            self.session = new_session()
            self.session.auth = (self.username, self.password)

    def query(self, query: str, time: datetime, timeout: int = 10) -> Dict[str, Any]:
        """Execute a PromQL query.

        Args:
            query: The PromQL query string.
            time: The evaluation timestamp.
            timeout: Request timeout in seconds.

        Returns:
            Dict containing the query results.
        """
        start_time = now()
        params: Dict[str, Union[str, int]] = {"query": query, "time": int(time.timestamp())}
        url = f"{self.base_url}/query"

        assert self.session is not None, "Session not initialized"

        try:
            response = get_data(
                session=self.session,
                url=url,
                params=params,
                retry_attempts=self.retry_attempts,
                timeout=timeout,
            )
            duration = (now() - start_time).total_seconds()
            log.info(
                "Prometheus query executed",
                query=query,
                duration=f"{duration:.2f}s",
            )
            return response
        except Exception as e:
            duration = (now() - start_time).total_seconds()
            log.error(
                "Prometheus query failed",
                query=query,
                error=str(e),
                duration=f"{duration:.2f}s",
            )
            raise

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
        query_start_time = now()
        params: Dict[str, Union[str, int]] = {
            "query": query,
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "step": step,
        }
        url = f"{self.base_url}/query_range"

        assert self.session is not None, "Session not initialized"

        try:
            response = get_data(
                session=self.session,
                url=url,
                params=params,
                retry_attempts=self.retry_attempts,
                timeout=timeout,
            )
            duration = (now() - query_start_time).total_seconds()
            log.info(
                "Prometheus range query executed",
                query=query,
                duration=f"{duration:.2f}s",
            )
            return response
        except Exception as e:
            duration = (now() - query_start_time).total_seconds()
            log.error(
                "Prometheus range query failed",
                query=query,
                error=str(e),
                duration=f"{duration:.2f}s",
            )
            raise
