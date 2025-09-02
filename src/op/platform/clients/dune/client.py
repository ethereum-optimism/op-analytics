from typing import Iterable

import polars as pl
import spice

from .config import Config
from ...infra.secrets.default import env_get
from ....core.interfaces.sql_runner import SqlRunner


class DuneClient(SqlRunner):
    """
    Thin SqlLikeClient for Dune's Python `spice` API.

    - `run_sql(sql, params=None)` returns List[Dict[str, Any]] to match SqlQuerySource.
    - `run_sql_df(sql)` returns a Polars DataFrame (convenience).
    - Executes remotely and blocks until completion (`poll=True`).
    """

    def __init__(self, cfg: Config = Config()) -> None:
        self._api_key = env_get("DUNE_API_KEY") if cfg.api_key is None else cfg.api_key
        self._timeout = cfg.timeout_seconds
        self._poll_interval = cfg.poll_interval_seconds
        self._max_retries = cfg.max_retries
        self._performance = cfg.performance  # e.g. "medium"

    # --- internal: single execution path that returns a DataFrame
    def _query_df(self, sql: str) -> pl.DataFrame:
        try:
            # spice handles polling when poll=True; performance tier controls compute budget.
            print(sql)
            df: pl.DataFrame = spice.query(
                query_or_execution=sql,
                api_key=self._api_key,
                refresh=True,   # bypass server-side cache
                cache=False,    # ensure fresh results
                performance=self._performance,
                poll=True,      # block until finished; returns DataFrame
            )
            return df
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"Dune query failed: {e}") from e

    # --- Convenience: keep a DF-returning method if callers want Polars directly
    def run_sql(self, sql: str) -> Iterable[dict]:
        df = self._query_df(sql)

        for row in df.iter_rows(named=True):  # type: ignore[attr-defined]
            yield row
        return
