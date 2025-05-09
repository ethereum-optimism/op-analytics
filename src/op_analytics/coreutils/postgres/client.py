"""PostgreSQL client module for database operations using Google Cloud SQL."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import polars as pl
from google.cloud.sql.connector import Connector, IPTypes
import pg8000

from op_analytics.coreutils.gcpauth import get_credentials
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


@dataclass
class PostgresClient:
    """PostgreSQL client for database operations using Google Cloud SQL."""

    instance_connection_name: str
    db_name: str
    db_user: str
    ip_type: IPTypes = IPTypes.PUBLIC

    def _get_connector(self) -> Connector:
        """Get a Google Cloud SQL connector with IAM authentication.

        Returns:
            Connector: Google Cloud SQL connector instance
        """
        connector = Connector(credentials=get_credentials())
        log.info("Initialized Google Cloud SQL connector")
        return connector

    def get_connection(self) -> pg8000.Connection:
        """Get a connection to the PostgreSQL database using IAM authentication.

        Returns:
            pg8000.Connection: PostgreSQL connection object
        """
        connector = self._get_connector()
        return connector.connect(
            self.instance_connection_name,
            "pg8000",
            user=self.db_user,
            db=self.db_name,
            enable_iam_auth=True,
            ip_type=self.ip_type,
        )

    def list_tables(self, schema: str = "public") -> List:
        """List all tables in the specified schema.

        Args:
            schema: Schema name to list tables from (default: "public")

        Returns:
            List of tuples containing table information (schema, table, owner, tablespace,
            is_shared, is_partitioned, is_foreign, is_temporary)
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = %s", (schema,))
            tables = cursor.fetchall()
            log.info(f"Found {len(tables)} tables in '{schema}' schema of '{self.db_name}'")
            return tables
        except Exception as e:
            log.error(f"Error listing tables: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def run_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> pl.DataFrame:
        """Execute a query and return results as a Polars DataFrame.

        Args:
            query: SQL query string
            parameters: Optional parameters for the query

        Returns:
            DataFrame containing query results
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Handle parameters properly
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, parameters)

            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()

            # Handle empty results
            if not data:
                return pl.DataFrame(schema=columns)

            return pl.DataFrame(data, schema=columns)

        except Exception as e:
            log.error(f"Error executing query: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
