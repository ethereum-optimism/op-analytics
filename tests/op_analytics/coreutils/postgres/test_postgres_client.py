"""Tests for the PostgreSQL client."""

from unittest import TestCase, mock

import polars as pl
from google.cloud.sql.connector import IPTypes

from op_analytics.coreutils.postgres.client import PostgresClient


class TestPostgresClient(TestCase):
    """Test cases for the PostgreSQL client."""

    def setUp(self):
        """Set up test environment."""
        self.instance_connection_name = "test-instance"
        self.db_name = "test_db"
        self.db_user = "test_user"
        self.client = PostgresClient(
            instance_connection_name=self.instance_connection_name,
            db_name=self.db_name,
            db_user=self.db_user,
        )

    @mock.patch("op_analytics.coreutils.postgres.client.Connector")
    def test_init(self, mock_connector):
        """Test client initialization."""
        client = PostgresClient(
            instance_connection_name=self.instance_connection_name,
            db_name=self.db_name,
            db_user=self.db_user,
        )
        self.assertEqual(client.instance_connection_name, self.instance_connection_name)
        self.assertEqual(client.db_name, self.db_name)
        self.assertEqual(client.db_user, self.db_user)
        self.assertEqual(client.ip_type, IPTypes.PUBLIC)

    @mock.patch("op_analytics.coreutils.postgres.client.Connector")
    @mock.patch("op_analytics.coreutils.postgres.client.get_credentials")
    def test_get_connector(self, mock_get_credentials, mock_connector):
        """Test getting a connector."""
        mock_credentials = mock.MagicMock()
        mock_get_credentials.return_value = mock_credentials

        connector = self.client._get_connector()

        mock_get_credentials.assert_called_once()
        mock_connector.assert_called_once_with(credentials=mock_credentials)
        self.assertEqual(connector, mock_connector.return_value)

    @mock.patch("op_analytics.coreutils.postgres.client.Connector")
    @mock.patch("op_analytics.coreutils.postgres.client.PostgresClient._get_connector")
    def test_get_connection(self, mock_get_connector, mock_connector):
        """Test getting a database connection."""
        mock_connector_instance = mock.MagicMock()
        mock_get_connector.return_value = mock_connector_instance
        mock_connector_instance.connect.return_value = mock.MagicMock()

        conn = self.client.get_connection()

        mock_get_connector.assert_called_once()
        mock_connector_instance.connect.assert_called_once_with(
            self.instance_connection_name,
            "pg8000",
            user=self.db_user,
            db=self.db_name,
            enable_iam_auth=True,
            ip_type=IPTypes.PUBLIC,
        )
        self.assertIsNotNone(conn)

    @mock.patch("op_analytics.coreutils.postgres.client.PostgresClient.get_connection")
    def test_list_tables(self, mock_get_connection):
        """Test listing tables in a schema."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Mock table data
        mock_tables = [
            ("public", "table1", "owner1", None, False, False, False, False),
            ("public", "table2", "owner2", None, False, False, False, False),
        ]
        mock_cursor.fetchall.return_value = mock_tables

        tables = self.client.list_tables()

        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = %s",
            ("public",),
        )
        self.assertEqual(tables, mock_tables)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("op_analytics.coreutils.postgres.client.PostgresClient.get_connection")
    def test_run_query_without_parameters(self, mock_get_connection):
        """Test running a query without parameters."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Mock query results with consistent types - both columns are strings
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [("1", "test1"), ("2", "test2")]

        query = "SELECT * FROM test_table"
        df = self.client.run_query(query)

        mock_cursor.execute.assert_called_once_with(query)
        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(df.shape, (2, 2))
        self.assertEqual(list(df.columns), ["id", "name"])
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("op_analytics.coreutils.postgres.client.PostgresClient.get_connection")
    def test_run_query_with_parameters(self, mock_get_connection):
        """Test running a query with parameters."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Mock query results
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "test")]

        query = "SELECT * FROM test_table WHERE id = %s"
        parameters = {"id": 1}
        df = self.client.run_query(query, parameters)

        mock_cursor.execute.assert_called_once_with(query, parameters)
        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(df.shape, (1, 2))
        self.assertEqual(list(df.columns), ["id", "name"])
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @mock.patch("op_analytics.coreutils.postgres.client.PostgresClient.get_connection")
    def test_run_query_empty_results(self, mock_get_connection):
        """Test running a query that returns no results."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Mock empty results
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []

        query = "SELECT * FROM empty_table"
        df = self.client.run_query(query)

        mock_cursor.execute.assert_called_once_with(query)
        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(df.shape, (0, 2))
        self.assertEqual(list(df.columns), ["id", "name"])
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
