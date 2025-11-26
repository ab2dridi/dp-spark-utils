"""
Tests for Hive operations module.

This module contains unit tests for Hive operations including
table existence checks and column mapping.
"""

from unittest.mock import MagicMock

from dp_spark_utils.hive import (
    check_table_exists,
    get_columns_map,
)


class TestCheckTableExists:
    """Tests for the check_table_exists function."""

    def test_table_exists_returns_true(self, spark_session_mock):
        """
        Test that check_table_exists returns True when table exists.
        """
        spark_session_mock.catalog.tableExists.return_value = True

        result = check_table_exists(spark_session_mock, "my_database", "my_table")

        assert result is True
        spark_session_mock.catalog.tableExists.assert_called_once_with(
            "my_table", "my_database"
        )

    def test_table_does_not_exist(self, spark_session_mock):
        """
        Test that check_table_exists returns False when table doesn't exist.
        """
        spark_session_mock.catalog.tableExists.return_value = False

        result = check_table_exists(
            spark_session_mock, "my_database", "nonexistent_table"
        )

        assert result is False

    def test_table_exists_with_different_database(self, spark_session_mock):
        """
        Test checking table existence in different databases.
        """
        spark_session_mock.catalog.tableExists.return_value = True

        result = check_table_exists(spark_session_mock, "prod_db", "users")

        spark_session_mock.catalog.tableExists.assert_called_once_with(
            "users", "prod_db"
        )
        assert result is True


class TestGetColumnsMap:
    """Tests for the get_columns_map function."""

    def test_get_columns_map_basic(self, spark_session_mock):
        """
        Test getting columns map for a table.
        """
        # Create mock column objects
        mock_col1 = MagicMock()
        mock_col1.name = "UserID"
        mock_col2 = MagicMock()
        mock_col2.name = "UserName"
        mock_col3 = MagicMock()
        mock_col3.name = "Email"

        spark_session_mock.catalog.listColumns.return_value = [
            mock_col1,
            mock_col2,
            mock_col3,
        ]

        columns, columns_map = get_columns_map(
            spark_session_mock, "my_database", "users"
        )

        assert columns == ["UserID", "UserName", "Email"]
        assert columns_map == {
            "userid": "UserID",
            "username": "UserName",
            "email": "Email",
        }

    def test_get_columns_map_empty_table(self, spark_session_mock):
        """
        Test getting columns map for a table with no columns.
        """
        spark_session_mock.catalog.listColumns.return_value = []

        columns, columns_map = get_columns_map(
            spark_session_mock, "my_database", "empty_table"
        )

        assert columns == []
        assert columns_map == {}

    def test_get_columns_map_case_insensitive_mapping(self, spark_session_mock):
        """
        Test that the columns map is case-insensitive for lookups.
        """
        mock_col1 = MagicMock()
        mock_col1.name = "COLUMN_A"
        mock_col2 = MagicMock()
        mock_col2.name = "Column_B"
        mock_col3 = MagicMock()
        mock_col3.name = "column_c"

        spark_session_mock.catalog.listColumns.return_value = [
            mock_col1,
            mock_col2,
            mock_col3,
        ]

        columns, columns_map = get_columns_map(
            spark_session_mock, "db", "mixed_case_table"
        )

        # Original case preserved in columns list
        assert "COLUMN_A" in columns
        assert "Column_B" in columns
        assert "column_c" in columns

        # Lowercase keys map to original names
        assert columns_map["column_a"] == "COLUMN_A"
        assert columns_map["column_b"] == "Column_B"
        assert columns_map["column_c"] == "column_c"

    def test_get_columns_map_calls_catalog_correctly(self, spark_session_mock):
        """
        Test that get_columns_map calls catalog.listColumns with correct arguments.
        """
        spark_session_mock.catalog.listColumns.return_value = []

        get_columns_map(spark_session_mock, "test_db", "test_table")

        spark_session_mock.catalog.listColumns.assert_called_once_with(
            "test_table", "test_db"
        )
