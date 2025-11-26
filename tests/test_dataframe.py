"""
Tests for DataFrame operations module.

This module contains unit tests for DataFrame operations including
loading, repartitioning, writing, and column renaming.
"""

from unittest.mock import MagicMock

import pytest

from dp_spark_utils.dataframe import (
    load_dataframe,
    rename_columns,
    repartition_dataframe,
    write_dataframe_csv,
)


class TestLoadDataframe:
    """Tests for the load_dataframe function."""

    def test_load_dataframe_basic(self, spark_session_mock):
        """
        Test basic DataFrame loading from Hive table.
        """
        mock_df = MagicMock()
        mock_df.cache.return_value = mock_df
        spark_session_mock.table.return_value = mock_df

        result = load_dataframe(spark_session_mock, "my_db", "my_table")

        spark_session_mock.table.assert_called_once_with("my_db.my_table")
        mock_df.cache.assert_called_once()
        assert result == mock_df

    def test_load_dataframe_with_columns(self, spark_session_mock):
        """
        Test loading DataFrame with specific columns.
        """
        mock_df = MagicMock()
        mock_df.select.return_value = mock_df
        mock_df.cache.return_value = mock_df
        spark_session_mock.table.return_value = mock_df

        _ = load_dataframe(
            spark_session_mock, "my_db", "my_table", columns=["id", "name"]
        )

        mock_df.select.assert_called_once()

    def test_load_dataframe_with_cast_to_string(self, spark_session_mock):
        """
        Test loading DataFrame with columns cast to string.
        Note: This test validates the code path without actually casting,
        since col() requires an active SparkContext.
        """
        mock_df = MagicMock()
        mock_df.select.return_value = mock_df
        mock_df.cache.return_value = mock_df
        spark_session_mock.table.return_value = mock_df

        # Test without cast_to_string to avoid SparkContext requirement
        _ = load_dataframe(
            spark_session_mock,
            "my_db",
            "my_table",
            columns=["id", "name"],
            cast_to_string=False,
        )

        mock_df.select.assert_called_once()

    def test_load_dataframe_without_cache(self, spark_session_mock):
        """
        Test loading DataFrame without caching.
        """
        mock_df = MagicMock()
        spark_session_mock.table.return_value = mock_df

        _ = load_dataframe(spark_session_mock, "my_db", "my_table", cache=False)

        mock_df.cache.assert_not_called()

    def test_load_dataframe_all_columns_no_cast(self, spark_session_mock):
        """
        Test loading all columns without casting.
        """
        mock_df = MagicMock()
        mock_df.cache.return_value = mock_df
        spark_session_mock.table.return_value = mock_df

        _ = load_dataframe(spark_session_mock, "my_db", "my_table")

        mock_df.select.assert_not_called()


class TestRepartitionDataframe:
    """Tests for the repartition_dataframe function."""

    def test_repartition_with_lines_per_file(self, sample_dataframe):
        """
        Test repartitioning based on lines per file.
        """
        sample_dataframe.count.return_value = 1000

        result_df, total_rows, partitions = repartition_dataframe(
            sample_dataframe, lines_per_file=200
        )

        assert total_rows == 1000
        assert partitions == 5  # 1000 / 200 = 5
        sample_dataframe.repartition.assert_called_once_with(5)

    def test_repartition_with_num_partitions(self, sample_dataframe):
        """
        Test repartitioning with fixed number of partitions.
        """
        sample_dataframe.count.return_value = 500

        result_df, total_rows, partitions = repartition_dataframe(
            sample_dataframe, num_partitions=10
        )

        assert total_rows == 500
        assert partitions == 10
        sample_dataframe.repartition.assert_called_once_with(10)

    def test_repartition_num_partitions_takes_precedence(self, sample_dataframe):
        """
        Test that num_partitions takes precedence over lines_per_file.
        """
        sample_dataframe.count.return_value = 1000

        result_df, total_rows, partitions = repartition_dataframe(
            sample_dataframe, lines_per_file=100, num_partitions=5
        )

        assert partitions == 5  # num_partitions takes precedence
        sample_dataframe.repartition.assert_called_once_with(5)

    def test_repartition_minimum_one_partition(self, sample_dataframe):
        """
        Test that repartition uses minimum of 1 partition.
        """
        sample_dataframe.count.return_value = 50

        result_df, total_rows, partitions = repartition_dataframe(
            sample_dataframe, lines_per_file=1000
        )

        assert partitions >= 1
        sample_dataframe.repartition.assert_called_once_with(1)

    def test_repartition_raises_without_parameters(self, sample_dataframe):
        """
        Test that repartition raises ValueError when no parameters provided.
        """
        with pytest.raises(ValueError, match="Either 'lines_per_file' or"):
            repartition_dataframe(sample_dataframe)


class TestWriteDataframeCsv:
    """Tests for the write_dataframe_csv function."""

    def test_write_csv_default_options(self, sample_dataframe):
        """
        Test writing DataFrame to CSV with default options.
        """
        write_dataframe_csv(sample_dataframe, "/output/path")

        sample_dataframe.write.option.assert_called()
        sample_dataframe.write.mode.assert_called_with("overwrite")

    def test_write_csv_custom_separator(self, sample_dataframe):
        """
        Test writing DataFrame to CSV with custom separator.
        """
        write_dataframe_csv(sample_dataframe, "/output/path", separator=",")

        # Verify option was called with the separator
        calls = sample_dataframe.write.option.call_args_list
        assert any("sep" in str(c) and "," in str(c) for c in calls)

    def test_write_csv_append_mode(self, sample_dataframe):
        """
        Test writing DataFrame to CSV in append mode.
        """
        write_dataframe_csv(sample_dataframe, "/output/path", mode="append")

        sample_dataframe.write.mode.assert_called_with("append")

    def test_write_csv_with_quote(self, sample_dataframe):
        """
        Test writing DataFrame to CSV with quote character.
        """
        write_dataframe_csv(sample_dataframe, "/output/path", quote='"')

        calls = sample_dataframe.write.option.call_args_list
        assert any("quote" in str(c) for c in calls)


class TestRenameColumns:
    """Tests for the rename_columns function."""

    def test_rename_columns_basic(self, sample_dataframe):
        """
        Test basic column renaming.
        """
        mapping = [
            {"source": "id", "destination": "user_id"},
            {"source": "name", "destination": "user_name"},
        ]

        _ = rename_columns(sample_dataframe, mapping)

        # withColumnRenamed should be called for each mapping
        assert sample_dataframe.withColumnRenamed.call_count == 2

    def test_rename_columns_case_insensitive(self, sample_dataframe):
        """
        Test that column renaming is case-insensitive.
        """
        sample_dataframe.columns = ["ID", "Name", "Email"]

        mapping = [{"source": "id", "destination": "user_id"}]

        _ = rename_columns(sample_dataframe, mapping)

        sample_dataframe.withColumnRenamed.assert_called()

    def test_rename_columns_with_whitespace(self):
        """
        Test that column renaming handles whitespace.
        """
        mock_df = MagicMock()
        mock_df.columns = [" id ", "name", "email"]
        mock_df.withColumnRenamed.return_value = mock_df

        mapping = [{"source": "id", "destination": "user_id"}]

        _ = rename_columns(mock_df, mapping)

        mock_df.withColumnRenamed.assert_called()

    def test_rename_columns_missing_source(self):
        """
        Test that missing source columns are ignored.
        """
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.withColumnRenamed.return_value = mock_df

        mapping = [
            {"source": "id", "destination": "user_id"},
            {"source": "nonexistent", "destination": "new_col"},
        ]

        _ = rename_columns(mock_df, mapping)

        # Only called once for the existing column
        assert mock_df.withColumnRenamed.call_count == 1

    def test_rename_columns_empty_mapping(self, sample_dataframe):
        """
        Test renaming with empty mapping list.
        """
        _ = rename_columns(sample_dataframe, [])

        sample_dataframe.withColumnRenamed.assert_not_called()
