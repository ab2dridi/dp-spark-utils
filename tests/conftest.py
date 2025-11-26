"""
Test configuration for dp-spark-utils.

This module provides pytest fixtures and configuration for testing
the dp-spark-utils package.
"""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def spark_session_mock():
    """
    Create a mock SparkSession for testing.

    This fixture creates a mock SparkSession that simulates the behavior
    of a real SparkSession without requiring a Spark cluster.
    """
    spark = MagicMock()

    # Mock JVM components
    spark._jvm = MagicMock()
    spark._jsc = MagicMock()

    # Mock Hadoop FileSystem
    mock_fs = MagicMock()
    spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs

    # Mock Hadoop Path
    spark._jvm.org.apache.hadoop.fs.Path = MagicMock()

    # Mock Hadoop Configuration
    spark._jsc.hadoopConfiguration.return_value = MagicMock()

    # Mock Catalog
    spark.catalog = MagicMock()

    return spark


@pytest.fixture
def mock_hadoop_fs(spark_session_mock):
    """
    Get the mock Hadoop FileSystem from the mock SparkSession.
    """
    return spark_session_mock._jvm.org.apache.hadoop.fs.FileSystem.get.return_value


@pytest.fixture
def sample_dataframe(spark_session_mock):
    """
    Create a mock DataFrame for testing.
    """
    df = MagicMock()
    df.columns = ["id", "name", "email"]
    df.count.return_value = 1000
    df.select.return_value = df
    df.cache.return_value = df
    df.repartition.return_value = df
    df.write = MagicMock()
    df.write.option.return_value = df.write
    df.write.mode.return_value = df.write
    df.withColumnRenamed.return_value = df
    return df
