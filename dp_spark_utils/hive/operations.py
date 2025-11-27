"""
Hive operations for PySpark.

This module contains functions for interacting with Hive tables through
the PySpark Catalog API.
"""

from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

from dp_spark_utils.logging_config import get_logger


def check_table_exists(spark: SparkSession, database: str, table_name: str) -> bool:
    """
    Check if a Hive table exists in the given database.

    This function uses the Spark Catalog API to verify whether a table
    exists in the specified Hive database.

    Args:
        spark (SparkSession): The active Spark session.
        database (str): The Hive database name.
        table_name (str): The Hive table name.

    Returns:
        bool: True if the table exists, False otherwise.

    Example:
        >>> spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        >>> check_table_exists(spark, "my_database", "my_table")
        True
        >>> check_table_exists(spark, "my_database", "nonexistent_table")
        False
    """
    return spark.catalog.tableExists(table_name, database)


def get_columns_map(
    spark: SparkSession, database: str, table_name: str
) -> Tuple[List[str], Dict[str, str]]:
    """
    Get available columns and a case-insensitive name mapping for a Hive table.

    This function retrieves the column metadata from a Hive table and creates
    both a list of column names and a dictionary mapping lowercase column
    names to their actual case-sensitive names.

    Args:
        spark (SparkSession): The active Spark session.
        database (str): The Hive database name.
        table_name (str): The Hive table name.

    Returns:
        Tuple[List[str], Dict[str, str]]: A tuple containing:
            - A list of column names as they appear in the table
            - A dictionary mapping lowercase column names to actual names

    Example:
        >>> spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        >>> columns, columns_map = get_columns_map(spark, "db", "users")
        >>> columns
        ['UserID', 'UserName', 'Email']
        >>> columns_map
        {'userid': 'UserID', 'username': 'UserName', 'email': 'Email'}
    """
    cols_meta = spark.catalog.listColumns(table_name, database)
    available_columns = [c.name for c in cols_meta]
    columns_map = {name.lower(): name for name in available_columns}

    get_logger(__name__).debug(
        "Retrieved %d columns from %s.%s", len(available_columns), database, table_name
    )

    return available_columns, columns_map
