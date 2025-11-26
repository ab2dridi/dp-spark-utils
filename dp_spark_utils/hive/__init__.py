"""
Hive operations module for dp-spark-utils.

This module provides utilities for interacting with Hive tables
through PySpark Catalog API.

Functions:
    - check_table_exists: Check if a Hive table exists
    - get_columns_map: Get column names and case-insensitive mapping
"""

from dp_spark_utils.hive.operations import (
    check_table_exists,
    get_columns_map,
)

__all__ = [
    "check_table_exists",
    "get_columns_map",
]
