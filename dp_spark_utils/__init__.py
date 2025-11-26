"""
dp-spark-utils: A utility package for PySpark operations on Cloudera CDP 7.1.9.

This package provides a collection of utilities for working with PySpark,
HDFS, and Hive in a Cloudera CDP 7.1.9 environment.

Modules:
    - hdfs: HDFS file system operations
    - hive: Hive table operations
    - dataframe: DataFrame manipulation utilities
    - schema: Schema and type mapping utilities
    - date: Date manipulation utilities
    - validation: Data validation utilities
"""

__version__ = "0.1.0"
__author__ = "Data Platform Team"

from dp_spark_utils.dataframe import (
    load_dataframe,
    rename_columns,
    repartition_dataframe,
    write_dataframe_csv,
)
from dp_spark_utils.date import (
    get_last_day_of_previous_month,
    get_last_day_of_previous_two_months,
)
from dp_spark_utils.hdfs import (
    check_file_exists,
    get_hadoop_fs,
    hdfs_list_files,
    move_files,
)
from dp_spark_utils.hive import (
    check_table_exists,
    get_columns_map,
)
from dp_spark_utils.schema import (
    get_ordered_columns_from_schema,
    map_spark_type,
)
from dp_spark_utils.validation import (
    validate_columns_match,
    validate_filename_pattern,
)

__all__ = [
    # HDFS operations
    "get_hadoop_fs",
    "check_file_exists",
    "hdfs_list_files",
    "move_files",
    # Hive operations
    "check_table_exists",
    "get_columns_map",
    # DataFrame operations
    "load_dataframe",
    "repartition_dataframe",
    "write_dataframe_csv",
    "rename_columns",
    # Schema operations
    "map_spark_type",
    "get_ordered_columns_from_schema",
    # Date operations
    "get_last_day_of_previous_month",
    "get_last_day_of_previous_two_months",
    # Validation operations
    "validate_columns_match",
    "validate_filename_pattern",
]
