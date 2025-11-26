"""
HDFS operations module for dp-spark-utils.

This module provides utilities for interacting with the Hadoop Distributed
File System (HDFS) through PySpark.

Functions:
    - get_hadoop_fs: Get the Hadoop FileSystem object
    - check_file_exists: Check if a file or directory exists in HDFS
    - hdfs_list_files: List files in an HDFS directory
    - move_files: Move files between HDFS directories
"""

from dp_spark_utils.hdfs.operations import (
    check_file_exists,
    get_hadoop_fs,
    hdfs_list_files,
    move_files,
)

__all__ = [
    "get_hadoop_fs",
    "check_file_exists",
    "hdfs_list_files",
    "move_files",
]
