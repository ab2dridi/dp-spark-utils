"""
HDFS operations for PySpark.

This module contains functions for interacting with HDFS through PySpark,
including file existence checks, listing directories, and moving files.
"""

from typing import List, Optional

from pyspark.sql import SparkSession

from dp_spark_utils.logging_config import get_logger


def get_hadoop_fs(spark: SparkSession):
    """
    Get the Hadoop FileSystem object from the given Spark session.

    This function retrieves the Hadoop FileSystem Java object which can be
    used for various HDFS operations like checking file existence, listing
    directories, and moving files.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        Hadoop FileSystem: Java object representing Hadoop's FileSystem.

    Example:
        >>> spark = SparkSession.builder.getOrCreate()
        >>> fs = get_hadoop_fs(spark)
        >>> # Now you can use fs for HDFS operations
    """
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )


def check_file_exists(
    spark: SparkSession,
    hdfs_path: str,
    extension: Optional[str] = None,
) -> bool:
    """
    Check if a file or directory exists in HDFS, optionally with a specific extension.

    This is a generic function that can verify the existence of any file or
    directory in HDFS. When an extension is provided, it also validates that
    the path ends with that extension.

    Args:
        spark (SparkSession): The active Spark session.
        hdfs_path (str): The full HDFS path to check.
        extension (Optional[str]): Optional file extension to validate
            (e.g., ".json", ".csv", ".parquet"). If None, only existence
            is checked.

    Returns:
        bool: True if the path exists (and matches the extension if provided),
              False otherwise.

    Example:
        >>> # Check if any file exists
        >>> check_file_exists(spark, "/data/myfile.json")
        True
        >>> # Check if a JSON file exists
        >>> check_file_exists(spark, "/data/myfile.json", extension=".json")
        True
        >>> # Check if a file exists with wrong extension
        >>> check_file_exists(spark, "/data/myfile.json", extension=".csv")
        False
    """
    fs = get_hadoop_fs(spark)
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    exists = fs.exists(path)

    if extension is not None:
        return exists and hdfs_path.endswith(extension)

    return exists


def hdfs_list_files(
    spark: SparkSession,
    path: str,
    extension: Optional[str] = None,
) -> List[str]:
    """
    List files in an HDFS directory.

    This function returns a list of file and directory names within the
    specified HDFS path. Optionally, results can be filtered by file extension.

    Args:
        spark (SparkSession): The active Spark session.
        path (str): The HDFS directory path to list.
        extension (Optional[str]): Optional file extension to filter results
            (e.g., ".csv", ".json"). If None, all files are returned.

    Returns:
        List[str]: A list of file/directory names in the specified path.
                   Returns an empty list if the path does not exist.

    Example:
        >>> # List all files
        >>> hdfs_list_files(spark, "/data/input/")
        ['file1.csv', 'file2.csv', 'subfolder']
        >>> # List only CSV files
        >>> hdfs_list_files(spark, "/data/input/", extension=".csv")
        ['file1.csv', 'file2.csv']
    """
    fs = get_hadoop_fs(spark)
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(hdfs_path):
        get_logger(__name__).warning("The path %s does not exist.", path)
        return []

    list_status = fs.listStatus(hdfs_path)
    file_names = [file.getPath().getName() for file in list_status]

    if extension is not None:
        file_names = [f for f in file_names if f.endswith(extension)]

    return file_names


def move_files(
    spark: SparkSession,
    source_folder: str,
    target_folder: str,
    extension: Optional[str] = None,
    overwrite: bool = True,
) -> List[str]:
    """
    Move files from a source folder to a target folder in HDFS.

    This function moves all files (or files with a specific extension) from
    the source directory to the target directory. If the target directory
    does not exist, it will be created.

    Args:
        spark (SparkSession): The active Spark session.
        source_folder (str): The HDFS path of the source folder.
        target_folder (str): The HDFS path of the target folder.
        extension (Optional[str]): Optional file extension to filter which
            files to move (e.g., ".csv"). If None, all files are moved.
        overwrite (bool): If True, overwrites existing files in target.
            Defaults to True.

    Returns:
        List[str]: A list of file names that were successfully moved.

    Example:
        >>> # Move all CSV files
        >>> move_files(spark, "/data/temp/", "/data/output/", extension=".csv")
        ['file1.csv', 'file2.csv']
        >>> # Move all files
        >>> move_files(spark, "/data/temp/", "/data/output/")
        ['file1.csv', 'file2.csv', 'metadata.json']
    """
    fs = get_hadoop_fs(spark)

    source_path = spark._jvm.org.apache.hadoop.fs.Path(source_folder)
    target_path = spark._jvm.org.apache.hadoop.fs.Path(target_folder)

    # Create target directory if it doesn't exist
    if not fs.exists(target_path):
        fs.mkdirs(target_path)
        get_logger(__name__).info("Created target directory: %s", target_folder)

    moved_files = []
    file_statuses = fs.listStatus(source_path)

    for status in file_statuses:
        file_name = status.getPath().getName()

        # Filter by extension if specified
        if extension is not None and not file_name.endswith(extension):
            continue

        source_file = status.getPath()
        target_file = spark._jvm.org.apache.hadoop.fs.Path(
            target_folder + "/" + file_name
        )

        # Delete existing file if overwrite is enabled
        if overwrite and fs.exists(target_file):
            fs.delete(target_file, False)
            get_logger(__name__).debug("Deleted existing file: %s", target_file)

        # Move the file
        fs.rename(source_file, target_file)
        moved_files.append(file_name)
        get_logger(__name__).info("Moved file from %s to %s", source_file, target_file)

    return moved_files
