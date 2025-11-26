"""
DataFrame operations for PySpark.

This module contains functions for manipulating PySpark DataFrames,
including loading data from Hive, repartitioning, writing to various
formats, and column operations.
"""

import math
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def load_dataframe(
    spark: SparkSession,
    database: str,
    table: str,
    columns: Optional[List[str]] = None,
    cast_to_string: bool = False,
    cache: bool = True,
) -> DataFrame:
    """
    Load a Hive table as a DataFrame with optional column selection and casting.

    This function loads data from a Hive table and provides options for
    selecting specific columns, casting all columns to string type, and
    caching the result.

    Args:
        spark (SparkSession): The active Spark session.
        database (str): The Hive database name.
        table (str): The Hive table name.
        columns (Optional[List[str]]): List of columns to select. If None,
            all columns are selected.
        cast_to_string (bool): If True, cast all selected columns to string
            type. Defaults to False.
        cache (bool): If True, cache the resulting DataFrame. Defaults to True.

    Returns:
        DataFrame: The loaded DataFrame.

    Example:
        >>> # Load entire table
        >>> df = load_dataframe(spark, "my_db", "users")
        >>> # Load specific columns cast to string
        >>> df = load_dataframe(
        ...     spark, "my_db", "users",
        ...     columns=["id", "name", "email"],
        ...     cast_to_string=True
        ... )
    """
    full_table_name = f"{database}.{table}"
    df = spark.table(full_table_name)

    if columns is not None:
        if cast_to_string:
            df = df.select(*[col(c).cast("string") for c in columns])
        else:
            df = df.select(*columns)

    if cache:
        df = df.cache()

    return df


def repartition_dataframe(
    df: DataFrame,
    lines_per_file: Optional[int] = None,
    num_partitions: Optional[int] = None,
) -> Tuple[DataFrame, int, int]:
    """
    Repartition a DataFrame based on target lines per file or number of partitions.

    This function calculates the optimal number of partitions based on the
    total row count and the desired number of lines per output file. It can
    also accept a fixed number of partitions.

    Args:
        df (DataFrame): The input DataFrame to repartition.
        lines_per_file (Optional[int]): Target number of lines per output file.
            If provided, partitions are calculated based on total rows.
        num_partitions (Optional[int]): Fixed number of partitions to use.
            Takes precedence over lines_per_file if both are provided.

    Returns:
        Tuple[DataFrame, int, int]: A tuple containing:
            - The repartitioned DataFrame
            - Total number of rows in the DataFrame
            - Number of partitions used

    Raises:
        ValueError: If neither lines_per_file nor num_partitions is provided.

    Example:
        >>> # Repartition based on lines per file
        >>> df, total_rows, partitions = repartition_dataframe(
        ...     df, lines_per_file=100000
        ... )
        >>> # Use fixed number of partitions
        >>> df, total_rows, partitions = repartition_dataframe(
        ...     df, num_partitions=10
        ... )
    """
    if lines_per_file is None and num_partitions is None:
        raise ValueError(
            "Either 'lines_per_file' or 'num_partitions' must be provided."
        )

    total_rows = df.count()

    if num_partitions is not None:
        partitions = num_partitions
    else:
        partitions = max(1, math.ceil(total_rows / lines_per_file))

    df = df.repartition(partitions)

    return df, total_rows, partitions


def write_dataframe_csv(
    df: DataFrame,
    output_path: str,
    header: bool = True,
    separator: str = ";",
    encoding: str = "ISO-8859-1",
    mode: str = "overwrite",
    quote: Optional[str] = None,
    escape: Optional[str] = None,
) -> None:
    """
    Write a DataFrame to CSV format with configurable options.

    This function writes a DataFrame to CSV files in the specified path
    with various formatting options.

    Args:
        df (DataFrame): The DataFrame to write.
        output_path (str): The output path (HDFS or local).
        header (bool): Include header row. Defaults to True.
        separator (str): Field separator character. Defaults to ";".
        encoding (str): Character encoding. Defaults to "ISO-8859-1".
        mode (str): Write mode ("overwrite", "append", "error", "ignore").
            Defaults to "overwrite".
        quote (Optional[str]): Quote character for fields containing special
            characters. Defaults to None (Spark default).
        escape (Optional[str]): Escape character for quotes. Defaults to None.

    Returns:
        None

    Example:
        >>> write_dataframe_csv(df, "/data/output/export")
        >>> # With custom options
        >>> write_dataframe_csv(
        ...     df, "/data/output/export",
        ...     separator=",",
        ...     encoding="UTF-8",
        ...     quote='"'
        ... )
    """
    writer = (
        df.write.option("header", str(header).lower())
        .option("sep", separator)
        .option("encoding", encoding)
    )

    if quote is not None:
        writer = writer.option("quote", quote)

    if escape is not None:
        writer = writer.option("escape", escape)

    writer.mode(mode).csv(output_path)


def rename_columns(df: DataFrame, column_mapping: List[Dict[str, str]]) -> DataFrame:
    """
    Rename DataFrame columns based on a source-destination mapping.

    This function renames columns in a DataFrame using a case-insensitive
    match on source column names. The mapping is a list of dictionaries
    with 'source' and 'destination' keys.

    Args:
        df (DataFrame): The input DataFrame.
        column_mapping (List[Dict[str, str]]): A list of mapping dictionaries,
            each containing:
            - 'source': The source column name (case-insensitive)
            - 'destination': The target column name

    Returns:
        DataFrame: A new DataFrame with renamed columns.

    Example:
        >>> mapping = [
        ...     {"source": "user_id", "destination": "UserID"},
        ...     {"source": "user_name", "destination": "UserName"},
        ... ]
        >>> df_renamed = rename_columns(df, mapping)
    """
    # Create case-insensitive mapping of existing columns
    df_cols_lower = [c.lower().strip() for c in df.columns]
    df_cols_map = {c.lower().strip(): c for c in df.columns}

    for field in column_mapping:
        col_source = field.get("source", "").lower().strip()
        col_destination = field.get("destination")

        if col_source in df_cols_lower and col_destination:
            actual_col_name = df_cols_map[col_source]
            df = df.withColumnRenamed(actual_col_name, col_destination)
            # Update the mapping after rename
            df_cols_map[col_source] = col_destination

    return df
