"""
Validation operations for data processing.

This module contains functions for validating data, filenames, and schemas
to ensure data quality and consistency in processing pipelines.
"""

import re
from typing import List, Set, Tuple

from dp_spark_utils.logging_config import get_logger


def validate_columns_match(
    source_columns: List[str],
    target_columns: List[str],
    case_sensitive: bool = False,
    ignore_whitespace: bool = True,
) -> Tuple[bool, Set[str], Set[str]]:
    """
    Validate that source columns match target columns.

    This function compares two lists of column names and identifies any
    mismatches. It can perform case-insensitive comparisons and optionally
    ignore whitespace differences.

    Args:
        source_columns (List[str]): List of column names from the source
            (e.g., CSV headers).
        target_columns (List[str]): List of column names from the target
            (e.g., schema definition).
        case_sensitive (bool): If True, perform case-sensitive comparison.
            Defaults to False.
        ignore_whitespace (bool): If True, strip whitespace from column names.
            Defaults to True.

    Returns:
        Tuple[bool, Set[str], Set[str]]: A tuple containing:
            - bool: True if columns match exactly, False otherwise
            - Set[str]: Columns in source but not in target
            - Set[str]: Columns in target but not in source

    Example:
        >>> source = ["UserID", "Name", "Email"]
        >>> target = ["userid", "name", "email"]
        >>> is_match, missing_in_target, missing_in_source = validate_columns_match(
        ...     source, target
        ... )
        >>> is_match
        True
        >>> # With extra column in source
        >>> source = ["UserID", "Name", "Email", "Phone"]
        >>> is_match, extra, missing = validate_columns_match(source, target)
        >>> is_match
        False
        >>> extra
        {'phone'}
    """

    def normalize(col: str) -> str:
        """Normalize column name based on settings."""
        if ignore_whitespace:
            col = col.strip().replace(" ", "")
        if not case_sensitive:
            col = col.lower()
        return col

    source_normalized = {normalize(col) for col in source_columns}
    target_normalized = {normalize(col) for col in target_columns}

    in_source_not_in_target = source_normalized - target_normalized
    in_target_not_in_source = target_normalized - source_normalized

    is_match = not in_source_not_in_target and not in_target_not_in_source

    if not is_match:
        if in_source_not_in_target:
            get_logger(__name__).warning(
                "Columns in source but not in target: %s", in_source_not_in_target
            )
        if in_target_not_in_source:
            get_logger(__name__).warning(
                "Columns in target but not in source: %s", in_target_not_in_source
            )

    return is_match, in_source_not_in_target, in_target_not_in_source


def validate_filename_pattern(filename: str, pattern: str) -> bool:
    """
    Validate that a filename matches a regular expression pattern.

    This function checks if the given filename matches the specified
    regex pattern. Useful for validating file naming conventions.

    Args:
        filename (str): The filename to validate.
        pattern (str): The regular expression pattern to match against.

    Returns:
        bool: True if the filename matches the pattern, False otherwise.

    Example:
        >>> # Validate date-prefixed files
        >>> validate_filename_pattern(
        ...     "20240131_export.csv",
        ...     r"^\\d{8}_export\\.csv$"
        ... )
        True
        >>> # Validate files with version numbers
        >>> validate_filename_pattern(
        ...     "data_v1.2.3.json",
        ...     r"^data_v\\d+\\.\\d+\\.\\d+\\.json$"
        ... )
        True
        >>> validate_filename_pattern("invalid.txt", r"^data_.*\\.csv$")
        False
    """
    return bool(re.match(pattern, filename))
