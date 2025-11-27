"""
Schema operations for PySpark.

This module contains functions for working with PySpark schemas and data types,
including type mapping and schema manipulation utilities.
"""

import re
from typing import Any, Dict, List, Optional

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from dp_spark_utils.logging_config import get_logger

# Default type mapping dictionary
DEFAULT_TYPE_MAPPING: Dict[str, Any] = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "bigint": LongType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "decimal": DecimalType(10, 2),
    "date": DateType(),
    "datetime": DateType(),
    "timestamp": TimestampType(),
}


def map_spark_type(
    type_name: str,
    custom_mapping: Optional[Dict[str, Any]] = None,
    default_type: Any = None,
) -> Any:
    """
    Map a string type name to a PySpark DataType.

    This function converts string representations of data types to their
    corresponding PySpark DataType objects. It supports custom type mappings
    and a configurable default type.

    Args:
        type_name (str): The string name of the type to map (case-insensitive).
            Examples: "string", "int", "bigint", "double", "date", "timestamp"
        custom_mapping (Optional[Dict[str, Any]]): A dictionary of custom type
            mappings to override or extend the default mappings.
        default_type (Any): The default type to return if no mapping is found.
            Defaults to StringType() if not specified.

    Returns:
        Any: The corresponding PySpark DataType object.

    Supported Types:
        - string -> StringType()
        - int, integer -> IntegerType()
        - bigint, long -> LongType()
        - double, float -> DoubleType()
        - decimal -> DecimalType(10, 2)
        - date, datetime -> DateType()
        - timestamp -> TimestampType()

    Example:
        >>> map_spark_type("string")
        StringType()
        >>> map_spark_type("int")
        IntegerType()
        >>> map_spark_type("unknown")
        StringType()  # default type
        >>> # With custom mapping
        >>> custom = {"money": DecimalType(18, 4)}
        >>> map_spark_type("money", custom_mapping=custom)
        DecimalType(18, 4)
    """
    if default_type is None:
        default_type = StringType()

    # Merge default and custom mappings
    type_mapping = DEFAULT_TYPE_MAPPING.copy()
    if custom_mapping:
        type_mapping.update(custom_mapping)

    type_name_lower = type_name.lower().strip()

    # First, try exact match
    if type_name_lower in type_mapping:
        return type_mapping[type_name_lower]

    # Then, try partial match using regex
    for key, spark_type in type_mapping.items():
        if re.search(key, type_name_lower, re.IGNORECASE):
            return spark_type

    get_logger(__name__).debug(
        "No mapping found for type '%s', using default: %s", type_name, default_type
    )
    return default_type


def get_ordered_columns_from_schema(
    schema_fields: List[Dict[str, str]],
    additional_columns: Optional[List[str]] = None,
    destination_key: str = "destination",
) -> List[str]:
    """
    Get ordered column names from a schema definition.

    This function extracts column names from a list of schema field
    dictionaries and optionally appends additional columns.

    Args:
        schema_fields (List[Dict[str, str]]): A list of dictionaries
            representing schema fields. Each dictionary should contain
            the column name under the key specified by destination_key.
        additional_columns (Optional[List[str]]): Additional column names
            to append to the end of the list.
        destination_key (str): The dictionary key containing the column name.
            Defaults to "destination".

    Returns:
        List[str]: An ordered list of column names.

    Example:
        >>> schema = [
        ...     {"source": "id", "destination": "user_id"},
        ...     {"source": "nm", "destination": "user_name"},
        ... ]
        >>> get_ordered_columns_from_schema(schema)
        ['user_id', 'user_name']
        >>> get_ordered_columns_from_schema(
        ...     schema, additional_columns=["created_at", "updated_at"]
        ... )
        ['user_id', 'user_name', 'created_at', 'updated_at']
    """
    columns = [
        field.get(destination_key)
        for field in schema_fields
        if field.get(destination_key) is not None
    ]

    if additional_columns:
        columns.extend(additional_columns)

    return columns
