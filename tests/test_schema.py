"""
Tests for Schema operations module.

This module contains unit tests for schema operations including
type mapping and column ordering.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from dp_spark_utils.schema import (
    get_ordered_columns_from_schema,
    map_spark_type,
)


class TestMapSparkType:
    """Tests for the map_spark_type function."""

    def test_map_string_type(self):
        """
        Test mapping 'string' to StringType.
        """
        result = map_spark_type("string")
        assert isinstance(result, StringType)

    def test_map_int_type(self):
        """
        Test mapping 'int' to IntegerType.
        """
        result = map_spark_type("int")
        assert isinstance(result, IntegerType)

    def test_map_integer_type(self):
        """
        Test mapping 'integer' to IntegerType.
        """
        result = map_spark_type("integer")
        assert isinstance(result, IntegerType)

    def test_map_bigint_type(self):
        """
        Test mapping 'bigint' to LongType.
        """
        result = map_spark_type("bigint")
        assert isinstance(result, LongType)

    def test_map_long_type(self):
        """
        Test mapping 'long' to LongType.
        """
        result = map_spark_type("long")
        assert isinstance(result, LongType)

    def test_map_double_type(self):
        """
        Test mapping 'double' to DoubleType.
        """
        result = map_spark_type("double")
        assert isinstance(result, DoubleType)

    def test_map_float_type(self):
        """
        Test mapping 'float' to DoubleType.
        """
        result = map_spark_type("float")
        assert isinstance(result, DoubleType)

    def test_map_decimal_type(self):
        """
        Test mapping 'decimal' to DecimalType.
        """
        result = map_spark_type("decimal")
        assert isinstance(result, DecimalType)

    def test_map_date_type(self):
        """
        Test mapping 'date' to DateType.
        """
        result = map_spark_type("date")
        assert isinstance(result, DateType)

    def test_map_datetime_type(self):
        """
        Test mapping 'datetime' to DateType.
        """
        result = map_spark_type("datetime")
        assert isinstance(result, DateType)

    def test_map_timestamp_type(self):
        """
        Test mapping 'timestamp' to TimestampType.
        """
        result = map_spark_type("timestamp")
        assert isinstance(result, TimestampType)

    def test_map_unknown_type_returns_default(self):
        """
        Test that unknown types return default StringType.
        """
        result = map_spark_type("unknown_type")
        assert isinstance(result, StringType)

    def test_map_type_case_insensitive(self):
        """
        Test that type mapping is case-insensitive.
        """
        assert isinstance(map_spark_type("STRING"), StringType)
        assert isinstance(map_spark_type("String"), StringType)
        assert isinstance(map_spark_type("INT"), IntegerType)
        assert isinstance(map_spark_type("Date"), DateType)

    def test_map_type_with_whitespace(self):
        """
        Test that type mapping handles whitespace.
        """
        result = map_spark_type("  string  ")
        assert isinstance(result, StringType)

    def test_map_type_with_custom_mapping(self):
        """
        Test mapping with custom type mapping.
        """
        custom = {"money": DecimalType(18, 4)}
        result = map_spark_type("money", custom_mapping=custom)
        assert isinstance(result, DecimalType)

    def test_map_type_custom_default(self):
        """
        Test mapping with custom default type.
        """
        result = map_spark_type("unknown", default_type=IntegerType())
        assert isinstance(result, IntegerType)

    def test_map_type_partial_match(self):
        """
        Test that partial matching works for type names.
        """
        # Types containing 'bigint' should match bigint mapping
        result = map_spark_type("bigint")
        assert isinstance(result, LongType)


class TestGetOrderedColumnsFromSchema:
    """Tests for the get_ordered_columns_from_schema function."""

    def test_basic_column_ordering(self):
        """
        Test extracting ordered columns from schema.
        """
        schema = [
            {"source": "id", "destination": "user_id"},
            {"source": "nm", "destination": "user_name"},
            {"source": "email", "destination": "user_email"},
        ]

        result = get_ordered_columns_from_schema(schema)

        assert result == ["user_id", "user_name", "user_email"]

    def test_column_ordering_with_additional_columns(self):
        """
        Test extracting columns with additional columns appended.
        """
        schema = [
            {"source": "id", "destination": "user_id"},
            {"source": "name", "destination": "user_name"},
        ]

        result = get_ordered_columns_from_schema(
            schema, additional_columns=["created_at", "updated_at"]
        )

        assert result == ["user_id", "user_name", "created_at", "updated_at"]

    def test_column_ordering_empty_schema(self):
        """
        Test extracting columns from empty schema.
        """
        result = get_ordered_columns_from_schema([])
        assert result == []

    def test_column_ordering_empty_with_additional(self):
        """
        Test extracting columns from empty schema with additional columns.
        """
        result = get_ordered_columns_from_schema([], additional_columns=["new_col"])
        assert result == ["new_col"]

    def test_column_ordering_custom_key(self):
        """
        Test extracting columns using custom destination key.
        """
        schema = [
            {"src": "id", "target": "user_id"},
            {"src": "name", "target": "user_name"},
        ]

        result = get_ordered_columns_from_schema(schema, destination_key="target")

        assert result == ["user_id", "user_name"]

    def test_column_ordering_missing_destination(self):
        """
        Test that missing destination values are skipped.
        """
        schema = [
            {"source": "id", "destination": "user_id"},
            {"source": "name"},  # Missing destination
            {"source": "email", "destination": "user_email"},
        ]

        result = get_ordered_columns_from_schema(schema)

        assert result == ["user_id", "user_email"]

    def test_column_ordering_none_values(self):
        """
        Test that None destination values are skipped.
        """
        schema = [
            {"source": "id", "destination": "user_id"},
            {"source": "name", "destination": None},
            {"source": "email", "destination": "user_email"},
        ]

        result = get_ordered_columns_from_schema(schema)

        assert result == ["user_id", "user_email"]
