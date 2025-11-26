"""
Tests for Validation operations module.

This module contains unit tests for validation functions including
column matching and filename pattern validation.
"""

from dp_spark_utils.validation import (
    validate_columns_match,
    validate_filename_pattern,
)


class TestValidateColumnsMatch:
    """Tests for the validate_columns_match function."""

    def test_columns_match_exact(self):
        """
        Test that identical columns match.
        """
        source = ["id", "name", "email"]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is True
        assert len(extra) == 0
        assert len(missing) == 0

    def test_columns_match_case_insensitive(self):
        """
        Test case-insensitive column matching.
        """
        source = ["ID", "Name", "EMAIL"]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is True

    def test_columns_match_with_whitespace(self):
        """
        Test column matching with whitespace.
        """
        source = [" id ", "name", "email "]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is True

    def test_columns_extra_in_source(self):
        """
        Test detection of extra columns in source.
        """
        source = ["id", "name", "email", "phone"]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is False
        assert "phone" in extra
        assert len(missing) == 0

    def test_columns_missing_in_source(self):
        """
        Test detection of missing columns in source.
        """
        source = ["id", "name"]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is False
        assert len(extra) == 0
        assert "email" in missing

    def test_columns_both_extra_and_missing(self):
        """
        Test detection of both extra and missing columns.
        """
        source = ["id", "name", "phone"]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is False
        assert "phone" in extra
        assert "email" in missing

    def test_columns_case_sensitive_mode(self):
        """
        Test case-sensitive column matching.
        """
        source = ["ID", "name", "email"]
        target = ["id", "name", "email"]

        is_match, extra, missing = validate_columns_match(
            source, target, case_sensitive=True
        )

        assert is_match is False
        assert "ID" in extra or "id" in extra
        assert "id" in missing or "ID" in missing

    def test_columns_with_spaces_in_names(self):
        """
        Test column matching with internal spaces.
        """
        source = ["user id", "user name"]
        target = ["userid", "username"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is True

    def test_columns_empty_lists(self):
        """
        Test matching empty column lists.
        """
        is_match, extra, missing = validate_columns_match([], [])

        assert is_match is True
        assert len(extra) == 0
        assert len(missing) == 0

    def test_columns_empty_source(self):
        """
        Test empty source against non-empty target.
        """
        source = []
        target = ["id", "name"]

        is_match, extra, missing = validate_columns_match(source, target)

        assert is_match is False
        assert len(missing) == 2


class TestValidateFilenamePattern:
    """Tests for the validate_filename_pattern function."""

    def test_pattern_match_date_prefix(self):
        """
        Test matching date-prefixed filenames.
        """
        result = validate_filename_pattern(
            "20240131_export.csv", r"^\d{8}_export\.csv$"
        )
        assert result is True

    def test_pattern_no_match_date_prefix(self):
        """
        Test non-matching date-prefixed filenames.
        """
        result = validate_filename_pattern(
            "2024-01-31_export.csv", r"^\d{8}_export\.csv$"
        )
        assert result is False

    def test_pattern_match_version_number(self):
        """
        Test matching versioned filenames.
        """
        result = validate_filename_pattern(
            "data_v1.2.3.json", r"^data_v\d+\.\d+\.\d+\.json$"
        )
        assert result is True

    def test_pattern_match_simple_csv(self):
        """
        Test matching simple CSV filenames.
        """
        result = validate_filename_pattern("export.csv", r"^.*\.csv$")
        assert result is True

    def test_pattern_no_match_wrong_extension(self):
        """
        Test non-matching extension.
        """
        result = validate_filename_pattern("export.txt", r"^.*\.csv$")
        assert result is False

    def test_pattern_match_any_character(self):
        """
        Test pattern with any character matching.
        """
        result = validate_filename_pattern(
            "report_2024_Q1.xlsx", r"^report_\d{4}_Q\d\.xlsx$"
        )
        assert result is True

    def test_pattern_match_underscore_separated(self):
        """
        Test matching underscore-separated filenames.
        """
        result = validate_filename_pattern(
            "user_data_20240131.csv", r"^[a-z_]+_\d{8}\.csv$"
        )
        assert result is True

    def test_pattern_empty_filename(self):
        """
        Test matching against empty filename.
        """
        result = validate_filename_pattern("", r"^$")
        assert result is True

    def test_pattern_complex_regex(self):
        """
        Test complex regex pattern.
        """
        pattern = r"^(prod|dev|test)_[a-zA-Z0-9]+_\d{4}-\d{2}-\d{2}\.parquet$"
        assert (
            validate_filename_pattern("prod_userdata_2024-01-31.parquet", pattern)
            is True
        )
        assert (
            validate_filename_pattern("dev_sales_2024-12-25.parquet", pattern) is True
        )
        assert (
            validate_filename_pattern("staging_data_2024-01-31.parquet", pattern)
            is False
        )

    def test_pattern_case_sensitivity(self):
        """
        Test case-sensitive pattern matching.
        """
        result = validate_filename_pattern("Export.CSV", r"^export\.csv$")
        assert result is False

    def test_pattern_with_special_characters(self):
        """
        Test pattern with special characters in filename.
        """
        result = validate_filename_pattern(
            "data@2024#test.csv", r"^data@\d{4}#test\.csv$"
        )
        assert result is True
