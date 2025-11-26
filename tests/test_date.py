"""
Tests for Date operations module.

This module contains unit tests for date utility functions including
previous month calculations.
"""

from datetime import date, datetime

from dp_spark_utils.date import (
    get_last_day_of_previous_month,
    get_last_day_of_previous_two_months,
)


class TestGetLastDayOfPreviousMonth:
    """Tests for the get_last_day_of_previous_month function."""

    def test_last_day_of_previous_month_january(self):
        """
        Test getting last day of December from January.
        """
        reference = date(2024, 1, 15)
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20231231"

    def test_last_day_of_previous_month_march(self):
        """
        Test getting last day of February from March (leap year).
        """
        reference = date(2024, 3, 10)  # 2024 is a leap year
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20240229"

    def test_last_day_of_previous_month_march_non_leap(self):
        """
        Test getting last day of February from March (non-leap year).
        """
        reference = date(2023, 3, 10)  # 2023 is not a leap year
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20230228"

    def test_last_day_of_previous_month_may(self):
        """
        Test getting last day of April from May.
        """
        reference = date(2024, 5, 20)
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20240430"

    def test_last_day_of_previous_month_with_datetime(self):
        """
        Test with datetime object as reference.
        """
        reference = datetime(2024, 2, 15, 12, 30, 0)
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20240131"

    def test_last_day_of_previous_month_custom_format(self):
        """
        Test with custom date format.
        """
        reference = date(2024, 2, 15)
        result = get_last_day_of_previous_month(
            reference_date=reference, date_format="%Y-%m-%d"
        )
        assert result == "2024-01-31"

    def test_last_day_of_previous_month_iso_format(self):
        """
        Test with ISO date format.
        """
        reference = date(2024, 2, 15)
        result = get_last_day_of_previous_month(
            reference_date=reference, date_format="%d/%m/%Y"
        )
        assert result == "31/01/2024"

    def test_last_day_of_previous_month_first_of_month(self):
        """
        Test when reference date is first day of month.
        """
        reference = date(2024, 3, 1)
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20240229"

    def test_last_day_of_previous_month_last_of_month(self):
        """
        Test when reference date is last day of month.
        """
        reference = date(2024, 1, 31)
        result = get_last_day_of_previous_month(reference_date=reference)
        assert result == "20231231"


class TestGetLastDayOfPreviousTwoMonths:
    """Tests for the get_last_day_of_previous_two_months function."""

    def test_last_day_of_two_months_ago_march(self):
        """
        Test getting last day of January from March.
        """
        reference = date(2024, 3, 15)
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20240131"

    def test_last_day_of_two_months_ago_april(self):
        """
        Test getting last day of February from April (leap year).
        """
        reference = date(2024, 4, 10)  # 2024 is a leap year
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20240229"

    def test_last_day_of_two_months_ago_february(self):
        """
        Test getting last day of December from February.
        """
        reference = date(2024, 2, 15)
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20231231"

    def test_last_day_of_two_months_ago_january(self):
        """
        Test getting last day of November from January.
        """
        reference = date(2024, 1, 20)
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20231130"

    def test_last_day_of_two_months_ago_with_datetime(self):
        """
        Test with datetime object as reference.
        """
        reference = datetime(2024, 5, 15, 10, 0, 0)
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20240331"

    def test_last_day_of_two_months_ago_custom_format(self):
        """
        Test with custom date format.
        """
        reference = date(2024, 3, 15)
        result = get_last_day_of_previous_two_months(
            reference_date=reference, date_format="%Y-%m-%d"
        )
        assert result == "2024-01-31"

    def test_last_day_of_two_months_ago_european_format(self):
        """
        Test with European date format.
        """
        reference = date(2024, 3, 15)
        result = get_last_day_of_previous_two_months(
            reference_date=reference, date_format="%d/%m/%Y"
        )
        assert result == "31/01/2024"

    def test_last_day_of_two_months_ago_first_of_month(self):
        """
        Test when reference date is first day of month.
        """
        reference = date(2024, 4, 1)
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20240229"

    def test_last_day_of_two_months_ago_year_boundary(self):
        """
        Test crossing year boundary.
        """
        reference = date(2024, 1, 15)
        result = get_last_day_of_previous_two_months(reference_date=reference)
        assert result == "20231130"
