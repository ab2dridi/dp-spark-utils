"""
Date operations for data processing.

This module contains functions for date calculations and manipulations
commonly used in data processing pipelines, such as calculating
reporting periods and date boundaries.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, Union

logger = logging.getLogger(__name__)


def get_last_day_of_previous_month(
    reference_date: Optional[Union[date, datetime]] = None,
    date_format: str = "%Y%m%d",
) -> str:
    """
    Get the last day of the previous month.

    This function calculates the last day of the month before the reference
    date. It is commonly used for determining reporting period boundaries.

    Args:
        reference_date (Optional[Union[date, datetime]]): The reference date
            to calculate from. If None, uses today's date.
        date_format (str): The output date format string.
            Defaults to "%Y%m%d" (e.g., "20240131").

    Returns:
        str: The formatted date string representing the last day of the
            previous month.

    Example:
        >>> # Assuming today is 2024-02-15
        >>> get_last_day_of_previous_month()
        '20240131'
        >>> # With custom reference date
        >>> from datetime import date
        >>> get_last_day_of_previous_month(date(2024, 3, 15))
        '20240229'
        >>> # With custom format
        >>> get_last_day_of_previous_month(date_format="%Y-%m-%d")
        '2024-01-31'
    """
    if reference_date is None:
        reference_date = datetime.today()

    # Get the first day of the current month
    first_day_of_current_month = reference_date.replace(day=1)

    # Subtract one day to get the last day of previous month
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

    return last_day_of_previous_month.strftime(date_format)


def get_last_day_of_previous_two_months(
    reference_date: Optional[Union[date, datetime]] = None,
    date_format: str = "%Y%m%d",
) -> str:
    """
    Get the last day of two months before the reference date.

    This function calculates the last day of the month that is two months
    before the reference date. Useful for quarterly or historical comparisons.

    Args:
        reference_date (Optional[Union[date, datetime]]): The reference date
            to calculate from. If None, uses today's date.
        date_format (str): The output date format string.
            Defaults to "%Y%m%d" (e.g., "20231231").

    Returns:
        str: The formatted date string representing the last day of two
            months before the reference date.

    Example:
        >>> # Assuming today is 2024-03-15
        >>> get_last_day_of_previous_two_months()
        '20240131'
        >>> # With custom reference date
        >>> from datetime import date
        >>> get_last_day_of_previous_two_months(date(2024, 4, 20))
        '20240229'
        >>> # With custom format
        >>> get_last_day_of_previous_two_months(date_format="%d/%m/%Y")
        '31/01/2024'
    """
    if reference_date is None:
        reference_date = datetime.today()

    # Get the last day of previous month first
    first_day_of_current_month = reference_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

    # Then get the last day of the month before that
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    last_day_of_previous_two_months = first_day_of_previous_month - timedelta(days=1)

    return last_day_of_previous_two_months.strftime(date_format)
