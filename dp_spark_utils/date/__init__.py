"""
Date operations module for dp-spark-utils.

This module provides utilities for date calculations and manipulations
commonly used in data processing pipelines.

Functions:
    - get_last_day_of_previous_month: Get the last day of the previous month
    - get_last_day_of_previous_two_months: Get the last day of two months ago
"""

from dp_spark_utils.date.operations import (
    get_last_day_of_previous_month,
    get_last_day_of_previous_two_months,
)

__all__ = [
    "get_last_day_of_previous_month",
    "get_last_day_of_previous_two_months",
]
