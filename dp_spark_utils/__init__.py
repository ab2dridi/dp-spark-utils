"""
dp_spark_utils package provides utility functions for working with Apache Spark.
"""

# Importing utility functions
from .data_preparation import prepare_data
from .data_analysis import analyze_data
from .visualization import plot_data

# Module initialization code (if any can go here)
# Initialization logic if necessary

# Exposing utility functions for easier access
__all__ = [
    "prepare_data",
    "analyze_data",
    "plot_data"
]