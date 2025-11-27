"""
Logging configuration for dp-spark-utils.

This module provides logging configuration utilities that allow users
to inject their own custom logging system into the package.

Example:
    Using a custom logger:

    >>> from dp_spark_utils import configure_logging
    >>> from my_monitoring import Monitoring
    >>>
    >>> # Create your custom logger
    >>> my_logger = Monitoring()
    >>>
    >>> # Configure dp-spark-utils to use your logger
    >>> configure_logging(logger=my_logger)
    >>>
    >>> # Now all dp-spark-utils modules will use your logger
    >>> from dp_spark_utils import load_dataframe
    >>> df = load_dataframe(spark, "db", "table")  # Logs via your logger
"""

import logging
from typing import Any, Optional

# Module-level logger storage
_custom_logger: Optional[Any] = None


def configure_logging(logger: Optional[Any] = None) -> None:
    """
    Configure the logging system for dp-spark-utils.

    This function allows you to inject your own custom logger into the
    dp-spark-utils package. All modules in the package will use the
    configured logger for their logging operations.

    The custom logger should implement the standard logging interface
    with methods: debug, info, warning, error, and critical.

    Args:
        logger (Optional[Any]): A custom logger instance that implements
            the standard logging interface (debug, info, warning, error,
            critical methods). If None, the package will use Python's
            standard logging module with module-specific loggers.

    Returns:
        None

    Example:
        >>> # Using a custom Monitoring logger
        >>> from dp_spark_utils import configure_logging
        >>> from my_app.monitoring import Monitoring
        >>>
        >>> monitoring = Monitoring()
        >>> configure_logging(logger=monitoring)
        >>>
        >>> # Reset to default logging
        >>> configure_logging(logger=None)
        >>>
        >>> # Using a pre-configured standard logger
        >>> import logging
        >>> my_logger = logging.getLogger("my_app")
        >>> my_logger.setLevel(logging.DEBUG)
        >>> configure_logging(logger=my_logger)
    """
    global _custom_logger
    _custom_logger = logger


def get_logger(name: str) -> Any:
    """
    Get a logger for the specified module name.

    This function returns the custom logger if one has been configured,
    otherwise it returns a standard Python logger for the module.

    Args:
        name (str): The module name (typically __name__).

    Returns:
        Any: The configured custom logger or a standard Python logger.

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Operation completed successfully")
    """
    if _custom_logger is not None:
        return _custom_logger
    return logging.getLogger(name)


def reset_logging() -> None:
    """
    Reset the logging configuration to default.

    This function removes any custom logger configuration and reverts
    to using Python's standard logging module.

    Returns:
        None

    Example:
        >>> from dp_spark_utils import configure_logging, reset_logging
        >>> configure_logging(logger=my_custom_logger)
        >>> # ... use the package ...
        >>> reset_logging()  # Revert to standard logging
    """
    global _custom_logger
    _custom_logger = None
