"""
Tests for logging configuration functionality.

This module contains tests to verify the logging configuration
feature works correctly with both standard and custom loggers.
"""

import logging
from unittest.mock import MagicMock

from dp_spark_utils import configure_logging, get_logger, reset_logging


class TestConfigureLogging:
    """Tests for configure_logging function."""

    def setup_method(self):
        """Reset logging state before each test."""
        reset_logging()

    def teardown_method(self):
        """Reset logging state after each test."""
        reset_logging()

    def test_configure_logging_with_custom_logger(self):
        """
        Test that a custom logger can be configured.
        """
        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        # Verify the custom logger is returned
        logger = get_logger("test_module")
        assert logger is custom_logger

    def test_configure_logging_with_none_uses_standard_logging(self):
        """
        Test that configuring with None uses standard logging.
        """
        configure_logging(logger=None)

        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_returns_standard_logger_by_default(self):
        """
        Test that get_logger returns a standard logger by default.
        """
        logger = get_logger("my_module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "my_module"

    def test_custom_logger_receives_log_calls(self):
        """
        Test that a custom logger receives log method calls.
        """
        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        logger = get_logger("test_module")
        logger.info("Test message")

        custom_logger.info.assert_called_once_with("Test message")

    def test_custom_logger_with_all_log_levels(self):
        """
        Test that custom logger works with all log levels.
        """
        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        logger = get_logger("test_module")

        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")

        custom_logger.debug.assert_called_once_with("Debug message")
        custom_logger.info.assert_called_once_with("Info message")
        custom_logger.warning.assert_called_once_with("Warning message")
        custom_logger.error.assert_called_once_with("Error message")
        custom_logger.critical.assert_called_once_with("Critical message")


class TestResetLogging:
    """Tests for reset_logging function."""

    def setup_method(self):
        """Reset logging state before each test."""
        reset_logging()

    def teardown_method(self):
        """Reset logging state after each test."""
        reset_logging()

    def test_reset_logging_clears_custom_logger(self):
        """
        Test that reset_logging clears the custom logger.
        """
        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        # Verify custom logger is active
        assert get_logger("test") is custom_logger

        # Reset and verify standard logging
        reset_logging()
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)

    def test_reset_logging_when_no_custom_logger(self):
        """
        Test that reset_logging works when no custom logger is set.
        """
        # Should not raise an exception
        reset_logging()
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)


class TestLoggingIntegrationWithModules:
    """Tests for logging integration with package modules."""

    def setup_method(self):
        """Reset logging state before each test."""
        reset_logging()

    def teardown_method(self):
        """Reset logging state after each test."""
        reset_logging()

    def test_dataframe_module_uses_custom_logger(self):
        """
        Test that dataframe module uses the configured custom logger.
        """
        from unittest.mock import MagicMock

        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        # Import and test a function that uses logging
        from dp_spark_utils.dataframe.operations import load_dataframe

        # Create a mock spark session
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.table.return_value = mock_df
        mock_df.cache.return_value = mock_df

        # Call the function
        load_dataframe(mock_spark, "test_db", "test_table")

        # Verify the custom logger was called
        custom_logger.info.assert_called()

    def test_hdfs_module_uses_custom_logger(self):
        """
        Test that hdfs module uses the configured custom logger.
        """
        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        from dp_spark_utils.hdfs.operations import hdfs_list_files

        # Create a mock spark session
        mock_spark = MagicMock()
        mock_fs = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs
        mock_fs.exists.return_value = False

        # Call the function with a non-existent path
        hdfs_list_files(mock_spark, "/nonexistent/path")

        # Verify the custom logger was called for warning
        custom_logger.warning.assert_called()

    def test_validation_module_uses_custom_logger(self):
        """
        Test that validation module uses the configured custom logger.
        """
        custom_logger = MagicMock()
        configure_logging(logger=custom_logger)

        from dp_spark_utils.validation.operations import validate_columns_match

        # Call with mismatching columns to trigger warning log
        validate_columns_match(
            ["col1", "col2", "extra_col"], ["col1", "col2", "missing_col"]
        )

        # Verify the custom logger was called for warnings
        assert custom_logger.warning.call_count >= 1


class TestCustomLoggerProtocol:
    """Tests for custom logger protocol compliance."""

    def setup_method(self):
        """Reset logging state before each test."""
        reset_logging()

    def teardown_method(self):
        """Reset logging state after each test."""
        reset_logging()

    def test_custom_logger_with_monitoring_like_interface(self):
        """
        Test that a Monitoring-like logger works correctly.

        This simulates a custom Monitoring class that the user might have.
        """

        class Monitoring:
            """Simulated custom monitoring/logging class."""

            def __init__(self):
                self.logs = []

            def debug(self, msg, *args):
                self.logs.append(("DEBUG", msg % args if args else msg))

            def info(self, msg, *args):
                self.logs.append(("INFO", msg % args if args else msg))

            def warning(self, msg, *args):
                self.logs.append(("WARNING", msg % args if args else msg))

            def error(self, msg, *args):
                self.logs.append(("ERROR", msg % args if args else msg))

            def critical(self, msg, *args):
                self.logs.append(("CRITICAL", msg % args if args else msg))

        # Create and configure custom monitoring
        monitoring = Monitoring()
        configure_logging(logger=monitoring)

        # Get logger and make calls
        logger = get_logger("my_module")
        logger.info("Processing started for %s", "test_table")
        logger.warning("Found %d missing records", 5)

        # Verify logs were captured
        assert len(monitoring.logs) == 2
        assert monitoring.logs[0] == ("INFO", "Processing started for test_table")
        assert monitoring.logs[1] == ("WARNING", "Found 5 missing records")


class TestLoggingImports:
    """Tests for logging function imports."""

    def test_logging_functions_available_at_package_level(self):
        """
        Test that logging functions are available at package level.
        """
        from dp_spark_utils import configure_logging, get_logger, reset_logging

        assert configure_logging is not None
        assert get_logger is not None
        assert reset_logging is not None

    def test_logging_functions_available_from_logging_config_module(self):
        """
        Test that logging functions can be imported from the module directly.
        """
        from dp_spark_utils.logging_config import (
            configure_logging,
            get_logger,
            reset_logging,
        )

        assert configure_logging is not None
        assert get_logger is not None
        assert reset_logging is not None
