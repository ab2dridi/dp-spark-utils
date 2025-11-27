"""
Tests for dp-spark-utils package initialization.

This module contains tests to verify the package can be imported
and all expected functions are available.
"""


class TestPackageImports:
    """Tests for package-level imports."""

    def test_package_import(self):
        """
        Test that the main package can be imported.
        """
        import dp_spark_utils

        assert dp_spark_utils is not None

    def test_package_version(self):
        """
        Test that package version is defined.
        """
        from dp_spark_utils import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)

    def test_hdfs_functions_available(self):
        """
        Test that HDFS functions are available at package level.
        """
        from dp_spark_utils import (
            check_file_exists,
            get_hadoop_fs,
            hdfs_list_files,
            move_files,
        )

        assert get_hadoop_fs is not None
        assert check_file_exists is not None
        assert hdfs_list_files is not None
        assert move_files is not None

    def test_hive_functions_available(self):
        """
        Test that Hive functions are available at package level.
        """
        from dp_spark_utils import (
            check_table_exists,
            get_columns_map,
        )

        assert check_table_exists is not None
        assert get_columns_map is not None

    def test_dataframe_functions_available(self):
        """
        Test that DataFrame functions are available at package level.
        """
        from dp_spark_utils import (
            load_dataframe,
            rename_columns,
            repartition_dataframe,
            write_dataframe_csv,
        )

        assert load_dataframe is not None
        assert repartition_dataframe is not None
        assert write_dataframe_csv is not None
        assert rename_columns is not None

    def test_schema_functions_available(self):
        """
        Test that schema functions are available at package level.
        """
        from dp_spark_utils import (
            get_ordered_columns_from_schema,
            map_spark_type,
        )

        assert map_spark_type is not None
        assert get_ordered_columns_from_schema is not None

    def test_date_functions_available(self):
        """
        Test that date functions are available at package level.
        """
        from dp_spark_utils import (
            get_last_day_of_previous_month,
            get_last_day_of_previous_two_months,
        )

        assert get_last_day_of_previous_month is not None
        assert get_last_day_of_previous_two_months is not None

    def test_validation_functions_available(self):
        """
        Test that validation functions are available at package level.
        """
        from dp_spark_utils import (
            validate_columns_match,
            validate_filename_pattern,
        )

        assert validate_columns_match is not None
        assert validate_filename_pattern is not None

    def test_logging_functions_available(self):
        """
        Test that logging configuration functions are available at package level.
        """
        from dp_spark_utils import (
            configure_logging,
            get_logger,
            reset_logging,
        )

        assert configure_logging is not None
        assert get_logger is not None
        assert reset_logging is not None


class TestSubmoduleImports:
    """Tests for submodule imports."""

    def test_hdfs_module_import(self):
        """
        Test that hdfs module can be imported separately.
        """
        from dp_spark_utils.hdfs import (  # noqa: F401
            check_file_exists,
            get_hadoop_fs,
            hdfs_list_files,
            move_files,
        )

        assert get_hadoop_fs is not None

    def test_hive_module_import(self):
        """
        Test that hive module can be imported separately.
        """
        from dp_spark_utils.hive import (  # noqa: F401
            check_table_exists,
            get_columns_map,
        )

        assert check_table_exists is not None

    def test_dataframe_module_import(self):
        """
        Test that dataframe module can be imported separately.
        """
        from dp_spark_utils.dataframe import (  # noqa: F401
            load_dataframe,
            rename_columns,
            repartition_dataframe,
            write_dataframe_csv,
        )

        assert load_dataframe is not None

    def test_schema_module_import(self):
        """
        Test that schema module can be imported separately.
        """
        from dp_spark_utils.schema import (  # noqa: F401
            get_ordered_columns_from_schema,
            map_spark_type,
        )

        assert map_spark_type is not None

    def test_date_module_import(self):
        """
        Test that date module can be imported separately.
        """
        from dp_spark_utils.date import (  # noqa: F401
            get_last_day_of_previous_month,
            get_last_day_of_previous_two_months,
        )

        assert get_last_day_of_previous_month is not None

    def test_validation_module_import(self):
        """
        Test that validation module can be imported separately.
        """
        from dp_spark_utils.validation import (  # noqa: F401
            validate_columns_match,
            validate_filename_pattern,
        )

        assert validate_columns_match is not None
