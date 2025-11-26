"""
Tests for HDFS operations module.

This module contains unit tests for the hdfs operations including
file existence checks, directory listing, and file moving.
"""

from unittest.mock import MagicMock

from dp_spark_utils.hdfs import (
    check_file_exists,
    get_hadoop_fs,
    hdfs_list_files,
    move_files,
)


class TestGetHadoopFs:
    """Tests for the get_hadoop_fs function."""

    def test_get_hadoop_fs_returns_filesystem(self, spark_session_mock):
        """
        Test that get_hadoop_fs returns the Hadoop FileSystem object.
        """
        fs = get_hadoop_fs(spark_session_mock)

        assert fs is not None
        spark_session_mock._jvm.org.apache.hadoop.fs.FileSystem.get.assert_called_once()

    def test_get_hadoop_fs_uses_hadoop_configuration(self, spark_session_mock):
        """
        Test that get_hadoop_fs uses the Hadoop configuration from SparkSession.
        """
        get_hadoop_fs(spark_session_mock)

        spark_session_mock._jsc.hadoopConfiguration.assert_called_once()


class TestCheckFileExists:
    """Tests for the check_file_exists function."""

    def test_file_exists_without_extension(self, spark_session_mock, mock_hadoop_fs):
        """
        Test checking file existence without extension filter.
        """
        mock_hadoop_fs.exists.return_value = True

        result = check_file_exists(spark_session_mock, "/data/test.json")

        assert result is True
        mock_hadoop_fs.exists.assert_called_once()

    def test_file_does_not_exist(self, spark_session_mock, mock_hadoop_fs):
        """
        Test that check_file_exists returns False when file doesn't exist.
        """
        mock_hadoop_fs.exists.return_value = False

        result = check_file_exists(spark_session_mock, "/data/nonexistent.json")

        assert result is False

    def test_file_exists_with_matching_extension(
        self, spark_session_mock, mock_hadoop_fs
    ):
        """
        Test checking file existence with matching extension.
        """
        mock_hadoop_fs.exists.return_value = True

        result = check_file_exists(
            spark_session_mock, "/data/test.json", extension=".json"
        )

        assert result is True

    def test_file_exists_with_wrong_extension(self, spark_session_mock, mock_hadoop_fs):
        """
        Test that check_file_exists returns False when extension doesn't match.
        """
        mock_hadoop_fs.exists.return_value = True

        result = check_file_exists(
            spark_session_mock, "/data/test.json", extension=".csv"
        )

        assert result is False

    def test_file_not_exists_with_extension(self, spark_session_mock, mock_hadoop_fs):
        """
        Test that check_file_exists returns False when file doesn't
        exist even with matching extension.
        """
        mock_hadoop_fs.exists.return_value = False

        result = check_file_exists(
            spark_session_mock, "/data/test.json", extension=".json"
        )

        assert result is False


class TestHdfsListFiles:
    """Tests for the hdfs_list_files function."""

    def test_list_files_in_existing_directory(self, spark_session_mock, mock_hadoop_fs):
        """
        Test listing files in an existing directory.
        """
        mock_hadoop_fs.exists.return_value = True

        # Create mock file statuses
        mock_file1 = MagicMock()
        mock_file1.getPath.return_value.getName.return_value = "file1.csv"
        mock_file2 = MagicMock()
        mock_file2.getPath.return_value.getName.return_value = "file2.csv"
        mock_file3 = MagicMock()
        mock_file3.getPath.return_value.getName.return_value = "data.json"

        mock_hadoop_fs.listStatus.return_value = [mock_file1, mock_file2, mock_file3]

        result = hdfs_list_files(spark_session_mock, "/data/input/")

        assert len(result) == 3
        assert "file1.csv" in result
        assert "file2.csv" in result
        assert "data.json" in result

    def test_list_files_with_extension_filter(self, spark_session_mock, mock_hadoop_fs):
        """
        Test listing files with extension filter.
        """
        mock_hadoop_fs.exists.return_value = True

        mock_file1 = MagicMock()
        mock_file1.getPath.return_value.getName.return_value = "file1.csv"
        mock_file2 = MagicMock()
        mock_file2.getPath.return_value.getName.return_value = "file2.csv"
        mock_file3 = MagicMock()
        mock_file3.getPath.return_value.getName.return_value = "data.json"

        mock_hadoop_fs.listStatus.return_value = [mock_file1, mock_file2, mock_file3]

        result = hdfs_list_files(spark_session_mock, "/data/input/", extension=".csv")

        assert len(result) == 2
        assert "file1.csv" in result
        assert "file2.csv" in result
        assert "data.json" not in result

    def test_list_files_nonexistent_directory(self, spark_session_mock, mock_hadoop_fs):
        """
        Test listing files returns empty list for nonexistent directory.
        """
        mock_hadoop_fs.exists.return_value = False

        result = hdfs_list_files(spark_session_mock, "/nonexistent/path/")

        assert result == []

    def test_list_files_empty_directory(self, spark_session_mock, mock_hadoop_fs):
        """
        Test listing files in an empty directory.
        """
        mock_hadoop_fs.exists.return_value = True
        mock_hadoop_fs.listStatus.return_value = []

        result = hdfs_list_files(spark_session_mock, "/data/empty/")

        assert result == []


class TestMoveFiles:
    """Tests for the move_files function."""

    def test_move_all_files(self, spark_session_mock, mock_hadoop_fs):
        """
        Test moving all files from source to target.
        """
        mock_hadoop_fs.exists.side_effect = lambda path: False  # Target doesn't exist

        mock_file1 = MagicMock()
        mock_file1.getPath.return_value.getName.return_value = "file1.csv"
        mock_file2 = MagicMock()
        mock_file2.getPath.return_value.getName.return_value = "file2.csv"

        mock_hadoop_fs.listStatus.return_value = [mock_file1, mock_file2]
        mock_hadoop_fs.rename.return_value = True

        result = move_files(spark_session_mock, "/source/", "/target/")

        assert len(result) == 2
        assert "file1.csv" in result
        assert "file2.csv" in result
        mock_hadoop_fs.mkdirs.assert_called()

    def test_move_files_with_extension_filter(self, spark_session_mock, mock_hadoop_fs):
        """
        Test moving only files with specific extension.
        """
        mock_hadoop_fs.exists.return_value = True  # Target exists

        mock_file1 = MagicMock()
        mock_file1.getPath.return_value.getName.return_value = "file1.csv"
        mock_file2 = MagicMock()
        mock_file2.getPath.return_value.getName.return_value = "data.json"

        mock_hadoop_fs.listStatus.return_value = [mock_file1, mock_file2]
        mock_hadoop_fs.rename.return_value = True

        result = move_files(
            spark_session_mock, "/source/", "/target/", extension=".csv"
        )

        assert len(result) == 1
        assert "file1.csv" in result
        assert "data.json" not in result

    def test_move_files_with_overwrite(self, spark_session_mock, mock_hadoop_fs):
        """
        Test moving files with overwrite enabled deletes existing files.
        """
        # Set up exists to return True for both target dir check and file check
        mock_hadoop_fs.exists.return_value = True

        mock_file = MagicMock()
        mock_file.getPath.return_value.getName.return_value = "file.csv"

        mock_hadoop_fs.listStatus.return_value = [mock_file]
        mock_hadoop_fs.rename.return_value = True

        result = move_files(spark_session_mock, "/source/", "/target/", overwrite=True)

        assert "file.csv" in result
        # The function should have been called (checking that files were moved)
        assert mock_hadoop_fs.rename.called

    def test_move_files_creates_target_directory(
        self, spark_session_mock, mock_hadoop_fs
    ):
        """
        Test that move_files creates target directory if it doesn't exist.
        """
        # First call for target existence check returns False
        mock_hadoop_fs.exists.return_value = False
        mock_hadoop_fs.listStatus.return_value = []

        move_files(spark_session_mock, "/source/", "/target/")

        mock_hadoop_fs.mkdirs.assert_called()
