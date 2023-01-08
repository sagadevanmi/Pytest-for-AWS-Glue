import os
import pytest
from glue import definitions
from glue.utils.file_loader import FileLoader


class TestFileLoader:
    """
    Class for testing FileLoader
    """
    
    def test_load_data(self):
        """
        test_load_data
        """
        test_folder_root = definitions.ROOT_DIR
        path_list = ["tests", "sample", "input"]
        file_path = FileLoader.load_data(path_list)
        assert file_path == os.path.join(test_folder_root, "tests", "sample", "input")


    def test_load_data_s3(self):
        """
        test_build_s3_path
        """
        path_list = ["tests", "sample", "input"]
        file_path = FileLoader.load_data_s3(path_list)
        assert file_path == "s3://tests/sample/input/"
