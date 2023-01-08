
import logging
from glue.utils.s3_operations import S3Utils
from glue.utils.file_loader import FileLoader
from glue.utils.rdbms_operations import RdbmsUtils
from glue.definitions import ROOT_DIR
import pytest
import yaml
from botocore.exceptions import ClientError


@pytest.mark.usefixtures("moto_server", "s3_client", "get_sample_dataframe")
class TestS3Utils:
    """
    Class for testing S3Utils
    """
    
    @staticmethod
    def setup_mocked_infra(s3_client):
        """
        Function to create S3Utils object
        """
        bucket_name = "sd-pytest-bucket"
        schema_prefix = "schema/postgres/"
        landing_prefix = "landing/postgres/"

        s3_obj = S3Utils(bucket_name, schema_prefix, landing_prefix)
        logging.info("Created test S3Utils object")

        return s3_obj


    def test_load_config(self, s3_client):
        """test load_config_file function"""
        s3_path = "s3://sd-pytest-bucket/config/config.yaml"
        config_dict = S3Utils.load_config_file(s3_path)
        assert config_dict is not None


    def test_read_schema(self, s3_client):
        """test read_schema function"""
        s3_obj = self.setup_mocked_infra(s3_client)
        s3_path = "s3://sd-pytest-bucket/schema/Student.yaml"
        config_dict = s3_obj.read_schema("Student")
        assert config_dict is not None


    def test_write_dataframe(self, moto_server, s3_client, get_sample_dataframe):
        """Test function for write dataframe"""
        s3_obj = self.setup_mocked_infra(s3_client)
        df = get_sample_dataframe
        path_key = s3_obj.write_dataframe("test/sample/", df, "Student")
        logging.info(path_key)
        assert path_key is not None
