import logging
from glue.definitions import ROOT_DIR
from glue.utils.rdbms_operations import RdbmsUtils
from glue.utils.file_loader import FileLoader
from glue.utils.s3_operations import S3Utils
from glue.utils.secret_manager_operations import SecretManagerOperations
import yaml
import pytest


@pytest.mark.usefixtures("setup_rdbms", "moto_server", "s3_client", "secret_client", "spark_create")
class TestRdbmsUtils:
    """
    Class for testing RDBMSUtils
    """

    @staticmethod
    def setup_mocked_infra(s3_client):
        """
        Function to create S3Utils and RDBMSUtils object
        """
        bucket_name = "sd-pytest-bucket"
        schema_prefix = "schema/postgres/"
        landing_prefix = "landing/postgres/"

        s3_obj = S3Utils(bucket_name, schema_prefix, landing_prefix)
        logging.info("Created test S3Utils object")
        
        # Get secret from Secrets Manager
        region = "us-west-2"
        secret_name = "source_secret"
        secret_manager_response = (
            SecretManagerOperations.load_secret_manager_details(secret_name, region)
        )
        src_details = {
            "src_url": secret_manager_response.get("src_url"),
            "database": secret_manager_response.get("database"),
            "user_name": secret_manager_response.get("user_name"),
            "password": secret_manager_response.get("password"),
            "schema": secret_manager_response.get("schema")                
        }

        # Create RDBMSUtils object
        rdbms_obj = RdbmsUtils(**src_details) if src_details else None
        logging.info("Created test RDBMSUtils object")
        return s3_obj, rdbms_obj

    def test_load_data_frame(self, s3_client, spark_create):
        """test load_data_frame function"""

        s3_obj, rdbms_obj = self.setup_mocked_infra(s3_client)
        (
            spark_context,
            glue_context,
            spark_session,
        ) = spark_create
        source_df = rdbms_obj.load_dataframe(
            s3_obj,
            "Student",
            spark_session
        )

        logging.info(source_df.count())
        assert source_df is not None
        assert source_df.count() == 10
