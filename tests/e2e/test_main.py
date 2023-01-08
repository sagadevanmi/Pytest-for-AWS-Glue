import os
import logging
from glue.definitions import ROOT_DIR
from glue.utils.s3_operations import S3Utils
from glue.utils.file_loader import FileLoader
from glue.main import DataTransfer
import pytest
import yaml
from mock import patch
from pyspark.sql.types import *


@pytest.mark.usefixtures("s3_client", "secret_client", "spark_create")
class TestMain:
    """
    Class for testing glue job
    """

    @staticmethod
    def setup_mocked_infra(s3_client):
        """
        Static Function to create mock aws infrastructure
        :param s3_client: Mock S3 client
        """

        # Set env variable which can be used in place of job parameter to glue job
        bucket_config = "sd-pytest-bucket"
        key = "config/config.yaml"
        os.environ["config_file_path"] = "s3://" + bucket_config + "/" + key
        logging.info(os.environ["config_file_path"])


    @patch(
        "glue.main.DataTransfer.createContexts"
    )
    def test_main(
        self, mock_contexts, s3_client, secret_client, spark_create
    ):
        """
        e2e test function
        :param mock_contexts: "glue.main.DataTransfer.createContexts" patch object
        :param s3_client: Mock S3 client
        :param secret_client: Mock Secrets Manager client
        :param spark_create: Fixture for creating SparkContext, GlueContext, SparkSession objects
        """

        msg_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        datetime_format = "%Y-%m-%d %H:%M:%S"
        logging.basicConfig(format=msg_format, datefmt=datetime_format)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        execute = False
        try:
            self.setup_mocked_infra(s3_client)
            logger.info("Mock infra setup successful")
            (
                spark_context,
                glue_context,
                spark_session
            ) = spark_create
            mock_contexts.return_value = spark_context , glue_context , spark_session,
            
            logger.info("Starting to test DataTransfer process")
            obj = DataTransfer()
            obj.main()
            logger.info("DataTransfer process tested")


            prefix = "landing/postgres/Student/"
            # objs = s3_client.list_objects_v2(Bucket = 'sd-pytest-bucket', Prefix = prefix)
            # fileCount = objs['KeyCount']
            # logging.info(f"Number of Objects in landing folder for table Student: {fileCount}")

            # Download Student table csv file from mock s3 bucket to local machine
            S3Utils.download_from_s3_to_dir(s3_client, 'sd-pytest-bucket', 'landing/postgres/Student/', 'output')
            logger.info("Files downloaded from mock S3 bucket")
            
            # Read csv files from mock S3 bucket and assert with expected count and columns
            schema = eval("StructType([StructField('studentID',IntegerType(),False),StructField('firstname',StringType(),False),StructField('lastname',StringType(),True),StructField('address',StringType(),False),StructField('city',StringType(),False),StructField('state',StringType(),True),StructField('courseID',IntegerType(),False)])")
            path = "s3://sd-pytest-bucket/" + prefix
            actual_df_in_s3 = (
                        spark_session.read.format("csv")
                        .option("header", True)
                        .schema(schema)
                        .load(path)
            )

            logger.info(f"S3 Dataframe Columns : {actual_df_in_s3.columns}")
            execute = True

        except Exception as exc:
            exc = str(exc)
            logger.error(f"Exception in e2e pytest: {exc}")

        assert execute == True


"""
cd tests/e2e
pytest -o log_cli=TRUE --log-cli-level=INFO tests/e2e/test_main.py
docker cp .\pg_hba.conf postgres:/var/lib/postgresql/data/pg_hba.conf
docker restart postgres
"""