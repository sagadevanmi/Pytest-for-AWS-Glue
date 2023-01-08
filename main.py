import sys
import os
import logging
from glue.utils.rdbms_operations import RdbmsUtils
from glue.utils.s3_operations import S3Utils
from glue.utils.secret_manager_operations import SecretManagerOperations
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


class DataTransfer:
    """
    Class where all the processing takes place
    """

    def __init__(self):
        """
        Constructor
        :param glue_context: GlueContext object
        :param spark_context: GlueContext object
        :param spark_session: SparkSession object
        :param s3_obj: S3Utils object
        :param rdbms_obj: RDBMSUtils object
        :param tables_to_be_processed: List of tables to be processed
        :param tables_executed: Count of tables which have been processed successfully
        """
        self.glue_context = None
        self.spark_context = None
        self.spark_session = None
        self.s3_obj = None
        self.rdbms_obj = None
        self.tables_to_be_processed = None
        self.tables_executed = 0

        msg_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        datetime_format = "%Y-%m-%d %H:%M:%S"
        logging.basicConfig(format=msg_format, datefmt=datetime_format)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)


    def read_from_rdbms(self, tablename):
        """
        Function which calls load_data_frame RDBMS Util function
        :param tablename: Table which is currently being processed
        :return source_df: Spark dataframe which was read from RDBMS source
        """
        try:
            source_df = self.rdbms_obj.load_dataframe(self.s3_obj, tablename, self.spark_session)
            return source_df
        
        except Exception as exc:
            exc = str(exc)
            self.logger.error(f"Exception while reading from RDBMS in function read_from_rdbms: {exc}")
            raise exc


    def write_to_s3(self, prefix, source_df, tablename):
        """
        Function which calls write_dataframe S3 Util function
        :param prefix: prefix of file to be written in S3
        :param source_df: Spark dataframe which was read from RDBMS
        :param tablename: Table which is currently being processed
        :return True: Status of write to S3
        """
        try:
            self.logger.info(f"Trying to load data into S3 folder: {prefix}")
            path = self.s3_obj.write_dataframe(prefix, source_df, tablename)

            if path is not None:
                return True

        except Exception as exc:
            exc = str(exc)
            self.logger.error(f"Exception while writing to S3 in function write_to_s3: {exc}")
            raise exc


    def process(self):
        """
        Function which calls read_from_rdbms and write_to_s3 functions
        :return None
        """
        for tablename, details in self.tables_to_be_processed.items():

            try:
                # Process table only if active_flag is set to 'T'
                if details["active_flag"] == "T":
                    
                    # Load data from Postgres table into a Spark dataframe
                    source_df = self.read_from_rdbms(tablename)
                    self.logger.info(f"{source_df.show(5)}")

                    # Write the Spark dataframe to S3
                    if source_df:
                        prefix = self.s3_obj.landing_prefix + tablename + "/"
                        status = self.write_to_s3(prefix, source_df, tablename)
                        if status:
                            self.logger.info("Successfully loaded data into S3")
                    self.tables_executed += 1
                else:
                    self.tables_executed += 1
                    
            except Exception as exc:
                exc = str(exc)
                self.logger.error(f"Exception while processing table {tablename} in function process: {exc}")


    def main(self):
        """
        Function which creates required objects for processing and calls process() function
        :return None
        """
        try:
            self.logger.info("Starting the job execution")

            # Create context objects
            (
                self.spark_context,
                self.glue_context,
                self.spark_session,
            ) = DataTransfer.createContexts()
            self.logger.info("Created context objects")

            # Read config file from S3
            config_file_path = None
            try:
                config_file_path = getResolvedOptions(
                        sys.argv, ["config_file_path"]
                    ).get("config_file_path")
            except Exception:
                self.logger.info("Testing environment")
                pass
            if config_file_path is None:
                config_file_path = os.environ["config_file_path"]
                self.logger.info(config_file_path)
            config_dict = S3Utils.load_config_file(config_file_path)

            # List of tables to be processed
            self.tables_to_be_processed = config_dict["Table_List"]

            # Create S3Utils object
            bucket_name = config_dict["Bucket_Name"]
            schema_prefix = config_dict["Schema_Prefix"]
            landing_prefix = config_dict["Landing_Prefix"]
            self.s3_obj = S3Utils(bucket_name, schema_prefix, landing_prefix)
            self.logger.info("Created S3Utils object")

            # Get secret from Secrets Manager
            secret_manager_details = config_dict.get("Secrets_Manager")
            region = secret_manager_details.get("region_name")
            secret_name = secret_manager_details.get("source_secret_name")
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
            self.rdbms_obj = RdbmsUtils(**src_details) if src_details else None
            self.logger.info("Created RDBMSUtils object")

            # Start Actual Execution
            self.logger.info(f"Executing the RDBMS processor")
            self.process()
            self.logger.info(f"Number of tables in config file: {len(self.tables_to_be_processed)}")
            self.logger.info(f"Number of tables processed successfully: {self.tables_executed}")
            self.logger.info("Execution completed successfully")

        except Exception as exc:
            exc = str(exc)
            self.logger.info(f"Number of tables in config file: {len(self.tables_to_be_processed)}")
            self.logger.info(f"Number of tables processed successfully: {self.tables_executed}")
            self.logger.error(f"Some exception inside function main: {exc}")
            raise exc


    @staticmethod
    def createContexts():
        """
        Static Function to create SparkContext, GlueContext, SparkSession objects
        :return spark_context: SparkContext object
        :return glue_context: SparkContext object
        :return spark_session: SparkSession object
        """
        logging.info("Initializing the glue context object for job run")
        spark_context = SparkContext()
        glue_context = GlueContext(spark_context)
        spark_session = glue_context.spark_session
        logging.info("Glue context has been created successfully")
        return spark_context, glue_context, spark_session


if __name__ == "__main__":
    obj = DataTransfer()
    obj.main()