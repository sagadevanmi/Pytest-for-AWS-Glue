
import os
import errno
import re
import logging
from glue.utils.date_utils import DateUtils
import boto3
import yaml
from pyspark.sql.types import *


class S3Utils:
    """
    Class which defines the read/write functions from/to S3 Bucket
    """

    def __init__(
        self,
        bucket_name = None,
        schema_prefix = None,
        landing_prefix = None,
    ):
        """
        Constructor
        :param bucket_name: s3 bucket name
        :param schema_prefix: location where schema files are stored
        :param landing_prefix: location where files read from source will be stored
        """
        self.bucket_name = bucket_name
        self.schema_prefix = schema_prefix
        self.landing_prefix = landing_prefix
        
        msg_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        datetime_format = "%Y-%m-%d %H:%M:%S"
        logging.basicConfig(format=msg_format, datefmt=datetime_format)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)


    @staticmethod
    def load_config_file(s3_path):
        """
        Static Function to read config file(.yaml) from S3 and return a dictionary
        :param s3_path: config file S3 path
        :return dict_data: config dictionary
        """
        # load the file from s3 using boto3 client
        pattern = re.compile(r"\/\/(?P<bucket>[a-z0-9][a-z0-9-]{1,61})\/?(?P<key>.*)")
        match = pattern.search(s3_path)

        bucket = match.group("bucket")
        key = match.group("key")

        s3_client = boto3.client("s3")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        yaml_data = obj["Body"].read().decode("utf-8")

        try:
            dict_data = yaml.safe_load(yaml_data)
        except yaml.YAMLError as exc:
            logging.error(f"Some exception while loading yaml file in function load_config_file: {exc}")
            raise exc

        return dict_data


    def read_schema(self, tablename):
        """
        Function to read schema from a yaml file and return Schema
        :param tablename: Table name
        :return schema: Spark schema of table being processed
        :return col_list: Source columns which need to be extracted from RDBMS source
        """
        key = self.schema_prefix + tablename + ".yaml"
        self.logger.info("Schema Key: " + key)
        s3_obj = boto3.client("s3")
        obj = s3_obj.get_object(Bucket = self.bucket_name, Key = key)
        yaml_data = obj["Body"].read().decode("utf-8")

        try:
            dict_data = yaml.safe_load(yaml_data)
        except yaml.YAMLError as exc:
            exc = str(exc)
            self.logger.error(f"Exception while loading yaml data in function read_schema: {exc}")
            raise exc
        
        schema = eval(dict_data[tablename][-1]["Schema"])
        self.logger.info("Schema: " + str(schema))
        col_list = dict_data[tablename][-1]["SrcColumnList"]
        return schema, col_list


    def write_dataframe(self, path, dataframe, tablename):
        """
        Function to write dataframe to S3
        :param dataframe: Dataframe which needs to be written to S3
        :param tablename: Table which is currently being processed
        :return path_key: S3 path key where data has been loaded
        """
        try:            
            partition, ts_for_filename = DateUtils.get_formatted_current_date()
            self.logger.info("Partition for writing to S3: " + partition)
            path_key = path + str(partition) + "/" 
            path = "s3://" + self.bucket_name + "/" + path_key
            self.logger.info("S3 write Path: " + path)
            schema, col_list = self.read_schema(tablename)

            dataframe.repartition(1).write.option("header", True).option(
                "schema", schema
            ).mode("Append").csv(path)
        except Exception as exc:
            exc = str(exc)
            self.logger.error(f"Exception while writing to S3 in function write_dataframe: {exc}")
            raise exc
        return path_key
    

    @staticmethod
    def assert_dir_exists(path):
        """
        Function to check if a directory exists in path. If not then create the directory.
        :param path: the path to check where directory exists or not
        """
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise


    @staticmethod
    def download_from_s3_to_dir(client, bucket, path, target):
        """
        Downloads recursively the given S3 path to the target local directory.
        :param client: S3 client to use.
        :param bucket: the name of the bucket to download from
        :param path: The S3 directory to download.
        :param target: the local directory to download the files to.
        """
        # Handle missing / at end of prefix
        if not path.endswith('/'):
            path += '/'

        paginator = client.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket = bucket, Prefix = path):
            # Download each file individually
            for key in result['Contents']:
                # Calculate relative path
                rel_path = key['Key'][len(path):]
                # Skip paths ending in /
                if not key['Key'].endswith('/'):
                    local_file_path = os.path.join(target, rel_path)
                    # Make sure directories exist
                    local_file_dir = os.path.dirname(local_file_path)
                    S3Utils.assert_dir_exists(local_file_dir)
                    client.download_file(bucket, key['Key'], local_file_path)