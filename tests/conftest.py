import logging
from glue.utils.file_loader import FileLoader
from glue.definitions import ROOT_DIR
import yaml
from pyspark import SparkContext, SparkConf
from awsglue.context import GlueContext
import pytest
import boto3
from moto import mock_s3, mock_secretsmanager
from moto.server import ThreadedMotoServer
import pg8000 as pg
import sqlparse


@pytest.fixture(scope = "session", autouse = True)
def setup_rdbms():
    """
    Function to setup mock RDBMS database with all objects required for processing
    """
    try:
        con = None
        con = pg.connect(
                database = 'postgres',
                user = 'postgres',
                password = 'q2wer56tgu8u!',
                host = 'postgres',
                port = '5432',
            )
        cursor = con.cursor()
        logging.info("RDBMS Connection successful")

        postgres_ddl_path = ROOT_DIR + "/tests/sqlscripts/" + 'postgres_ddl.sql'
        file = open(postgres_ddl_path, 'r')

        statements = sqlparse.split(file.read())

        for statement in statements:
            # logging.info(statement)
            cursor.execute(statement)
            # logging.info("Execution successful")

    except Exception as exc:
        exc = str(exc)
        logging.error(f"Some exception occurred in function setup_rdbms: {exc}")

    finally:
        if con is not None:
            con.commit()
            cursor.close()
            file.close()

@pytest.fixture(scope = "session")
def get_sample_dataframe(spark_create):
    """
    Function to create a sample dataframe
    """
    (
        spark_context,
        glue_context,
        spark_session,
    ) = spark_create
    columns = ["studentID", "firstname", "lastname", "address", "city", "state", "courseID"]
    data = [(1, "SD", "K", "ndqekjhew", "Pune", "MH", "123"), 
            (2, "PD", "A", "ndqekjhew", "Pune", "MH", "123"), 
            (3, "AJ", "S", "ndqekjhew", "Pune", "MH", "123")]
    df = spark_session.createDataFrame(data).toDF(*columns)
    yield df

@pytest.fixture(scope = "session")
def moto_server():
    """
    Function to start moto server
    """
    server = ThreadedMotoServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope = "session")
def s3_client(moto_server):
    """
    Function to create mock S3 infrastructure
    """
    with mock_s3():

        s3_client = boto3.client("s3", endpoint_url = "http://localhost:5000")

        bucket_config = "sd-pytest-bucket"
        file_path_list_config = ["input", "config", "config.yaml"]
        config_key = "config/config.yaml"
        file_path_config = FileLoader.load_data(file_path_list_config)
        

        file_path_list_schema = [   
                                    ["input", "schema", "Professor.yaml"],
                                    ["input", "schema", "Student.yaml"],
                                    ["input", "schema", "Course.yaml"],
                                    ["input", "schema", "Dummy.yaml"]
                                ]
        schema_keys = ["schema/postgres/Professor.yaml", "schema/postgres/Student.yaml", 
                        "schema/postgres/Course.yaml", "schema/postgres/Dummy.yaml"]
        
        file_path_schema1 = FileLoader.load_data(file_path_list_schema[0])
        file_path_schema2 = FileLoader.load_data(file_path_list_schema[1])
        file_path_schema3 = FileLoader.load_data(file_path_list_schema[2])
        file_path_schema4 = FileLoader.load_data(file_path_list_schema[3])

        s3_client.create_bucket(Bucket = bucket_config)
        s3_client.upload_file(file_path_config, bucket_config, config_key)

        s3_client.upload_file(file_path_schema1, bucket_config, schema_keys[0])
        s3_client.upload_file(file_path_schema2, bucket_config, schema_keys[1])
        s3_client.upload_file(file_path_schema3, bucket_config, schema_keys[2])
        s3_client.upload_file(file_path_schema4, bucket_config, schema_keys[3])

        yield s3_client


@pytest.fixture(scope = "session")
def spark_create(moto_server):
    """Fixture for creating a spark context"""
    # config_ = SparkConf()
    # config_.set("spark.jars", ROOT_DIR + "/postgresql-42.1.2.jar")
    # spark_context = SparkContext(conf = config_)

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark_session = glue_context.spark_session
    # spark_session.conf.set("spark.jars", ROOT_DIR + "postgresql-42.2.20.jar")
    # config = spark_context.getConf().getAll()
    # logging.info(config)

    hadoop_conf = spark_session._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "mock")
    hadoop_conf.set("fs.s3a.secret.key", "mock")
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:5000")

    yield spark_context, glue_context, spark_session,
    
    if spark_context is not None:
        spark_context.stop()


@pytest.fixture(scope = "session")
def secret_client(moto_server):
    """
    Function to mock Secrets Manager and create a secret with Postgres connection details
    """
    with mock_secretsmanager():

        secret_client = boto3.client(
            "secretsmanager",
            region_name = "us-west-2",
            endpoint_url = "http://localhost:5000",
        )

        secret_client.create_secret(
            Name = "source_secret",
            SecretString = f'{{\
                                "user_name": "postgres", \
                                "password": "q2wer56tgu8u!", \
                                "src_url": "jdbc:postgresql://postgres:5432/postgres", \
                                "database": "postgres", \
                                "schema": "dbo" \
                            }}'
        )
        yield secret_client