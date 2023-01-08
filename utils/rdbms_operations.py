import logging
from glue.utils.s3_operations import S3Utils


class RdbmsUtils:
    """
    Class for RDBMS util functions
    """

    def __init__(
        self,
        src_url = None,
        database = None,
        user_name = None,
        password = None,
        schema = None,
    ):
        """
        Constructor
        :param src_url: URL for the src database connection
        :param database: database name
        :param user_name: user name of the database
        :param password: password of the database
        :param schema: schema of database
        """
        self.src_url = src_url
        self.database = database
        self.user_name = user_name
        self.password = password
        self.schema = schema

        msg_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        datetime_format = "%Y-%m-%d %H:%M:%S"
        logging.basicConfig(format=msg_format, datefmt=datetime_format)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)


    def load_dataframe(
        self,
        s3_obj,
        table,
        spark_session = None,
    ):
        """
        Function to create spark dataframe by reading data from RDBMS source
        :param s3_obj: S#Utils object
        :param table: table name which needs to be loaded
        :param spark_session:  SparkSession object
        :return: spark dataframe
        """
        self.logger.info(f"Trying to read data for table: {table}")

        try:
            df_schema, col_list = s3_obj.read_schema(table)
        except Exception as exc:
            exc = str(exc)
            self.logger.error("Exception while trying to read schema file from S3 in function load_data_frame: {exc}")
            raise exc
        
        try:
            if spark_session is not None:
                if col_list:
                    table = self.schema + '.' + table
                    query = "SELECT {} FROM {}".format(col_list, table)
                    self.logger.info(f"Read Query: {query}")
                    
                    # Reading using spark session
                    data_frame = (
                        spark_session.read.format("jdbc")
                        .option("url", self.src_url)
                        .option("user", self.user_name)
                        .option("password", self.password)
                        .option("query", query)
                        .option("driver", "org.postgresql.Driver")  # Need to change for other SQL engines
                        .load()
                    )

                    self.logger.info(f"Dataframe Count : {data_frame.count()}")
                    self.logger.info(f"Dataframe Columns : {data_frame.columns}")

                return data_frame
            return None
        except Exception as exc:
            exc = str(exc)
            self.logger.error(f"Exception while trying to read data from RDBMS in function load_data_frame: {exc}")
            raise exc