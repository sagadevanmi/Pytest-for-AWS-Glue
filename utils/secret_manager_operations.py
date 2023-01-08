import json
import logging
import boto3
from botocore.exceptions import ClientError


class SecretManagerOperations:
    """
    Class for handling secrets manager operations
    """

    @staticmethod
    def load_secret_manager_details(secret_name, region_name):
        """
        Static Function to retrieve secrets from secrets manager
        :param secret_name: name of the secret which need to be fetched
        :param region_name: region where the service is running
        :return: Secret
        """
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            if get_secret_value_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                secret_string = get_secret_value_response["SecretString"]
                secret_manager_details = json.loads(secret_string)
                return secret_manager_details

        except ClientError as exc:
            logging.errro(f"Some exception occurred while retrieving secret in function load_secret_manager_details: {exc}")
            raise exc