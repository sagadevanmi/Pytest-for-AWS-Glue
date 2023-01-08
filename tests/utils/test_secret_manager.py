from glue.utils.secret_manager_operations import SecretManagerOperations
from glue.definitions import ROOT_DIR
import pytest


@pytest.mark.usefixtures("moto_server", "secret_client")
class TestSecretManagerOperations():
	"""
	Class for testing SecretManagerOperations
	"""

	def test_secret(self):
		"""
		Function to test load_secret_manager_details()
		"""
		secrets = SecretManagerOperations.load_secret_manager_details(
			"source_secret", "us-west-2"
		)
		username = secrets["user_name"]
		password = secrets["password"]

		assert username == "postgres"
