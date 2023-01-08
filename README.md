# Pytest-for-AWS-Glue
# Writing pytests for an AWS Glue job which reads data from Postgres and dumps it to AWS S3 using PySpark and Docker

# To run:
cd glue

docker-compose up -d

docker cp .\pg_hba.conf postgres:/var/lib/postgresql/data/pg_hba.conf

docker restart postgres

# Attach VS Code to the running container sd_glue_pytest
# Run E2E test case
pytest -o log_cli=TRUE --log-cli-level=INFO tests/e2e/test_main.py

# Run all test cases
pytest -o log_cli=TRUE --log-cli-level=INFO tests/
