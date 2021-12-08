import datetime
import os
from urllib.parse import urlparse

import pendulum
import pytest
from airflow import DAG
from airflow.models.connection import Connection
from airflow.settings import TIMEZONE
from postgres_table_copy_operator import PostgresHook
from pytest_postgresql import factories

pytest_plugins = ["helpers_namespace"]

# We take the postgresql database credentials from an environment var DATABASE_URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://dataservices:insecure@localhost:5416/dataservices",
)

dsn_url = urlparse(DATABASE_URL)
PG_PORT = dsn_url.port
PG_USER = dsn_url.username
PG_PASSWD = dsn_url.password
PG_DBNAME = "test_dags"


@pytest.fixture
def test_dag():
    """Return fixture that sets up a testdag."""
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": pendulum.yesterday(tz=TIMEZONE)},
        schedule_interval=datetime.timedelta(days=1),
    )


@pytest.helpers.register  # type: ignore
def run_task(task, dag):
    """Return a fixture that runs an Airflow tas."""
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )


postgresql_in_docker = factories.postgresql_noproc(port=PG_PORT, user=PG_USER, password=PG_PASSWD)
postgresql = factories.postgresql("postgresql_in_docker", dbname="test_dags")


@pytest.fixture
def mock_pghook(mocker, request):
    """Mocks Airflow PostgresHook."""
    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            host="localhost",
            conn_type="postgres",
            login=PG_USER,
            password=PG_PASSWD,
            schema=PG_DBNAME,  # For airflow, `schema` is the name of the database
            port=PG_PORT,
        ),
    )
