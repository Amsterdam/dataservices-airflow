import datetime
from collections import namedtuple

import pendulum
import pytest
from airflow import DAG
from airflow.models import Connection
from airflow.settings import TIMEZONE
from psycopg2 import extras
from pytest_docker_tools import container, fetch

pytest_plugins = ["helpers_namespace"]


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


@pytest.fixture(scope="module")
def postgres_credentials():
    """Return a fixture that configures postgres credentials."""
    PostgresCredentials = namedtuple("PostgresCredentials", ["username", "password"])
    return PostgresCredentials("testuser", "testpass")


postgres_image = fetch(repository="postgres:11.1-alpine")  # type: ignore


postgres = container(  # type: ignore
    image="{postgres_image.id}",
    environment={
        "POSTGRES_USER": "{postgres_credentials.username}",
        "POSTGRES_PASSWORD": "{postgres_credentials.password}",
    },
    ports={"5432/tcp": None},
)


@pytest.fixture
def mock_pghook(mocker, postgres, postgres_credentials):
    """Returns a fixture that mocks Airflow PostgresHook."""

    def _apply_mock(PostgresHook):
        mocker.patch.object(
            PostgresHook,
            "get_connection",
            return_value=Connection(
                host="localhost",
                conn_type="postgres",
                login=postgres_credentials.username,
                password=postgres_credentials.password,
                port=postgres.ports["5432/tcp"][0],
            ),
        )

    return _apply_mock


@pytest.fixture
def pg_cursor(mocker, postgres, postgres_credentials, request):
    """Returns a fixture that provides a postgres cursor."""
    # Use the request.module to get PostgresHook
    # from the requesting module. By keeping this PostgresHook
    # dynamic, this fixture can be use to test different airflow modules.
    mocker.patch.object(
        request.module.PostgresHook,
        "get_connection",
        return_value=Connection(
            host="localhost",
            conn_type="postgres",
            login=postgres_credentials.username,
            password=postgres_credentials.password,
            port=postgres.ports["5432/tcp"][0],
        ),
    )
    hook = request.module.PostgresHook()
    with hook.get_conn() as conn:
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            yield cursor
