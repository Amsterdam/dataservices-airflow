from datetime import timedelta
from environs import Env
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago


env = Env()

RECREATE_SCHEMA_SQL = """
    BEGIN;
    DROP SCHEMA IF EXISTS pte CASCADE;
    CREATE SCHEMA pte;
    COMMIT;
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": False,  # do not backfill
}

dag_config = Variable.get("dag_config", deserialize_json=True)
vsd_config = dag_config["vsd"]
pg_params = " ".join(
    [
        "-X",
        "--set",
        "ON_ERROR_STOP",
        "-h",
        env("POSTGRES_HOST"),
        "-p",
        env("POSTGRES_PORT"),
        "-U",
        env("POSTGRES_USER"),
        "-W",
        env("POSTGRES_PASSWORD"),
    ]
)

with DAG(
    "VSD",
    default_args=default_args,
    description="VSD",
    schedule_interval="*/15 * * * *",
) as dag:

    # Uses postgres_default connection, defined in env var
    recreate_schema = PostgresOperator(
        task_id="recreate_schema", sql=RECREATE_SCHEMA_SQL
    )

    fetch_dumps = []
    unzip_dumps = []
    load_dumps = []

    for ds_filename in vsd_config["afval"]["files"]:
        fetch_dumps.append(
            BashOperator(
                task_id=f"fetch_{ds_filename}",
                bash_command=f"swift download afval acceptance/{ds_filename}.zip "
                f"-o /tmp/{ds_filename}.zip",
            )
        )

        unzip_dumps.append(
            BashOperator(
                task_id=f"unzip_{ds_filename}",
                bash_command="unzip -o /tmp/{ds_filename}.zip -d /tmp",
            )
        )

        load_dumps.append(
            BashOperator(
                task_id=f"load_{ds_filename}",
                bash_command=f"psql {pg_params} < /tmp/{ds_filename}.backup",
            )
        )


for fetch, unzip, load in zip(fetch_dumps, unzip_dumps, load_dumps):
    fetch >> unzip >> load
recreate_schema >> load_dumps
