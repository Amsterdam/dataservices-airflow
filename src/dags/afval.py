from environs import Env
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import pg_params, default_args

env = Env()

RECREATE_SCHEMA_SQL = """
    BEGIN;
    DROP SCHEMA IF EXISTS pte CASCADE;
    CREATE SCHEMA pte;
    COMMIT;
"""

RENAME_TABLES_SQL = """
    BEGIN;
    DROP TABLE IF EXISTS public.{{ params.ds_filename }};
    ALTER TABLE pte.{{ params.ds_filename }} SET SCHEMA public;
    COMMIT;
"""

dag_id = "huishoudelijkafval"
dag_config = Variable.get(dag_id, deserialize_json=True)

with DAG(
    "huishoudelijkafval",
    default_args=default_args,
    description="huishoudelijkafval",
    schedule_interval="@daily",
) as dag:

    # Uses postgres_default connection, defined in env var
    recreate_schema = PostgresOperator(
        task_id="recreate_schema", sql=RECREATE_SCHEMA_SQL
    )

    fetch_dumps = []
    unzip_dumps = []
    load_dumps = []
    rename_tables = []

    for ds_filename in dag_config["files"]:
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
                bash_command=f"unzip -o /tmp/{ds_filename}.zip -d /tmp",
            )
        )

        load_dumps.append(
            BashOperator(
                task_id=f"load_{ds_filename}",
                bash_command=f"psql {pg_params()} < /tmp/{ds_filename}.backup",
            )
        )
        rename_tables.append(
            PostgresOperator(
                task_id=f"rename_table_for_{ds_filename}",
                sql=RENAME_TABLES_SQL,
                params=dict(ds_filename=ds_filename),
            )
        )


for fetch, unzip, load, rename in zip(
    fetch_dumps, unzip_dumps, load_dumps, rename_tables
):
    fetch >> unzip >> load >> rename
recreate_schema >> fetch_dumps
