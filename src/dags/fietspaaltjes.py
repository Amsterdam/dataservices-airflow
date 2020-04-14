import pathlib
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from http_fetch_operator import HttpFetchOperator

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)
from importscripts.import_fietspaaltjes import import_fietspaaltjes
from common.sql import SQL_TABLE_RENAME

dag_id = "fietspaaltjes"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
# dag_config = Variable.get(dag_id, deserialize_json=True)


with DAG(dag_id, default_args=default_args, template_searchpath=["/"]) as dag:

    tmp_dir = f"/tmp/{dag_id}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    fetch_json = HttpFetchOperator(
        task_id="fetch_json",
        endpoint="mladvies/data_export.json",
        http_conn_id="fietspaaltjes_conn_id",
        tmp_file=f"{tmp_dir}/{dag_id}.json",
    )

    create_sql = PythonOperator(
        task_id="create_sql",
        python_callable=import_fietspaaltjes,
        op_args=[f"{tmp_dir}/{dag_id}.json", f"{tmp_dir}/{dag_id}.sql",],
    )

    create_and_fill_table = PostgresOperator(
        task_id="create_and_fill_table",
        sql=[f"{sql_path}/fietspaaltjes_create.sql", f"{tmp_dir}/{dag_id}.sql"],
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params=dict(tablename="fietspaaltjes", pk="pkey"),
    )

slack_at_start >> fetch_json >> create_sql >> create_and_fill_table >> rename_table
