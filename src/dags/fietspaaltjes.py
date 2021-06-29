from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from common.sql import SQL_TABLE_RENAME
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_fietspaaltjes import import_fietspaaltjes
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator

DAG_ID: Final = "fietspaaltjes"
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
SQL_PATH: Final = Path(__file__).resolve().parents[0] / "sql"

with DAG(
    DAG_ID,
    default_args=default_args,
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    fetch_json = HttpFetchOperator(
        task_id="fetch_json",
        endpoint="mladvies/data_export.json",
        http_conn_id="fietspaaltjes_conn_id",
        tmp_file=TMP_DIR.joinpath(DAG_ID).with_suffix(".json").as_posix(),
    )

    create_sql = PythonOperator(
        task_id="create_sql",
        python_callable=import_fietspaaltjes,
        op_args=[
            TMP_DIR.joinpath(DAG_ID).with_suffix(".json").as_posix(),
            TMP_DIR.joinpath(DAG_ID).with_suffix(".sql").as_posix(),
        ],
    )

    create_and_fill_table = PostgresFilesOperator(
        task_id="create_and_fill_table",
        sql_files=[
            SQL_PATH.joinpath("fietspaaltjes_create.sql").as_posix(),
            TMP_DIR.joinpath(DAG_ID).with_suffix(".sql").as_posix(),
        ],
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params={"tablename": f"{DAG_ID}_{DAG_ID}", "pk": "pkey"},
    )

    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        rename_indexes=False,
        pg_schema="public",
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

(
    slack_at_start
    >> fetch_json
    >> create_sql
    >> create_and_fill_table
    >> rename_table
    >> provenance_translation
    >> grant_db_permissions
)
