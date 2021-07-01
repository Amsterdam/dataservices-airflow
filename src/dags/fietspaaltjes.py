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
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_fietspaaltjes import import_fietspaaltjes
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

DAG_ID: Final = "fietspaaltjes"
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
SQL_PATH: Final = Path(__file__).resolve().parents[0] / "sql"
TMP_TABLE_PREFIX: Final = "tmp_"
TABLE: Final = f"{DAG_ID}_fietspaaltjes"

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

    mkdir = mk_dir(TMP_DIR, clean_if_exists=True)

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

    rm_tmp_tables = PostgresOperator(
        task_id="rm_tmp_tables",
        sql="DROP TABLE IF EXISTS {tables} CASCADE".format(tables=f"{TMP_TABLE_PREFIX}{TABLE}"),
    )

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DAG_ID,
        ind_extra_index=True,
    )

    postgres_create_tables_like = PostgresTableCopyOperator(
        task_id=f"postgres_create_tables_like_{TABLE}",
        source_table_name=TABLE,
        target_table_name=f"{TMP_TABLE_PREFIX}{TABLE}",
        # Only copy table definitions. Don't do anything else.
        truncate_target=False,
        copy_data=False,
        drop_source=False,
    )

    fill_table = PostgresFilesOperator(
        task_id="fill_table",
        sql_files=[
            TMP_DIR.joinpath(DAG_ID).with_suffix(".sql").as_posix(),
        ],
    )

    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table_for_{TABLE}",
        new_table_name=TABLE,
        old_table_name=f"{TMP_TABLE_PREFIX}{TABLE}",
        cascade=True,
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    (
        slack_at_start
        >> mkdir
        >> fetch_json
        >> create_sql
        >> rm_tmp_tables
        >> sqlalchemy_create_objects_from_schema
        >> postgres_create_tables_like
        >> fill_table
        >> rename_table
        >> grant_db_permissions
    )
