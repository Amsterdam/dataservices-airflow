import os
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.operators.python import PythonOperator
from common import SHARED_DIR, MessageOperator, default_args
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_fietspaaltjes import import_fietspaaltjes
from postgres_files_operator import PostgresFilesOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_BOR"]

DAG_ID: Final = "fietspaaltjes_az"
DATASET_ID: Final = "fietspaaltjes"
TMP_DIR: Final = Path(SHARED_DIR) / DATASET_ID
SQL_PATH: Final = Path(__file__).resolve().parents[0] / "sql"
TMP_TABLE_PREFIX: Final = "tmp_"
TABLE: Final = f"{DATASET_ID}_fietspaaltjes"

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    mkdir = mk_dir(TMP_DIR, clean_if_exists=True)

    fetch_json = HttpFetchOperator(
        task_id="fetch_json",
        endpoint="mladvies/data_export.json",
        http_conn_id="fietspaaltjes_conn_id",
        tmp_file=TMP_DIR.joinpath(DATASET_ID).with_suffix(".json").as_posix(),
    )

    create_sql = PythonOperator(
        task_id="create_sql",
        python_callable=import_fietspaaltjes,
        op_args=[
            TMP_DIR.joinpath(DATASET_ID).with_suffix(".json").as_posix(),
            TMP_DIR.joinpath(DATASET_ID).with_suffix(".sql").as_posix(),
        ],
    )

    rm_tmp_tables = PostgresOnAzureOperator(
        task_id="rm_tmp_tables",
        sql="DROP TABLE IF EXISTS {tables} CASCADE".format(tables=f"{TMP_TABLE_PREFIX}{TABLE}"),
    )

    # TODO: does not cope with array like data types. So
    # the table is not fully created (missing columns).
    # Fix this in schema-tools
    # sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
    #     task_id="sqlalchemy_create_objects_from_schema",
    #     data_schema_name=DAG_ID,
    #     ind_extra_index=True,
    # )

    postgres_create_tables_like = PostgresTableCopyOperator(
        task_id=f"postgres_create_tables_like_{TABLE}",
        dataset_name_lookup=DATASET_ID,
        dataset_name=DATASET_ID,
        source_table_name=TABLE,
        target_table_name=f"{TMP_TABLE_PREFIX}{TABLE}",
        # Only copy table definitions. Don't do anything else.
        truncate_target=False,
        copy_data=False,
        drop_source=False,
    )

    fill_table = PostgresFilesOperator(
        task_id="fill_table",
        dataset_name=DATASET_ID,
        sql_files=[
            TMP_DIR.joinpath(DATASET_ID).with_suffix(".sql").as_posix(),
        ],
    )

    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table_for_{TABLE}",
        new_table_name=TABLE,
        old_table_name=f"{TMP_TABLE_PREFIX}{TABLE}",
        cascade=True,
    )

    # Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

    (
        slack_at_start
        >> mkdir
        >> fetch_json
        >> create_sql
        >> rm_tmp_tables
        >> postgres_create_tables_like
        >> fill_table
        >> rename_table
        >> grant_db_permissions
    )
