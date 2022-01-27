import shutil
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    EPHEMERAL_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from postgres_insert_csv_operator import FileTable, PostgresInsertCsvOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from sql.bbga_sqlite_transform import BBGA_SQLITE_TRANSFORM_SCRIPT
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

FileStem = str
UrlPath = str

DAG_ID: Final = "bbga"
EXPORT_DIR: Final = Path(EPHEMERAL_DIR) / DAG_ID
SQLITE_CSV_TRANSFORM_FILE: Final = EXPORT_DIR / "transform_csv_files.sqlite3"
TMP_TABLE_PREFIX: Final = "tmp_"
VARS: Final = Variable.get(DAG_ID, deserialize_json=True)
data_endpoints: dict[FileStem, UrlPath] = VARS["data_endpoints"]
table_mappings: dict[FileStem, str] = VARS["table_mappings"]

assert set(data_endpoints.keys()) == set(
    table_mappings.keys()
), "Both mappings should have the same set of keys."


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    mkdir = mk_dir(EXPORT_DIR)

    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_stem}",
            endpoint=url_path,
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=EXPORT_DIR / f"{file_stem}.csv",
        )
        for file_stem, url_path in data_endpoints.items()
    ]

    def rm_tmp_tables(task_id_postfix: str) -> PostgresOperator:
        """Remove temporary tables."""
        return PostgresOperator(
            task_id=f"rm_tmp_tables{task_id_postfix}",
            sql="DROP TABLE IF EXISTS {tables}".format(
                tables=", ".join(map(lambda t: f"{TMP_TABLE_PREFIX}{t}", table_mappings.values()))
            ),
        )

    rm_tmp_tables_pre = rm_tmp_tables("_pre")

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema", data_schema_name=DAG_ID
    )

    postgres_create_tables_like = [
        PostgresTableCopyOperator(
            task_id=f"postgres_create_tables_like_{table}",
            source_table_name=table,
            target_table_name=f"{TMP_TABLE_PREFIX}{table}",
            # Only copy table definitions. Don't do anything else.
            truncate_target=False,
            drop_target_if_unequal=True,
            copy_data=False,
            drop_source=False,
        )
        for table in table_mappings.values()
    ]

    def _create_sqlite_transform_file(file: Path, contents: str) -> None:
        with file.open(mode="w") as f:
            f.write(contents)

    create_sqlite_transform_file = PythonOperator(
        task_id="create_sqlite_transform_file",
        python_callable=_create_sqlite_transform_file,
        op_kwargs={"file": SQLITE_CSV_TRANSFORM_FILE, "contents": BBGA_SQLITE_TRANSFORM_SCRIPT},
    )

    transform_csv_files = BashOperator(
        task_id="transform_csv_files",
        bash_command=f"cd {EXPORT_DIR}; sqlite3 bbga.db < {SQLITE_CSV_TRANSFORM_FILE}",
    )

    data = tuple(
        FileTable(file=EXPORT_DIR / f"{file_stem}.csv", table=f"{TMP_TABLE_PREFIX}{table}")
        for file_stem, table in table_mappings.items()
    )
    postgres_insert_csv = PostgresInsertCsvOperator(task_id="postgres_insert_csv", data=data)

    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_tables_for_{table}",
            new_table_name=table,
            old_table_name=f"{TMP_TABLE_PREFIX}{table}",
            cascade=True,
        )
        for table in table_mappings.values()
    ]

    rm_tmp_tables_post = rm_tmp_tables("_post")

    rm_tmp_dir = PythonOperator(
        task_id="rm_tmp_dir", python_callable=shutil.rmtree, op_args=[EXPORT_DIR]
    )

    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    (
        slack_at_start
        >> mkdir
        >> download_data
        # Ensure we don't have any lingering tmp tables from a previously failed run.
        >> rm_tmp_tables_pre
        >> sqlalchemy_create_objects_from_schema
        >> postgres_create_tables_like
        >> create_sqlite_transform_file
        >> transform_csv_files
        >> postgres_insert_csv
        >> rename_tables
        >> rm_tmp_tables_post
        >> rm_tmp_dir
        >> grant_db_permissions
    )
