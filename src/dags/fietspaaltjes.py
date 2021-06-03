import pathlib
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator


from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)
from importscripts.import_fietspaaltjes import import_fietspaaltjes
from common.sql import SQL_TABLE_RENAME

dag_id = "fietspaaltjes"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
# dag_config = Variable.get(dag_id, deserialize_json=True)


with DAG(
     dag_id,
     default_args=default_args,
     template_searchpath=["/"],
     on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id)
) as dag:

    tmp_dir = f"{SHARED_DIR}/{dag_id}"

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

    create_and_fill_table = PostgresFilesOperator(
        task_id="create_and_fill_table",
        sql_files=[f"{sql_path}/fietspaaltjes_create.sql", f"{tmp_dir}/{dag_id}.sql"],
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params=dict(tablename=f"{dag_id}_{dag_id}", pk="pkey"),
    )

    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        rename_indexes=False,
        pg_schema="public",
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(
        task_id="grants",
        dag_name=dag_id
    )

slack_at_start >> fetch_json >> create_sql >> create_and_fill_table >> rename_table >> provenance_translation >> grant_db_permissions
