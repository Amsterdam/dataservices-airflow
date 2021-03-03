import pathlib
from airflow import DAG


from postgres_check_operator import PostgresCheckOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from postgres_xcom_operator import PostgresXcomOperator

from common import (
    vsd_default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)
from importscripts.oplaadpalen.import_oplaadpalen_allego import import_oplaadpalen

from common.sql import (
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
)

SQL_EXISTS_CHECK = """
    SELECT 1 WHERE EXISTS (
       SELECT
       FROM   information_schema.tables
       WHERE  table_schema = 'public'
       AND    table_name = 'oplaadpalen'
       )
"""

SQL_TABLE_RENAME = """
    ALTER TABLE IF EXISTS oplaadpalen RENAME TO oplaadpalen_old;
    ALTER TABLE oplaadpalen_new RENAME TO oplaadpalen;
    CREATE OR REPLACE VIEW oplaadpunten AS SELECT op.*,
        concat_ws(' ', op.provider, '-', op.street, op.housenumber) as name
    FROM oplaadpalen op
        WHERE op.connector_type <> 'SHUNK_PANTOGRAPH' AND op.status not in ('Deleted');
    DROP TABLE IF EXISTS oplaadpalen_old;
"""

dag_id = "oplaadpalen"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"


def choose_branch(**kwargs):
    ti = kwargs["ti"]
    oplaadpalen_exists = ti.xcom_pull(task_ids="check_table_exists") is not None
    return "update_oplaadpalen" if oplaadpalen_exists else "create_oplaadpalen"


with DAG(
    dag_id,
    default_args=vsd_default_args,
    template_searchpath=["/"],
    schedule_interval="@hourly",
) as dag:

    tmp_dir = f"{SHARED_DIR}/{dag_id}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    check_table_exists = PostgresXcomOperator(
        task_id="check_table_exists", sql=SQL_EXISTS_CHECK, do_xcom_push=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task", python_callable=choose_branch
    )

    update_oplaadpalen = PostgresOperator(
        task_id="update_oplaadpalen", sql=f"{sql_path}/oplaadpalen_copy.sql"
    )

    create_oplaadpalen = PostgresOperator(
        task_id="create_oplaadpalen", sql=f"{sql_path}/oplaadpalen_create.sql"
    )

    # The trigger_rule is essential, otherwise the skipped path blocks progress
    import_allego = PythonOperator(
        task_id="import_allego",
        python_callable=import_oplaadpalen,
        trigger_rule="none_failed_or_skipped",
        op_args=[
            PostgresHook(
                postgres_conn_id=dag.default_args["postgres_conn_id"]
            ).get_conn()
        ],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=10),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(tablename=f"{dag_id}_new", geotype="ST_Point"),
    )

    rename_table = PostgresOperator(task_id="rename_table", sql=SQL_TABLE_RENAME,)

slack_at_start >> check_table_exists >> branch_task
branch_task >> update_oplaadpalen >> import_allego
branch_task >> create_oplaadpalen >> import_allego
import_allego >> [check_count, check_geo] >> rename_table
