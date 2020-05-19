from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

from common import (
    default_args,
    MessageOperator,
    DATAPUNT_ENVIRONMENT,
    slack_webhook_token,
)

DATASTORE_TYPE = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)


RENAME_COLUMNS = """
    ALTER TABLE pte.beschermde_stadsdorpsgezichten
        RENAME COLUMN bk_beschermde_stadsdorpsgezichten  TO id;
    ALTER TABLE pte.beschermde_stadsdorpsgezichten
        RENAME COLUMN geometrie TO geometry;
"""

RENAME_TABLES_SQL = """
    DROP TABLE IF EXISTS public.beschermdestadsdorpsgezichten_beschermdestadsdorpsgezichten;
    ALTER TABLE pte.beschermde_stadsdorpsgezichten SET SCHEMA public;
    ALTER TABLE beschermde_stadsdorpsgezichten
        RENAME TO beschermdestadsdorpsgezichten_beschermdestadsdorpsgezichten;
"""

dag_id = "beschermde_stadsdorpsgezichten"
owner = "team_ruimte"

with DAG(dag_id, default_args={**default_args, **{"owner": owner}}) as dag:

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    drop_and_create_schema = PostgresOperator(
        task_id="drop_and_create_schema",
        sql="DROP SCHEMA IF EXISTS pte CASCADE; CREATE SCHEMA pte;",
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"beschermde_stads_en_dorpsgezichten/{DATASTORE_TYPE}/"
        "beschermde_stadsdorpsgezichten.zip",
        swift_conn_id="objectstore_dataservices",
    )

    # rename_columns = PostgresOperator(task_id=f"rename_columns", sql=RENAME_COLUMNS,)
    rename_table = PostgresOperator(task_id=f"rename_table", sql=RENAME_TABLES_SQL,)

slack_at_start >> drop_and_create_schema >> swift_load_task >> rename_table
