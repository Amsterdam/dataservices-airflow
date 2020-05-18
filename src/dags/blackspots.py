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

DROP_IMPORT_TABLES = """
    DROP SEQUENCE IF EXISTS blackspots_spotexport_id_seq CASCADE;
    DROP TABLE IF EXISTS blackspots_spotexport CASCADE;
"""

RENAME_COLUMNS = """
    ALTER TABLE blackspots_spotexport
        RENAME COLUMN point TO geometry;
"""

RENAME_TABLES_SQL = """
    ALTER TABLE IF EXISTS blackspots_blackspots RENAME TO blackspots_blackspots_old;
    -- ALTER SEQUENCE IF EXISTS blackspots_id_seq RENAME TO blackspots_old_id_seq;
    ALTER TABLE blackspots_spotexport RENAME TO blackspots_blackspots;
    -- We do not need a sequence for the api I think?
    DROP SEQUENCE IF EXISTS blackspots_spotexport_id_seq CASCADE;
    DROP TABLE IF EXISTS blackspots_blackspots_old;
    -- DROP SEQUENCE IF EXISTS blackspots_old_id_seq CASCADE;
    ALTER INDEX blackspots_spotexport_pkey RENAME TO blackspots_blackspots_pkey;
    ALTER INDEX blackspots_spotexport_locatie_id_key RENAME TO blackspots_blackspots_locatie_id_key;
    ALTER INDEX blackspots_spotexport_locatie_id_6d5f324e_like
        RENAME TO blackspots_blackspots_locatie_id_6d5f324e_like;
    ALTER INDEX blackspots_spotexport_point_id RENAME TO blackspots_blackspots_point_id;
"""

dag_id = "blackspots"

with DAG(dag_id, default_args=default_args,) as dag:

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    drop_tables = PostgresOperator(task_id="drop_tables", sql=DROP_IMPORT_TABLES)

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="blackspots",
        object_id=f"{DATASTORE_TYPE}/spots.sql",
    )

    rename_columns = PostgresOperator(task_id=f"rename_columns", sql=RENAME_COLUMNS,)
    rename_tables = PostgresOperator(task_id="rename_tables", sql=RENAME_TABLES_SQL)

slack_at_start >> drop_tables >> swift_load_task >> rename_columns >> rename_tables
