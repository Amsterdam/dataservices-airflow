import os
from typing import Final

from airflow import DAG
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import DATASTORE_TYPE, MessageOperator, default_args
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_permissions_operator import PostgresPermissionsOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

DROP_IMPORT_TABLES: Final = """
    DROP SEQUENCE IF EXISTS blackspots_spotexport_id_seq CASCADE;
    DROP TABLE IF EXISTS blackspots_spotexport CASCADE;
"""

RENAME_COLUMNS: Final = """
    create or replace function is_date(s varchar) returns boolean as $$
    begin
      if (s = '') IS NOT FALSE then
        return false;
      end if;
      perform to_date(s, 'DD/MM/YY');
      return true;
    exception when others then
      return false;
    end;
    $$ language plpgsql;
    -- Add extra columns for date opmerkingen
    ALTER TABLE blackspots_spotexport ADD COLUMN start_opmerking varchar(64);
    ALTER TABLE blackspots_spotexport ADD COLUMN eind_opmerking varchar(64);
    UPDATE blackspots_spotexport SET
        start_opmerking = start_uitvoering, start_uitvoering = NULL
        WHERE NOT is_date(start_uitvoering);
    UPDATE blackspots_spotexport SET
        eind_opmerking = eind_uitvoering, eind_uitvoering = NULL
        WHERE NOT is_date(eind_uitvoering);
    ALTER TABLE blackspots_spotexport ALTER COLUMN start_uitvoering TYPE DATE
        USING to_date(start_uitvoering, 'DD/MM/YY');
    ALTER TABLE blackspots_spotexport ALTER COLUMN eind_uitvoering TYPE DATE
        USING to_date(eind_uitvoering, 'DD/MM/YY');
    ALTER TABLE blackspots_spotexport
        RENAME COLUMN point TO geometry;
"""

RENAME_TABLES_SQL: Final = """
    ALTER TABLE IF EXISTS blackspots_blackspots RENAME TO blackspots_blackspots_old;
    ALTER TABLE blackspots_spotexport RENAME TO blackspots_blackspots;
    -- We do not need a sequence for the api I think?
    DROP SEQUENCE IF EXISTS blackspots_spotexport_id_seq CASCADE;
    DROP TABLE IF EXISTS blackspots_blackspots_old;
    ALTER INDEX blackspots_spotexport_pkey RENAME TO blackspots_blackspots_pkey;
    ALTER INDEX blackspots_spotexport_locatie_id_key RENAME TO blackspots_blackspots_locatie_id_key;
    ALTER INDEX blackspots_spotexport_locatie_id_6d5f324e_like
        RENAME TO blackspots_blackspots_locatie_id_6d5f324e_like;
    ALTER INDEX blackspots_spotexport_point_id RENAME TO blackspots_blackspots_point_id;
"""

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_MOBI"]

DAG_ID: Final = "blackspots_az"
DATASET_ID: Final = "blackspots"

with DAG(
    DAG_ID,
    default_args=default_args,
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
    schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    drop_tables = PostgresOnAzureOperator(task_id="drop_tables", sql=DROP_IMPORT_TABLES)

    swift_load_task = SwiftLoadSqlOperator(
        task_id=f"swift_load_task",
        container="blackspots",
        dataset_name=DATASET_ID,
        swift_conn_id="SWIFT_DEFAULT",
        object_id=f"{DATASTORE_TYPE}/spots.sql",
    )

    rename_columns = PostgresOnAzureOperator(
        task_id="rename_columns",
        sql=RENAME_COLUMNS,
    )

    rename_tables = PostgresOnAzureOperator(task_id="rename_tables",sql=RENAME_TABLES_SQL)

    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

(
    slack_at_start
    >> drop_tables
    >> swift_load_task
    >> rename_columns
    >> rename_tables
    >> grant_db_permissions
)
