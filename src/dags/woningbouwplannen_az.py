import os
from typing import Final

from airflow import DAG
from common import DATASTORE_TYPE, MessageOperator, default_args
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swap_schema_operator import SwapSchemaOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "woningbouwplannen_az"
DATASET_ID: Final = "woningbouwplannen"

NM_RENAMES_SQL: Final = """
    DROP TABLE IF EXISTS public.woningbouwplannen_strategischeruimtes_buurten;
    ALTER TABLE pte.wbw_rel_strategischeruimte_buurt SET SCHEMA public;
    ALTER TABLE wbw_rel_strategischeruimte_buurt
        RENAME TO woningbouwplannen_strategischeruimtes_buurten;

    DROP TABLE IF EXISTS public.woningbouwplannen_woningbouwplan_buurten;
    ALTER TABLE pte.wbw_rel_woningbouwplan_buurt SET SCHEMA public;
    ALTER TABLE wbw_rel_woningbouwplan_buurt
        RENAME TO woningbouwplannen_woningbouwplan_buurten;

    ALTER TABLE woningbouwplannen_strategischeruimtes_buurten
        RENAME COLUMN wbw_strategischeruimte_id TO strategischeruimtes_id;
    ALTER TABLE woningbouwplannen_strategischeruimtes_buurten
        RENAME COLUMN gbd_buurt_id TO buurten_id;

    -- Temporary add a rename of the pk of the through table
    -- Owner of the dataset has been asked to change this
    -- in the sources datasets (the incoming zipped dump).
    ALTER TABLE woningbouwplannen_woningbouwplan_buurten
        RENAME COLUMN wbw_rel_woningbouwplan_buurt_id TO id;

    ALTER TABLE woningbouwplannen_woningbouwplan_buurten
        RENAME COLUMN wbw_woningbouwplan_id TO woningbouwplan_id;
    ALTER TABLE woningbouwplannen_woningbouwplan_buurten
        RENAME COLUMN gbd_buurt_id TO buurten_id;

    ALTER TABLE woningbouwplannen_woningbouwplan_buurten ALTER COLUMN
        woningbouwplan_id TYPE integer USING woningbouwplan_id::integer;
    ALTER TABLE woningbouwplannen_strategischeruimtes_buurten ALTER COLUMN
        strategischeruimtes_id TYPE integer USING strategischeruimtes_id::integer;
"""

owner = "team_ruimte"
with DAG(
    DAG_ID,
    default_args=default_args | {"owner": owner},
    # the access_control defines perms on DAG level. Not needed in Azure
    # since each datateam will get its own instance.
    access_control={owner: {"can_dag_read", "can_dag_edit"}},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Drop tables in target schema PTE
    #    (schema which orginates from the DB dump file, see next step)
    #    based upon presence in the Amsterdam schema definition
    #    For now, we add the autogenerated nm-tables to be dropped, should be done by
    #    ProvenanceDropFromSchemaOperator automatically.
    drop_tables = ProvenanceDropFromSchemaOperator(
        task_id="drop_tables",
        dataset_name="woningbouwplannen",
        pg_schema="pte",
        additional_table_names=[
            "wbw_rel_strategischeruimte_buurt",
            "wbw_rel_woningbouwplan_buurt",
        ],
    )

    # 3. load the dump file
    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"woningbouwplannen/{DATASTORE_TYPE}/woningbouwplannen.zip",
        dataset_name=DATASET_ID,
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present
        db_target_schema="pte",
    )

    # 4. Make the provenance translations
    provenance_renames = ProvenanceRenameOperator(
        task_id="provenance_renames",
        dataset_name="woningbouwplannen",
        pg_schema="pte",
        rename_indexes=True,
    )

    # 5. Swap tables to target schema public
    swap_schema = SwapSchemaOperator(task_id="swap_schema", dataset_name=DATASET_ID)

    # 6. Extra manual renaming for the nm-tables
    nm_tables_rename = PostgresOnAzureOperator(
        task_id="nm_tables_rename",
        sql=NM_RENAMES_SQL,
    )

    # 7. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

# FLOW
(
    slack_at_start
    >> drop_tables
    >> swift_load_task
    >> provenance_renames
    >> swap_schema
    >> nm_tables_rename
    >> grant_db_permissions
)
