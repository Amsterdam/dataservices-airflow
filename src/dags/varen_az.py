import os
from typing import Final

from airflow import DAG
from common import DATASTORE_TYPE, MessageOperator, default_args
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swap_schema_operator import SwapSchemaOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_MOBI"]

DAG_ID: Final = "varen_az"
DATASET_ID: Final = "varen"
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

    # 2. Drop tables in target schema PTE (schema which orginates from the DB
    # dump file, see next step) based upon presence in the Amsterdam schema
    # definition.
    drop_tables = ProvenanceDropFromSchemaOperator(
        task_id="drop_tables",
        dataset_name=DATASET_ID,
        pg_schema="pte",
    )

    # 3. load the dump file
    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"varen/{DATASTORE_TYPE}/varen.zip",
        dataset_name=DATASET_ID,
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present
        db_target_schema="pte",
        db_search_path=["pte", "extensions", "public"],
        bash_cmd_before_psql="sed 's/public.geometry/geometry/g' | sed 's/SELECT pg_catalog.set_config.*//g'",
    )

    # 4. Make the provenance translations
    provenance_renames = ProvenanceRenameOperator(
        task_id="provenance_renames",
        dataset_name=DATASET_ID,
        pg_schema="pte",
        rename_indexes=True,
    )

    # 5. Swap tables to target schema public
    swap_schema = SwapSchemaOperator(task_id="swap_schema", dataset_name=DATASET_ID)

    # 6. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

# FLOW

(
    slack_at_start
    >> drop_tables
    >> swift_load_task
    >> provenance_renames
    >> swap_schema
    >> grant_db_permissions
)
