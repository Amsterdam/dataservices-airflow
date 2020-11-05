from airflow import DAG
from swift_load_sql_operator import SwiftLoadSqlOperator
from provenance_rename_operator import ProvenanceRenameOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from swap_schema_operator import SwapSchemaOperator

from common import (
    default_args,
    DATAPUNT_ENVIRONMENT,
    slack_webhook_token,
    MessageOperator,

)

DATASTORE_TYPE = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)

dag_id = "huishoudelijkafval"


owner = "team_ruimte"
with DAG(dag_id, default_args={**default_args, **{"owner": owner}}) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Drop tables in target schema PTE (schema which orginates from the DB dump file, see next step)
    #    based upon presence in the Amsterdam schema definition
    drop_tables = ProvenanceDropFromSchemaOperator(
        task_id="drop_tables", 
        dataset_name="huishoudelijkafval", 
        pg_schema="pte",
    )

    # 3. load the dump file
    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"afval_huishoudelijk/{DATASTORE_TYPE}/" "afval_api.zip",
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present 
        db_target_schema="pte",      
    )

    # 4. Make the provenance translations
    provenance_renames = ProvenanceRenameOperator(
        task_id="provenance_renames",
        dataset_name="huishoudelijkafval",
        pg_schema="pte",
        rename_indexes=True,
    )

    # 5. Swap tables to target schema public
    swap_schema = SwapSchemaOperator(
        task_id="swap_schema", dataset_name="huishoudelijkafval"
    )

#FLOW

slack_at_start >> drop_tables >> swift_load_task >> provenance_renames >> swap_schema
