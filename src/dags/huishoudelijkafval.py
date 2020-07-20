from airflow import DAG
from swift_load_sql_operator import SwiftLoadSqlOperator
from provenance_rename_operator import ProvenanceRenameOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from swap_schema_operator import SwapSchemaOperator

from common import (
    default_args,
    DATAPUNT_ENVIRONMENT,
)

DATASTORE_TYPE = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)

dag_id = "huishoudelijkafval"


owner = "team_ruimte"
with DAG(dag_id, default_args={**default_args, **{"owner": owner}}) as dag:

    drop_tables = ProvenanceDropFromSchemaOperator(
        task_id="drop_tables",
        dataset_name="huishoudelijkafval",
        pg_schema="pte",
        additional_table_names=["afval_api_adres_loopafstand"],
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"afval_huishoudelijk/{DATASTORE_TYPE}/" "afval_api.zip",
        swift_conn_id="objectstore_dataservices",
    )

    provenance_renames = ProvenanceRenameOperator(
        task_id="provenance_renames",
        dataset_name="huishoudelijkafval",
        pg_schema="pte",
        rename_indexes=True,
    )

    swap_schema = SwapSchemaOperator(
        task_id="swap_schema", dataset_name="huishoudelijkafval"
    )


drop_tables >> swift_load_task >> provenance_renames >> swap_schema
