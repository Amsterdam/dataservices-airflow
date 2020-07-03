import operator
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from swift_load_sql_operator import SwiftLoadSqlOperator
from provenance_rename_operator import ProvenanceRenameOperator
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    COLNAMES_CHECK,
    GEO_CHECK,
)

from common import (
    default_args,
    MessageOperator,
    DATAPUNT_ENVIRONMENT,
    slack_webhook_token,
)

DATASTORE_TYPE = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)

RENAME_TABLES_SQL = """
    DROP TABLE IF EXISTS public.rioolleidingen_rioolknopen;
    ALTER TABLE pte.rioolknopen SET SCHEMA public;
    ALTER TABLE rioolknopen
        RENAME TO rioolleidingen_rioolknopen;
"""

dag_id = "riool"
owner = "team_ruimte"

with DAG(dag_id, default_args={**default_args, **{"owner": owner}}) as dag:

    checks = []

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    drop_table = PostgresOperator(
        task_id="drop_table",
        sql=[
            "DROP TABLE IF EXISTS pte.kel_rioolknopen CASCADE",
            "DROP TABLE IF EXISTS pte.rioolknopen CASCADE",
        ],
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"riool/{DATASTORE_TYPE}/" "riool.zip",
        swift_conn_id="objectstore_dataservices",
    )

    # XXX When second dataset table is available, add extra checks
    checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=180000,
            params=dict(table_name="pte.kel_rioolknopen"),
            result_checker=operator.ge,
        )
    )

    # XXX Get colnames from schema (provenance info)
    checks.append(
        COLNAMES_CHECK.make_check(
            check_id="colname_check",
            parameters=["pte", "kel_rioolknopen"],
            pass_value={"objnr", "knoopnr", "objectsoor", "type_funde", "geometrie",},
            result_checker=operator.ge,
        )
    )

    checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params=dict(
                table_name="pte.kel_rioolknopen",
                geo_column="geometrie",
                geotype="POINT",
            ),
            pass_value=1,
        )
    )

    multi_check = PostgresMultiCheckOperator(task_id="multi_check", checks=checks)

    rename_columns = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name="rioolleidingen", pg_schema="pte"
    )

    rename_table = PostgresOperator(task_id="rename_table", sql=RENAME_TABLES_SQL,)


slack_at_start >> drop_table >> swift_load_task >> multi_check >> rename_columns >> rename_table
