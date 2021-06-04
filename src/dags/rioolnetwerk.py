import operator

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    DATASTORE_TYPE,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from postgres_check_operator import (
    COLNAMES_CHECK,
    COUNT_CHECK,
    GEO_CHECK,
    PostgresMultiCheckOperator,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

RENAME_TABLES_SQL = """
    DROP TABLE IF EXISTS public.rioolnetwerk_rioolknopen;
    ALTER TABLE pte.rioolknopen SET SCHEMA public;
    ALTER TABLE rioolknopen
        RENAME TO rioolnetwerk_rioolknopen;
    DROP TABLE IF EXISTS public.rioolnetwerk_rioolleidingen;
    ALTER TABLE pte.rioolleidingen SET SCHEMA public;
    ALTER TABLE rioolleidingen
        RENAME TO rioolnetwerk_rioolleidingen;
"""

dag_id = "rioolnetwerk"
owner = "team_ruimte"

with DAG(
    dag_id,
    default_args={**default_args, **{"owner": owner}},
    user_defined_filters={"quote": quote_string},
) as dag:

    checks = []

    table_names = []

    for table_name in (
        "rioolknopen",
        "rioolleidingen",
    ):
        for prefix in ("", "kel_"):
            table_names.append(f"{prefix}{table_name}")

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    drop_tables = PostgresOperator(
        task_id="drop_tables",
        sql=[f"DROP TABLE IF EXISTS pte.{table_name} CASCADE" for table_name in table_names],
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"rioolnetwerk/{DATASTORE_TYPE}/" "rioolnetwerk.zip",
        swift_conn_id="objectstore_dataservices",
    )

    for table_name, count, geo_type, field_names in (
        (
            "kel_rioolknopen",
            180000,
            "POINT",
            {
                "objnr",
                "knoopnr",
                "objectsoor",
                "type_funde",
                "geometrie",
            },
        ),
        (
            "kel_rioolleidingen",
            194000,
            ["MULTILINESTRING", "LINESTRING"],
            {"objnr", "leidingnaa", "br_diamete", "vorm"},
        ),
    ):

        checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{table_name}",
                pass_value=count,
                params=dict(table_name=f"pte.{table_name}"),
                result_checker=operator.ge,
            )
        )

        # XXX Get colnames from schema (provenance info)
        checks.append(
            COLNAMES_CHECK.make_check(
                check_id=f"colname_check_{table_name}",
                parameters=["pte", table_name],
                pass_value=field_names,
                result_checker=operator.ge,
            )
        )

        checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{table_name}",
                params=dict(
                    table_name=f"pte.{table_name}",
                    geo_column="geometrie",
                    geotype=geo_type,
                ),
                pass_value=1,
            )
        )

    multi_check = PostgresMultiCheckOperator(task_id="multi_check", checks=checks)

    rename_columns = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name="rioolnetwerk", pg_schema="pte"
    )

    rename_tables = PostgresOperator(
        task_id="rename_tables",
        sql=RENAME_TABLES_SQL,
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


[
    slack_at_start
    >> drop_tables
    >> swift_load_task
    >> multi_check
    >> rename_columns
    >> rename_tables
    >> grant_db_permissions
]
