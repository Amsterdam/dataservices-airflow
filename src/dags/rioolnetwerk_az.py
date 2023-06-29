import operator
import os
from typing import Final

from airflow import DAG
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import DATASTORE_TYPE, MessageOperator, default_args, quote_string
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import (
    COLNAMES_CHECK,
    COUNT_CHECK,
    GEO_CHECK,
    PostgresMultiCheckOperator,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

RENAME_TABLES_SQL: Final = """
    DROP TABLE IF EXISTS public.rioolnetwerk_rioolknopen;
    ALTER TABLE pte.rioolknopen SET SCHEMA public;
    ALTER TABLE rioolknopen
        RENAME TO rioolnetwerk_rioolknopen;
    DROP TABLE IF EXISTS public.rioolnetwerk_rioolleidingen;
    ALTER TABLE pte.rioolleidingen SET SCHEMA public;
    ALTER TABLE rioolleidingen
        RENAME TO rioolnetwerk_rioolleidingen;
"""

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "rioolnetwerk_az"
DATASET_ID: Final = "rioolnetwerk"
owner = "team_ruimte"

with DAG(
    DAG_ID,
    default_args=default_args | {"owner": owner},
    # the access_control defines perms on DAG level. Not needed in Azure
    # since each datateam will get its own instance.
    access_control={owner: {"can_dag_read", "can_dag_edit"}},
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    checks = []

    table_names = []

    for table_name in (
        "rioolknopen",
        "rioolleidingen",
    ):
        for prefix in ("", "kel_"):
            table_names.append(f"{prefix}{table_name}")

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    drop_tables = PostgresOnAzureOperator(
        task_id="drop_tables",
        sql=[f"DROP TABLE IF EXISTS pte.{table_name} CASCADE" for table_name in table_names],
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"rioolnetwerk/{DATASTORE_TYPE}/" "rioolnetwerk.zip",
        dataset_name=DATASET_ID,
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
                params={"table_name": f"pte.{table_name}"},
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
                params={
                    "table_name": f"pte.{table_name}",
                    "geo_column": "geometrie",
                    "geotype": geo_type,
                },
                pass_value=1,
            )
        )

    multi_check = PostgresMultiCheckOperator(task_id="multi_check", checks=checks)

    rename_columns = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name="rioolnetwerk", pg_schema="pte"
    )

    rename_tables = PostgresOnAzureOperator(
        task_id="rename_tables",
        sql=RENAME_TABLES_SQL,
    )

    # Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)


[
    slack_at_start
    >> drop_tables
    >> swift_load_task
    >> multi_check
    >> rename_columns
    >> rename_tables
    >> grant_db_permissions
]
