import os
import operator
from typing import Final

from airflow import DAG
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import DATASTORE_TYPE, MessageOperator, default_args
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import (
    COLNAMES_CHECK,
    COUNT_CHECK,
    GEO_CHECK,
    PostgresMultiCheckOperator,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

CORRECT_GEO: Final = """
    UPDATE pte.beschermde_stadsdorpsgezichten
        SET geometry = ST_CollectionExtract(ST_MakeValid("geometry"), 3)
    WHERE ST_IsValid(geometry) = False;
"""

RENAME_TABLES_SQL: Final = """
    DROP TABLE IF EXISTS public.beschermdestadsdorpsgezichten_beschermdestadsdorpsgezichten;
    ALTER TABLE pte.beschermde_stadsdorpsgezichten SET SCHEMA public;
    ALTER TABLE beschermde_stadsdorpsgezichten
        RENAME TO beschermdestadsdorpsgezichten_beschermdestadsdorpsgezichten;
"""

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "beschermde_stadsdorpsgezichten_az"
DATASET_ID: Final = "beschermde_stadsdorpsgezichten"
owner = "team_ruimte"

with DAG(
    DAG_ID,
    default_args=default_args | {"owner": owner},
    # the access_control defines perms on DAG level. Not needed in Azure
    # since each datateam will get its own instance.
    access_control={owner: {"can_dag_read", "can_dag_edit"}},
    on_failure_callback=get_contact_point_on_failure_callback(
        dataset_id="beschermdestadsdorpsgezichten"
    ),
) as dag:

    checks = []

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # XXX Potentially dangerous, because more team-ruimte tables
    # will be in schema pte, make more specific
    drop_imported_table = PostgresOnAzureOperator(
        task_id="drop_imported_table",
        sql="DROP TABLE IF EXISTS pte.beschermde_stadsdorpsgezichten CASCADE",
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"beschermde_stads_en_dorpsgezichten/{DATASTORE_TYPE}/"
        "beschermde_stadsdorpsgezichten.zip",
        dataset_name=DATASET_ID,
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present
        db_target_schema="pte",
        db_search_path=["pte", "extensions", "public"],
        bash_cmd_before_psql="sed 's/public.geometry/geometry/g' | sed 's/SELECT pg_catalog.set_config.*//g'",
    )

    checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=10,
            params={"table_name": "pte.beschermde_stadsdorpsgezichten"},
            result_checker=operator.ge,
        )
    )

    checks.append(
        COLNAMES_CHECK.make_check(
            check_id="colname_check",
            parameters=["pte", "beschermde_stadsdorpsgezichten"],
            pass_value={
                "id",
                "naam",
                "status",
                "aanwijzingsdatum",
                "intrekkingsdatum",
                "geometry",
            },
            result_checker=operator.ge,
        )
    )

    checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={"table_name": "pte.beschermde_stadsdorpsgezichten", "geotype": "MULTIPOLYGON"},
            pass_value=1,
        )
    )

    correct_geo = PostgresOnAzureOperator(
        task_id="correct_geo",
        sql=CORRECT_GEO,
    )
    multi_check = PostgresMultiCheckOperator(task_id="multi_check", checks=checks)

    rename_table = PostgresOnAzureOperator(
        task_id="rename_table",
        sql=RENAME_TABLES_SQL,
    )

    # Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

(
    slack_at_start
    >> drop_imported_table
    >> swift_load_task
    >> correct_geo
    >> multi_check
    >> rename_table
    >> grant_db_permissions
)
