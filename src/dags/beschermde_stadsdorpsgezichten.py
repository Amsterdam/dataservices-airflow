import operator

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    DATASTORE_TYPE,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import (
    COLNAMES_CHECK,
    COUNT_CHECK,
    GEO_CHECK,
    PostgresMultiCheckOperator,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

CORRECT_GEO = """
    UPDATE pte.beschermde_stadsdorpsgezichten
        SET geometry = ST_CollectionExtract(ST_MakeValid("geometry"), 3)
    WHERE ST_IsValid(geometry) = False;
"""

RENAME_TABLES_SQL = """
    DROP TABLE IF EXISTS public.beschermdestadsdorpsgezichten_beschermdestadsdorpsgezichten;
    ALTER TABLE pte.beschermde_stadsdorpsgezichten SET SCHEMA public;
    ALTER TABLE beschermde_stadsdorpsgezichten
        RENAME TO beschermdestadsdorpsgezichten_beschermdestadsdorpsgezichten;
"""

dag_id = "beschermde_stadsdorpsgezichten"
owner = "team_ruimte"

with DAG(
    dag_id,
    default_args={**default_args, **{"owner": owner}},
    on_failure_callback=get_contact_point_on_failure_callback(
        dataset_id="beschermdestadsdorpsgezichten"
    ),
) as dag:

    checks = []

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # XXX Potentially dangerous, because more team-ruimte tables
    # will be in schema pte, make more specific
    drop_imported_table = PostgresOperator(
        task_id="drop_imported_table",
        sql="DROP TABLE IF EXISTS pte.beschermde_stadsdorpsgezichten CASCADE",
    )

    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"beschermde_stads_en_dorpsgezichten/{DATASTORE_TYPE}/"
        "beschermde_stadsdorpsgezichten.zip",
        swift_conn_id="objectstore_dataservices",
    )

    checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=10,
            params=dict(table_name="pte.beschermde_stadsdorpsgezichten"),
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
            params=dict(
                table_name="pte.beschermde_stadsdorpsgezichten",
                geotype="MULTIPOLYGON",
            ),
            pass_value=1,
        )
    )

    correct_geo = PostgresOperator(
        task_id="correct_geo",
        sql=CORRECT_GEO,
    )
    multi_check = PostgresMultiCheckOperator(task_id="multi_check", checks=checks)

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=RENAME_TABLES_SQL,
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

(
    slack_at_start
    >> drop_imported_table
    >> swift_load_task
    >> correct_geo
    >> multi_check
    >> rename_table
    >> grant_db_permissions
)
