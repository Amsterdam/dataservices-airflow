import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from http_fetch_operator import HttpFetchOperator

from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_permissions_operator import PostgresPermissionsOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

from sql.bekendmakingen import CONVERT_TO_GEOM

dag_id = "bekendmakingen"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="bekendmakingen en kennisgevingen",
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download data
    download_data = HttpFetchOperator(
        task_id="wfs_fetch",
        endpoint=variables["wfs_endpoint"],
        http_conn_id="geozet_conn_id",
        data=variables["wfs_params"],
        output_type="text",
        tmp_file=f"{tmp_dir}/{dag_id}.json",
    )

    # 4. Create SQL
    JSON_to_SQL = BashOperator(
        task_id="JSON_to_SQL",
        bash_command=f"ogr2ogr -f 'PGDump' "
        f"-t_srs EPSG:28992 "
        f"-nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {tmp_dir}/{dag_id}.json "
        f"-lco GEOMETRY_NAME=geometry "
        f"-lco FID=id",
    )

    # 5. Create TABLE
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.sql",
    )

    # 6. Convert BBOX (array of floats) to GEOM datatype
    convert_to_geom = PostgresOperator(
        task_id="convert_bbox_to_geom",
        sql=CONVERT_TO_GEOM,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id=f"count_check",
            pass_value=50,
            params=dict(table_name=f"{dag_id}_{dag_id}_new "),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id=f"geo_check",
            params=dict(
                table_name=f"{dag_id}_{dag_id}_new",
                geotype=["POINT"],
            ),
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 8. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id=f"multi_check", checks=total_checks)

    # 9. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table_{dag_id}",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 10. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

(
    slack_at_start
    >> mkdir
    >> download_data
    >> JSON_to_SQL
    >> create_table
    >> convert_to_geom
    >> provenance_translation
    >> multi_checks
    >> rename_table
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summery
    This DAG containts official announcements about licence applications (vergunningaanvragen e.d.)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/bekendmakingen.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/bekendmakingen.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=bekendmakingen/bekendmakingen&x=111153&y=483288&radius=10
"""
