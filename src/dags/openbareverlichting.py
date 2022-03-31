import operator
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common import (
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params,
    quote_string
)
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_openbare_verlichting import import_openbare_verlichting
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator

dag_id = "openbareverlichting"
variables = Variable.get("openbareverlichting", deserialize_json=True)
data_endpoints = variables["data_endpoints"]
dataset_objects = list(data_endpoints.keys())[0]
dataset_objecttypes = list(data_endpoints.keys())[1]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []


with DAG(
    dag_id,
    description="locatie, status en type van een openbare verlichting.",
    schedule_interval="*/15 * * * *",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    catchup=False,
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start"
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{dataset}",
            endpoint=endpoint,
            http_conn_id="verlichting_conn_id",
            tmp_file=f"{tmp_dir}/{dataset}.json",
        )
        for dataset, endpoint in data_endpoints.items()
    ]

    # 4. Convert json to Geojson
    convert_to_geojson = PythonOperator(
        task_id="convert_to_geojson",
        python_callable=import_openbare_verlichting,
        op_args=[
            # input: openbare verlichting objects (main data)
            f"{tmp_dir}/{dataset_objects}.json",
            # input: openbare verlichting objecttypes (reference data)
            f"{tmp_dir}/{dataset_objecttypes}.json",
            # output: geojson
            f"{tmp_dir}/objects.geo.json",
        ],
    )

    # 5. Geojson to SQL
    geojson_to_SQL = BashOperator(
        task_id="geojson_to_SQL",
        bash_command="ogr2ogr --config PG_USE_COPY YES -f 'PGDump' "
        "-t_srs EPSG:28992 "
        f"-nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/objects.sql {tmp_dir}/objects.geo.json "
        "-lco GEOMETRY_NAME=geometry "
        "-lco FID=ID",
    )

    # 6. Insert data into DB
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/objects.sql",
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
            check_id="count_check",
            pass_value=25,
            params=dict(table_name=f"{dag_id}_{dag_id}_new"),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params=dict(
                table_name=f"{dag_id}_{dag_id}_new",
                geotype=["POINT"],
            ),
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 8. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)
    # 9. Rename table
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 10. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> download_data

# for data in zip(download_data):

(
    download_data
    >> convert_to_geojson
    >> geojson_to_SQL
    >> create_table
    >> provenance_translation
    >> multi_checks
    >> rename_table
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains location, status and type of public lighting
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/openbare_verlichting.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/openbare_verlichting.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=openbare_verlichting/openbare_verlichting&x=111153&y=483288&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=openbare_verlichting/openbare_verlichting&x=111153&y=483288&radius=10
"""
