import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator
from http_fetch_operator import HttpFetchOperator


from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

from importscripts.import_openbare_verlichting import import_openbare_verlichting

dag_id = "openbareverlichting"
variables = Variable.get("openbareverlichting", deserialize_json=True)
data_endpoints = variables["data_endpoints"]
dataset_objects = list(data_endpoints.keys())[0]
dataset_objecttypes = list(data_endpoints.keys())[1]
tmp_dir = f"/tmp/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="locatie, status en type van een openbare verlichting.",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
    template_searchpath=["/"],
) as dag:

    # 1. Post message on slack
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
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{dataset}",
            endpoint=f"{endpoint}",
            http_conn_id="verlichting_conn_id",
            output_type="text",
            tmp_file=f"{tmp_dir}/{dataset}.json",
        )
        for dataset, endpoint in data_endpoints.items()
    ]

    # 4. Convert json to Geojson
    convert_to_geojson = PythonOperator(
        task_id="convert_to_geojson",
        python_callable=import_openbare_verlichting,
        op_args=[
            f"{tmp_dir}/{dataset_objects}.json",  # input: openbare verlichting objects (main data)
            f"{tmp_dir}/{dataset_objecttypes}.json",  # input: openbare verlichting objecttypes (reference data)
            f"{tmp_dir}/objects.geo.json",  # output: geojson
        ],
    )

    # 5. Geojson to SQL
    geojson_to_SQL = BashOperator(
        task_id="geojson_to_SQL",
        bash_command=f"ogr2ogr --config PG_USE_COPY YES -f 'PGDump' "
        f"-t_srs EPSG:28992 "
        f"-nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/objects.sql {tmp_dir}/objects.geo.json "
        f"-lco GEOMETRY_NAME=geometry "
        f"-lco FID=ID",
    )

    # 6. Insert data into DB
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/objects.sql",
    )

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id=f"count_check",
            pass_value=25,
            params=dict(table_name=f"{dag_id}_{dag_id}_new"),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id=f"geo_check",
            params=dict(table_name=f"{dag_id}_{dag_id}_new", geotype=["POINT"],),
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 8. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(
        task_id=f"multi_check", checks=total_checks
    )
    # 9. Rename table
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )


for data in zip(download_data):

    data >> convert_to_geojson >> geojson_to_SQL >> create_table >> provenance_translation >> multi_checks >> rename_table

slack_at_start >> mkdir >> download_data

dag.doc_md = """
    #### DAG summery
    This DAG containts location, status and type of public lighting
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
