import operator
import re

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params,
    quote_string,
    slack_webhook_token,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_touringcars import import_touringcars
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator

dag_id = "touringcars"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
data_endpoints = variables["data_endpoints"]
metadataschema_endpoint = variables["metadataschema_endpoint"]
metadataschema_file = f"{tmp_dir}/touringcars_metadataschema.json"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


# remove space hyphen characters
def clean_data(file_name):
    data = open(file_name, "r").read()
    result = re.sub(r"[\xc2\xad]", "", data)
    with open(file_name, "w") as output:
        output.write(result)


with DAG(
    dag_id,
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. create download temp directory to store the data
    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. download the data into temp directory
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_name}",
            endpoint=url,
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{tmp_dir}/{file_name}.json",
            output_type="text",
        )
        for file_name, url in data_endpoints.items()
    ]

    # 4. Cleanse the downloaded data (remove the space hyphen characters)
    clean_data = [
        PythonOperator(
            task_id=f"clean_data_{file_name}",
            python_callable=clean_data,
            op_args=[f"{tmp_dir}/{file_name}.json"],
        )
        for file_name in data_endpoints.keys()
    ]

    # 5. Transform json to geojson
    translate_json_to_geojson = [
        PythonOperator(
            task_id=f"json_to_geojson_{file_name}",
            python_callable=import_touringcars,
            op_args=[
                f"{tmp_dir}/{file_name}.json",
                f"{tmp_dir}/{file_name}.geo.json",
            ],
        )
        for file_name in data_endpoints.keys()
    ]

    # 6.create the SQL for creating the table using ORG2OGR PGDump
    extract_geojsons = [
        BashOperator(
            task_id=f"extract_geojson_{file_name}",
            bash_command=f"echo $PWD; cat {tmp_dir}/{file_name}.json; ogr2ogr -f 'PGDump' "
            "-s_srs EPSG:4326 -t_srs EPSG:28992 "
            f"-nln {file_name} "
            f"{tmp_dir}/{file_name}.sql {tmp_dir}/{file_name}.geo.json "
            "-lco FID=ID -lco GEOMETRY_NAME=geometry ",
        )
        for file_name in data_endpoints.keys()
    ]

    # 7. Load data into the table
    load_tables = [
        BashOperator(
            task_id=f"load_table_{file_name}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{file_name}.sql",
        )
        for file_name in data_endpoints.keys()
    ]

    # Prepare the checks and added them per source to a dictionary
    for file_name in data_endpoints.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{file_name}",
                pass_value=2,
                params=dict(table_name=file_name),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{file_name}",
                params=dict(
                    table_name=file_name,
                    geotype=[
                        "POLYGON",
                        "MULTIPOLYGON",
                        "MULTILINESTRING",
                        "LINESTRING",
                    ],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[file_name] = total_checks

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{file_name}", checks=check_name[file_name]
        )
        for file_name in data_endpoints.keys()
    ]

    # 9. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name=dag_id, pg_schema="public"
    )

    # 10. DROP Exisiting TABLE
    drop_tables = [
        PostgresOperator(
            task_id=f"drop_existing_table_{file_name}",
            sql=[
                f"DROP TABLE IF EXISTS {dag_id}_{file_name} CASCADE",
            ],
        )
        for file_name in data_endpoints.keys()
    ]

    # 11. RENAME TABLES
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{file_name}",
            old_table_name=file_name,
            new_table_name=f"{dag_id}_{file_name}",
        )
        for file_name in data_endpoints.keys()
    ]

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW. define flow with parallel executing of serial tasks for each file
slack_at_start >> mk_tmp_dir >> download_data

for (
    data,
    clean_data,
    json_to_geojson,
    extract_geo,
    load_table,
    multi_check,
    drop_table,
    rename_table,
) in zip(
    download_data,
    clean_data,
    translate_json_to_geojson,
    extract_geojsons,
    load_tables,
    multi_checks,
    drop_tables,
    rename_tables,
):

    (
        [data >> clean_data >> json_to_geojson >> extract_geo >> load_table >> multi_check]
        >> provenance_translation
        >> drop_table
    )

    [drop_table >> rename_table]

rename_tables >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains touringcars data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Used @dataportaal to inform busdrivers
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/touringcars/aanbevolenroutes/
    https://api.data.amsterdam.nl/v1/touringcars/verplichteroutes/
    https://api.data.amsterdam.nl/v1/touringcars/parkeerplaatsen/
    https://api.data.amsterdam.nl/v1/touringcars/haltes/
    https://api.data.amsterdam.nl/v1/touringcars/wegwerkzaamheden/
    https://api.data.amsterdam.nl/v1/touringcars/doorrijhoogtes/
"""
