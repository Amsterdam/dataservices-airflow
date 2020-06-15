import re

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from http_fetch_operator import HttpFetchOperator
from provenance_operator import ProvenanceOperator

from postgres_check_operator import PostgresCheckOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)
from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
)

dag_id = "precariobelasting"
variables = Variable.get(dag_id, deserialize_json=True)
data_end_points = variables["data_end_points"]
schema_end_point = variables["schema_end_point"]
tmp_dir = f"/tmp/{dag_id}"
metadataschema = f"{tmp_dir}/precariobelasting_dataschema.json"

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


# remove space hyphen characters
def clean_data(file):
    data = open(file, "r").read()
    result = re.sub(r"[\xc2\xad]", "", data)
    with open(file, "w") as output:
        output.write(result)


with DAG(
    dag_id, default_args=default_args, user_defined_filters=dict(quote=quote),
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

    # 3. download dataschema into temp directory
    download_schema = HttpFetchOperator(
        task_id=f"download_schema",
        endpoint=f"{schema_end_point}",
        http_conn_id="schema_precariobelasting_conn_id",
        tmp_file=f"{metadataschema}",
        output_type="text",
    )

    # 4. download the data into temp directory
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_name}",
            endpoint=f"{url}",
            http_conn_id="data_precariobelasting_conn_id",
            tmp_file=f"{tmp_dir}/{file_name}.json",
            output_type="text",
        )
        for file_name, url in data_end_points.items()
    ]

    # 5. Cleanse the downloaded data (remove the space hyphen characters)
    clean_data = [
        PythonOperator(
            task_id=f"clean_data_{file_name}",
            python_callable=clean_data,
            op_args=[f"{tmp_dir}/{file_name}.json"],
        )
        for file_name in data_end_points.keys()
    ]

    # 6.create the SQL for creating the table using ORG2OGR PGDump
    extract_geojsons = [
        BashOperator(
            task_id=f"extract_geojson_{file_name}",
            bash_command=f"ogr2ogr -f 'PGDump' "
            f"-t_srs EPSG:28992 "
            f"-nln {dag_id}_{file_name}_new "
            f"{tmp_dir}/{file_name}.sql {tmp_dir}/{file_name}.json "
            f"-lco FID=ID -lco GEOMETRY_NAME=geometry ",
        )
        for file_name in data_end_points.keys()
    ]

    # 7. because the -sql option cannot be applied to a json source in step 4,
    #   replace the columns names from generated .sql with possible translated names in the metadataschema
    provenance_translations = [
        ProvenanceOperator(
            task_id=f"provenance_{file_name}",
            metadataschema=f"{metadataschema}",
            source_file=f"{tmp_dir}/{file_name}.sql",
            table_to_get_columns=f"{file_name}",
        )
        for file_name in data_end_points.keys()
    ]

    # 8. Load data into the table
    load_tables = [
        BashOperator(
            task_id=f"load_table_{file_name}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{file_name}.sql",
        )
        for file_name in data_end_points.keys()
    ]

    # 9. Check the minimal requirement for number of records
    check_counts = [
        PostgresCheckOperator(
            task_id=f"check_count_{file_name}",
            sql=SQL_CHECK_COUNT,
            params=dict(tablename=f"{dag_id}_{file_name}_new", mincount=1),
        )
        for file_name in data_end_points.keys()
    ]

    # 10. Check if geometry is valid based on the specified geotype(s)
    check_geos = [
        PostgresCheckOperator(
            task_id=f"check_geo_{file_name}",
            sql=SQL_CHECK_GEO,
            params=dict(
                tablename=f"{dag_id}_{file_name}_new",
                geotype=["ST_Polygon", "ST_MultiPolygon"],
                geo_column="geometry",
                check_valid=False,
            ),
        )
        for file_name in data_end_points.keys()
    ]

    # 11. Rename the table from <tablename>_new to <tablename>
    rename_tables = [
        PostgresOperator(
            task_id=f"rename_table_{file_name}",
            sql=SQL_TABLE_RENAME,
            params=dict(tablename=f"{dag_id}_{file_name}", geo_column="geometry",),
        )
        for file_name in data_end_points.keys()
    ]

    # FLOW. define flow with parallel executing of serial tasks for each file
    for (
        data,
        clean_data,
        extract_geo,
        get_column,
        load_table,
        check_count,
        check_geo,
        rename_table,
    ) in zip(
        download_data,
        clean_data,
        extract_geojsons,
        provenance_translations,
        load_tables,
        check_counts,
        check_geos,
        rename_tables,
    ):
        data >> clean_data >> extract_geo >> get_column >> load_table >> check_count >> check_geo >> rename_table

    slack_at_start >> mk_tmp_dir >> download_schema >> download_data

    dag.doc_md = """
    #### DAG summery
    This DAG containts precariobelasting data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/precariobelasting/precariobelasting/
"""
