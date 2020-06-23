import operator, re, pathlib

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from http_fetch_operator import HttpFetchOperator
from postgres_check_operator import PostgresCheckOperator
from provenance_operator import ProvenanceOperator
from postgres_rename_operator import PostgresTableRenameOperator

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
from sql.precariobelasting_add import ADD_GEBIED_COLUMN, ADD_TITLE

dag_id = "precariobelasting"
variables = Variable.get(dag_id, deserialize_json=True)
data_end_points = variables["data_end_points"]
data_end_points = variables["temp_data"]
schema_end_point = variables["schema_end_point"]
tmp_dir = f"/tmp/{dag_id}"
metadataschema = f"{tmp_dir}/precariobelasting_dataschema.json"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


# remove space hyphen characters
def clean_data(file_name):
    data = open(file_name, "r").read()
    result = re.sub(r"[\xc2\xad]", "", data)
    with open(file_name, "w") as output:
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
        http_conn_id="schemas_data_amsterdam_conn_id",
        tmp_file=f"{metadataschema}",
        output_type="text",
    )

    # 4. download the data into temp directory
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_name}",
            endpoint=f"{url}",
            http_conn_id="airflow_home_conn_id",
            tmp_file=f"{tmp_dir}/{file_name}.json",
            output_type="file",
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

    # 9. Prepare the checks and added them per source to a dictionary
    for file_name in data_end_points.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{file_name}",
                pass_value=2,
                params=dict(table_name=f"{dag_id}_{file_name}_new"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{file_name}",
                params=dict(
                    table_name=f"{dag_id}_{file_name}_new",
                    geotype=["POLYGON", "MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[f"{file_name}"] = total_checks

    # 10. Execute bundled checks (step 9) on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{file_name}", checks=check_name[f"{file_name}"]
        )
        for file_name in data_end_points.keys()
    ]

    # 11. Rename the table from <tablename>_new to <tablename>
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{file_name}",
            old_table_name=f"{dag_id}_{file_name}_new",
            new_table_name=f"{dag_id}_{file_name}",
        )
        for file_name in data_end_points.keys()
    ]

    # 12. Add derived columns (woonschepen en bedrijfsvaartuigen are missing gebied as column)
    add_title_columns = [
        PostgresOperator(
            task_id=f"add_title_column_{file_name}",
            sql=ADD_TITLE,
            params=dict(tablenames=[f"precariobelasting_{file_name}"]),
        )
        for file_name in data_end_points.keys()
    ]

    # 13. Dummy operator is used act as an interface between one set of parallel tasks to another parallel taks set (without this intermediar Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 14. Add derived columns (only woonschepen en bedrijfsvaartuigen are missing gebied as column)
    add_gebied_columns = [
        PostgresOperator(
            task_id=f"add_gebied_column_{file_name}",
            sql=ADD_GEBIED_COLUMN,
            params=dict(tablenames=[f"precariobelasting_{file_name}"]),
        )
        for file_name in ["woonschepen", "bedrijfsvaartuigen"]
    ]

    # FLOW. define flow with parallel executing of serial tasks for each file
    for (
        data,
        clean_data,
        extract_geo,
        get_column,
        load_table,
        multi_check,
        rename_table,
        add_title_column,
    ) in zip(
        download_data,
        clean_data,
        extract_geojsons,
        provenance_translations,
        load_tables,
        multi_checks,
        rename_tables,
        add_title_columns,
    ):

        [
            data
            >> clean_data
            >> extract_geo
            >> get_column
            >> load_table
            >> multi_check
            >> rename_table
            >> add_title_column
        ] >> Interface >> add_gebied_columns

    for add_gebied_column in zip(add_gebied_columns):
        add_gebied_column

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
