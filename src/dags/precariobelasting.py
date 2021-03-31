import operator, re

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

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
from sql.precariobelasting_add import (
    ADD_GEBIED_COLUMN,
    ADD_TITLE,
    RENAME_DATAVALUE_GEBIED,
)

dag_id = "precariobelasting"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
data_endpoints = variables["data_endpoints"]
data_endpoints = variables["temp_data"]
metadataschema_endpoint = variables["metadataschema_endpoint"]
metadataschema_file = f"{tmp_dir}/precariobelasting_metadataschema.json"
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

    # 3. download the data into temp directory
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_name}",
            endpoint=f"{url}",
            http_conn_id="airflow_home_conn_id",
            tmp_file=f"{tmp_dir}/{file_name}.json",
            output_type="file",
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

    # 5.create the SQL for creating the table using ORG2OGR PGDump
    extract_geojsons = [
        BashOperator(
            task_id=f"extract_geojson_{file_name}",
            bash_command=f"ogr2ogr -f 'PGDump' "
            f"-t_srs EPSG:28992 "
            f"-nln {file_name} "
            f"{tmp_dir}/{file_name}.sql {tmp_dir}/{file_name}.json "
            f"-lco FID=ID -lco GEOMETRY_NAME=geometry ",
        )
        for file_name in data_endpoints.keys()
    ]

    # 6. Load data into the table
    load_tables = [
        BashOperator(
            task_id=f"load_table_{file_name}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{file_name}.sql",
        )
        for file_name in data_endpoints.keys()
    ]

    # 7. Prepare the checks and added them per source to a dictionary
    for file_name in data_endpoints.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{file_name}",
                pass_value=2,
                params=dict(table_name=f"{file_name}"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{file_name}",
                params=dict(
                    table_name=f"{file_name}", geotype=["POLYGON", "MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[f"{file_name}"] = total_checks

    # 8. Execute bundled checks (step 7) on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{file_name}", checks=check_name[f"{file_name}"]
        )
        for file_name in data_endpoints.keys()
    ]

    # 10. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name=f"{dag_id}", pg_schema="public"
    )

    # 11. DROP Exisiting TABLE
    drop_tables = [
        PostgresOperator(
            task_id=f"drop_existing_table_{file_name}",
            sql=[f"DROP TABLE IF EXISTS {dag_id}_{file_name} CASCADE",],
        )
        for file_name in data_endpoints.keys()
    ]

    # 12. Rename the table from <tablename>_new to <tablename>
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{file_name}",
            old_table_name=f"{file_name}",
            new_table_name=f"{dag_id}_{file_name}",
        )
        for file_name in data_endpoints.keys()
    ]

    # 13. Add column TITLE as its was set in the display property in the metadataschema
    add_title_columns = [
        PostgresOperator(
            task_id=f"add_title_column_{file_name}",
            sql=ADD_TITLE,
            params=dict(tablenames=[f"{dag_id}_{file_name}"]),
        )
        for file_name in data_endpoints.keys()
    ]

    # 14. Dummy operator is used act as an interface between one set of parallel tasks to another parallel taks set (without this intermediar Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 15. Add derived columns (only woonschepen en bedrijfsvaartuigen are missing gebied as column)
    add_gebied_columns = [
        PostgresOperator(
            task_id=f"add_gebied_{file_name}",
            sql=ADD_GEBIED_COLUMN,
            params=dict(tablenames=[f"{dag_id}_{file_name}"]),
        )
        for file_name in ["woonschepen", "bedrijfsvaartuigen"]
    ]

    # 16. rename values in column gebied for terrassen en passagiersvaartuigen (woonschepen en bedrijfsvaartuigen are added in the previous step)
    rename_value_gebieden = [
        PostgresOperator(
            task_id=f"rename_value_{file_name}",
            sql=RENAME_DATAVALUE_GEBIED,
            params=dict(tablenames=[f"{dag_id}_{file_name}"]),
        )
        for file_name in ["terrassen", "passagiersvaartuigen"]
    ]

    # 17. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(
        task_id="grants",
        dag_name=dag_id
    )

    # FLOW. define flow with parallel executing of serial tasks for each file
    slack_at_start >> mk_tmp_dir >> download_data

    for (
        data,
        clean_data,
        extract_geojson,
        load_table,
        multi_check,
        add_title_column,
        drop_table,
        rename_table,
    ) in zip(
        download_data,
        clean_data,
        extract_geojsons,
        load_tables,
        multi_checks,
        add_title_columns,
        drop_tables,
        rename_tables,
    ):

        [
            data >> clean_data >> extract_geojson >> load_table >> multi_check
        ] >> provenance_translation >> drop_table

        [
            drop_table >> rename_table >> add_title_column
        ] >> Interface >> add_gebied_columns

    for add_gebied_column, rename_value_gebied in zip(
        add_gebied_columns, rename_value_gebieden
    ):
        add_gebied_column >> rename_value_gebied

    rename_value_gebieden >> grant_db_permissions

    dag.doc_md = """
    #### DAG summary
    This DAG contains precariobelasting data
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
