import json
import operator

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from environs import Env

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.bouwstroompunten_pk import ADD_PK

dag_id = "bouwstroompunten"
variables = Variable.get(dag_id, deserialize_json=True)
auth_endpoint = variables["data_endpoints"]["auth"]
data_endpoint = variables["data_endpoints"]["data"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
data_file = f"{tmp_dir}/bouwstroompunten_data.geojson"
env = Env()
password = env("AIRFLOW_CONN_BOUWSTROOMPUNTEN_PASSWD")
user = env("AIRFLOW_CONN_BOUWSTROOMPUNTEN_USER")
base_url = env("AIRFLOW_CONN_BOUWSTROOMPUNTEN_BASE_URL")
total_checks = []
count_checks = []
geo_checks = []

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


# data connection
def get_data():
    """ getting the the access token and calling the data endpoint """

    # get token
    token_url = f"{base_url}/{auth_endpoint}"
    token_payload = {"identifier": user, "password": password}
    token_headers = {"content-type": "application/json"}
    # verify=Fals because source API is apparently configured incorrectly.
    token_request = requests.post(
        token_url, data=json.dumps(token_payload), headers=token_headers, verify=False
    )
    token_load = json.loads(token_request.text)
    token_get = token_load["jwt"]

    # get data
    data_url = f"{base_url}/{data_endpoint}"
    data_headers = {
        "content-type": "application/json",
        "Authorization": f"Bearer {token_get}",
    }
    data_data = requests.get(data_url, headers=data_headers, verify=False)

    # store data
    with open(data_file, "w") as file:
        file.write(data_data.text)
    file.close()


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id)
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
    download_data = PythonOperator(task_id="download_data", python_callable=get_data)

    # 4. Create SQL
    # ogr2ogr demands the PK is of type intgger. In this case the source ID is of type varchar.
    # So FID=ID cannot be used.
    create_SQL = BashOperator(
        task_id="create_SQL_based_on_geojson",
        bash_command="ogr2ogr -f 'PGDump' "
        "-t_srs EPSG:28992 "
        f"-nln {dag_id} "
        f"{tmp_dir}/{dag_id}.sql {data_file} "
        "-lco GEOMETRY_NAME=geometry ",
    )

    # 5. Create TABLE
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.sql",
    )

    # 6. Drop Exisiting TABLE
    drop_tables = PostgresOperator(
        task_id="drop_existing_table",
        sql=[
            f"DROP TABLE IF EXISTS {dag_id}_{dag_id} CASCADE",
        ],
    )

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name=dag_id, pg_schema="public"
    )

    # 8. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=dag_id,
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 9. RE-define PK(see step 4 why)
    redefine_pk = PostgresOperator(
        task_id="re-define_pk",
        sql=ADD_PK,
        params=dict(tablename=f"{dag_id}_{dag_id}"),
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=25,
            params=dict(table_name=f"{dag_id}_{dag_id}"),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params=dict(
                table_name=f"{dag_id}_{dag_id}",
                geotype=["POINT"],
            ),
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 10. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


(
    slack_at_start
    >> mkdir
    >> download_data
    >> create_SQL
    >> create_table
    >> drop_tables
    >> provenance_translation
    >> rename_table
    >> redefine_pk
    >> multi_checks
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains power stations data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/bouwstroompunten/bouwstroompunten/
"""
