import operator
from pathlib import Path

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    OTAP_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params,
    quote_string,
    slack_webhook_token,
)
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.evenementen import SET_DATE_DATATYPE

dag_id = "evenementen"

variables_evenementen = Variable.get("evenementen", deserialize_json=True)
data_endpoint = variables_evenementen["data_endpoint"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
data_file = f"{tmp_dir}/evenementen.geojson"
total_checks = []
count_checks = []
geo_checks = []


# data connection
def get_data():
    """calling the data endpoint"""

    # get data
    data_url = data_endpoint
    data_data = requests.get(data_url)

    # store data
    with open(data_file, "w") as file:
        file.write(data_data.text)
    file.close()


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f":runner: Starting {dag_id} ({OTAP_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = PythonOperator(task_id="download_data", python_callable=get_data)

    # 4. Create SQL
    # ogr2ogr demands the PK is of type intgger. In this case the source ID is of type varchar.
    # So FID=ID cannot be used.
    create_SQL = BashOperator(
        task_id="create_SQL_based_on_geojson",
        bash_command="ogr2ogr -f 'PGDump' "
        "-t_srs EPSG:28992 "
        f"-nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {data_file} "
        "-lco GEOMETRY_NAME=geometry "
        "-oo DATE_AS_STRING=NO "
        "-lco FID=id",
    )

    # 5. Create TABLE
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.sql",
    )

    # 6. Set datatype date for date columns that where not detected by ogr2ogr
    set_datatype_date = PostgresOperator(
        task_id="set_datatype_date",
        sql=SET_DATE_DATATYPE,
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

    # 9. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 10. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


(
    slack_at_start
    >> mkdir
    >> download_data
    >> create_SQL
    >> create_table
    >> set_datatype_date
    >> provenance_translation
    >> multi_checks
    >> rename_table
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains data about leisure events (evenementen)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/evenementen.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/evenementen.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=evenementen/evenementen&x=106434&y=488995&radius=10
"""
