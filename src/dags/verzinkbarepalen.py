import pathlib
import operator

from swift_hook import SwiftHook

from airflow.models import Variable
from airflow import DAG

from ogr2ogr_operator import Ogr2OgrOperator
from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator

from airflow.operators.bash_operator import BashOperator
from http_fetch_operator import HttpFetchOperator
from postgres_files_operator import PostgresFilesOperator

from airflow.operators.python_operator import PythonOperator


from common import (
    default_args,
    slack_webhook_token,
    MessageOperator,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

dag_id = "verzinkbarepalen"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data" / dag_id
tmp_dir = f"{SHARED_DIR}/{dag_id}"
variables = Variable.get(dag_id, deserialize_json=True)
data_endpoints = variables["data_endpoints"]
total_checks = []
count_checks = []
geo_checks = []


def checker(records, pass_value):
    found_colnames = set(r[0] for r in records)
    return found_colnames == set(pass_value)


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
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
    fetch_json = HttpFetchOperator(
        task_id="fetch_json",
        endpoint=f"{data_endpoints}",
        http_conn_id="ams_maps_conn_id",
        tmp_file=f"{tmp_dir}/{dag_id}.geojson",
    )

    # 4. Translate to geojson to SQL
    geojson_to_sql = Ogr2OgrOperator(
        task_id=f"create_SQL",
        target_table_name=f"{dag_id}_{dag_id}_new",
        sql_output_file=f"{tmp_dir}/{dag_id}_{dag_id}_new.sql",
        input_file=f"{tmp_dir}/{dag_id}.geojson",
    )

    # 5. Import data into DB
    create_table = PostgresFilesOperator(
        task_id="create_table", sql_files=[f"{tmp_dir}/{dag_id}_{dag_id}_new.sql"]
    )

    # 6. Rename COLUMNS based on Provenance
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

    # 7. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(
        task_id=f"multi_check", checks=total_checks
    )

    # 8. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table_{dag_id}",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )


(
    slack_at_start
    >> mkdir
    >> fetch_json
    >> geojson_to_sql
    >> create_table
    >> provenance_translation
    >> multi_checks
    >> rename_table
)


dag.doc_md = """
    #### DAG summery
    This DAG containts data about retractable posts and other closing mechanisms 
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing  
    https://api.data.amsterdam.nl/v1/docs/datasets/verzinkbarepalen.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/verzinkbarepalen.html
    Example geosearch: 
    https://api.data.amsterdam.nl/geosearch?datasets=verzinkbarepalen/verzinkbarepalen&x=106434&y=488995&radius=10
"""