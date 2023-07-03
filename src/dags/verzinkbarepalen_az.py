import operator
import os
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "verzinkbarepalen_az"
DATASET_ID: Final = "verzinkbarepalen"
data_path = Path(__file__).resolve().parents[1] / "data" / DATASET_ID
tmp_dir = f"{SHARED_DIR}/{DATASET_ID}"
variables = Variable.get(DATASET_ID, deserialize_json=True)
data_endpoints = variables["data_endpoints"]
total_checks = []
count_checks = []
geo_checks = []


def checker(records: list, pass_value: str) -> bool:
    """Checks if column name equals given value."""
    found_colnames = {r[0] for r in records}
    return found_colnames == set(pass_value)


with DAG(
    DAG_ID,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    fetch_json = HttpFetchOperator(
        task_id="fetch_json",
        endpoint=data_endpoints,
        http_conn_id="ams_maps_conn_id",
        tmp_file=f"{tmp_dir}/{DATASET_ID}.geojson",
    )

    # 4. Translate to geojson to SQL
    geojson_to_sql = Ogr2OgrOperator(
        task_id="create_SQL",
        target_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        sql_output_file=f"{tmp_dir}/{DATASET_ID}_{DATASET_ID}_new.sql",
        input_file=f"{tmp_dir}/{DATASET_ID}.geojson",
        mode="PGDump",
    )

    # 5. Import data into DB
    create_table = PostgresFilesOperator(
        task_id="create_table",
        dataset_name=DATASET_ID,
        sql_files=[f"{tmp_dir}/{DATASET_ID}_{DATASET_ID}_new.sql"],
    )

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=50,
            params={"table_name": f"{DATASET_ID}_{DATASET_ID}_new "},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": f"{DATASET_ID}_{DATASET_ID}_new",
                "geotype": ["POINT"],
            },
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 7. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 8. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table_{DATASET_ID}",
        old_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        new_table_name=f"{DATASET_ID}_{DATASET_ID}",
    )

    # 9. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)


(
    slack_at_start
    >> mkdir
    >> fetch_json
    >> geojson_to_sql
    >> create_table
    >> provenance_translation
    >> multi_checks
    >> rename_table
    >> grant_db_permissions
)


dag.doc_md = """
    #### DAG summary
    This DAG contains data about retractable posts and other closing mechanisms
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
