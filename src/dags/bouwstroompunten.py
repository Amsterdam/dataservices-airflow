import json
import operator
from pathlib import Path
from typing import Any, Union

import dsnparse
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from http_fetch_operator import HttpFetchOperator
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.bouwstroompunten_pk import ADD_PK

dag_id = "bouwstroompunten"
variables = Variable.get(dag_id, deserialize_json=True)
auth_endpoint = variables["data_endpoints"]["auth"]
data_endpoint = variables["data_endpoints"]["data"]
tmp_dir = Path(SHARED_DIR) / dag_id
data_file = f"{tmp_dir}/bouwstroompunten_data.geojson"
env = Env()
password = env("AIRFLOW_CONN_BOUWSTROOMPUNTEN_PASSWD")
user = env("AIRFLOW_CONN_BOUWSTROOMPUNTEN_USER")
base_url = env("AIRFLOW_CONN_BOUWSTROOMPUNTEN_BASE_URL")
total_checks = []
count_checks = []
geo_checks = []

db_conn: object = DatabaseEngine()


def get_token() -> Union[str, Any]:
    """Getting the access token for calling the data endpoint.

    Returns:
        API access token.

    Note: Because of the additional /https in the base url environment variable
    `AIRFLOW_CONN_BOUWSTROOMPUNTEN_BASE_URL` the scheme en host must be extracted
    and merged into a base endpoint.
    """
    scheme = dsnparse.parse(base_url).scheme
    host = dsnparse.parse(base_url).hostloc
    base_endpoint = "".join([scheme, "://", host])
    token_url = f"{base_endpoint}/{auth_endpoint}"
    token_payload = {"identifier": user, "password": password}
    token_headers = {"content-type": "application/json"}

    try:
        token_request = requests.post(
            token_url,
            data=json.dumps(token_payload),
            headers=token_headers,
            verify=True,
        )
        token_request.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise requests.exceptions.HTTPError(
            "Something went wrong, please check the get_token function.", err.response.text
        )

    token_load = json.loads(token_request.text)
    return token_load["jwt"]


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
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(tmp_dir, clean_if_exists=False)

    # 3. Download data
    download_data = HttpFetchOperator(
        task_id="download",
        endpoint=data_endpoint,
        http_conn_id="BOUWSTROOMPUNTEN_BASE_URL",
        tmp_file=data_file,
        headers={
            "content-type": "application/json",
            "Authorization": f"Bearer {get_token()}",
        },
    )

    # 4. Import data
    # ogr2ogr demands the PK is of type intgger. In this case the source ID is of type varchar.
    # So FID=ID cannot be used.
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{dag_id}_{dag_id}_new",
        input_file=data_file,
        s_srs=None,
        fid="ogc_fid",
        auto_detect_type="YES",
        mode="PostgreSQL",
        db_conn=db_conn,
    )

    # 5. Drop Exisiting TABLE
    drop_tables = PostgresOperator(
        task_id="drop_existing_table",
        sql="DROP TABLE IF EXISTS {{ params.table_id }} CASCADE",
        params={"table_id": f"{dag_id}_{dag_id}"},
    )

    # 6. Rename COLUMNS based on provenance (if specified)
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 7. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 8. RE-define PK(see step 4 why)
    redefine_pk = PostgresOperator(
        task_id="re-define_pk",
        sql=ADD_PK,
        params={"tablename": f"{dag_id}_{dag_id}"},
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=25,
            params={"table_name": f"{dag_id}_{dag_id}"},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": f"{dag_id}_{dag_id}",
                "geotype": ["POINT"],
            },
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 9. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 10. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

    (
        slack_at_start
        >> mkdir
        >> download_data
        >> import_data
        >> drop_tables
        >> provenance_translation
        >> rename_table
        >> redefine_pk
        >> multi_checks
        >> grant_db_permissions
    )

dag.doc_md = """
    #### DAG summary
    This DAG contains power stations data for event or market
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
