import logging
import operator
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    OTAP_ENVIRONMENT,
    SHARED_DIR,
    SLACK_ICON_START,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.path import mk_dir
from common.sql import SQL_DROP_TABLE
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator

logger = logging.getLogger(__name__)
db_conn = DatabaseEngine()

DAG_ID: Final = "hoofdroutes"
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
variables: dict[str, dict[str, str]] = Variable.get(DAG_ID, deserialize_json=True)
files_to_download: dict[str, str] = variables["files_to_download"]
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


with DAG(
    DAG_ID,
    description="Aangewezen verkeer hoofdroutes voor bijzondere vracht of specifieke voertuigen.",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"{SLACK_ICON_START} Starting {DAG_ID} ({OTAP_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(TMP_DIR))

    # 3. download .geojson data from data.amsterdam.nl
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{resource_name}",
            endpoint=url,
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{TMP_DIR}/{resource_name}.geojson",
            output_type="text",
            encoding_schema="UTF-8",
        )
        for resource_name, url in files_to_download.items()
    ]

    # 4. convert geojson to csv
    load_data = [
        Ogr2OgrOperator(
            task_id=f"import_{resource_name}",
            target_table_name=f"{DAG_ID}_{resource_name}_new",
            input_file=f"{TMP_DIR}/{resource_name}.geojson",
            s_srs="EPSG:4326",
            t_srs="EPSG:28992",
            input_file_sep="SEMICOLON",
            auto_detect_type="YES",
            geometry_name="geometry",
            mode="PostgreSQL",
            fid="id",
            db_conn=db_conn,
        )
        for resource_name in files_to_download
    ]

    # 5. RENAME columns based on PROVENANCE
    provenance_trans = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    for resource_name in files_to_download:

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{resource_name}",
                pass_value=2,
                params={"table_name": f"{DAG_ID}_{resource_name}_new"},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{resource_name}",
                params={
                    "table_name": f"{DAG_ID}_{resource_name}_new",
                    "geotype": [
                        "POINT",
                        "MULTILINESTRING",
                    ],
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[resource_name] = [*total_checks]

    # 6. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{resource_name}", checks=check_name[resource_name]
        )
        for resource_name in files_to_download
    ]

    # 7. Check for changes to merge in target table
    change_data_capture = [
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{resource_name}",
            dataset_name=DAG_ID,
            source_table_name=f"{DAG_ID}_{resource_name}_new",
            target_table_name=f"{DAG_ID}_{resource_name}",
            drop_target_if_unequal=True,
        )
        for resource_name in files_to_download
    ]

    # 8. Clean up (remove temp table _new)
    clean_ups = [
        PostgresOperator(
            task_id=f"clean_up_{resource_name}",
            sql=SQL_DROP_TABLE,
            params={"tablename": f"{DAG_ID}_{resource_name}_new"},
        )
        for resource_name in files_to_download
    ]

    # 9. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)


# FLOW
slack_at_start >> mkdir >> download_data

for download, load in zip(download_data, load_data):
    [download >> load]

load_data >> provenance_trans >> multi_checks

for (multi_check, capture_data, clean_up) in zip(multi_checks, change_data_capture, clean_ups):

    [multi_check >> capture_data >> clean_up]

clean_ups >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about traffic routes (verkeer hoofdroutes)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner (Bas Bussink) at bereikbaarheidsthermometer@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/hoofdroutes.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/hoofdroutes.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=hoofdroutes/routes_gevaarlijke_stoffen&x=118601&y=490715&radius=1
    https://api.data.amsterdam.nl/geosearch?datasets=hoofdroutes/tunnels_gevaarlijke_stoffen&x=124630&y=480803&radius=1
"""
