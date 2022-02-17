import operator
from pathlib import Path
from typing import Final, Union, cast

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    OTAP_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

DAG_ID: Final = "spoorlijnen"
variables: dict[str, Union[list[str], dict[str, str]]] = Variable.get(
    "spoorlijnen", deserialize_json=True
)
files_to_download = cast(dict[str, list[str]], variables["files_to_download"])
TMP_PATH: Final = f"{SHARED_DIR}/{DAG_ID}"
total_checks: list[int] = []
count_checks: list[int] = []
geo_checks: list[int] = []
check_name: dict[str, list[int]] = {}
db_conn = DatabaseEngine()

with DAG(
    DAG_ID,
    description="spoorlijnen metro en tram",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
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
    mkdir = mk_dir(Path(TMP_PATH))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="SWIFT_DEFAULT",
            container="spoorlijnen",
            object_id=str(file),
            output_path=f"{TMP_PATH}/{file}",
        )
        for files in files_to_download.values()
        for file in files
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel tasks
    # with different number of lanes (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Import shape into database
    import_data = [
        Ogr2OgrOperator(
            task_id=f"import_{file}",
            target_table_name=f"{DAG_ID}_{file}_new",
            input_file=f"{TMP_PATH}/{file}.shp",
            s_srs="EPSG:28992",
            promote_to_multi=True,
            auto_detect_type="YES",
            mode="PostgreSQL",
            fid="id",
            db_conn=db_conn,
        )
        for file in files_to_download
    ]

    # 7. Revalidate or Remove invalid geometry records
    # the source has some invalid records where the geometry is not present (NULL)
    # or the geometry in itself is not valid (revalidate)
    # the removal of these records (less then 5) prevents errorness behaviour
    # to do: inform the source maintainer
    revalidate_remove_null_geometry_records = [
        PostgresOperator(
            task_id=f"revalidate_remove_geom_{key}",
            sql=[
                """UPDATE {{ params.table_id }} SET geometry = ST_CollectionExtract(st_makevalid(geometry),2)
                WHERE ST_IsValid(geometry) is false; COMMIT;""",
                "DELETE FROM {{ params.table_id }} WHERE geometry IS NULL; COMMIT;",
            ],
            params={"table_id": f"{DAG_ID}_{key}_new"},
        )
        for key in files_to_download.keys()
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    for key in files_to_download.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=500,
                params={"table_name": f"{DAG_ID}_{key}_new"},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params={
                    "table_name": f"{DAG_ID}_{key}_new",
                    "geotype": ["MULTILINESTRING"],
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 9. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in files_to_download.keys()
    ]

    # 10. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=f"{DAG_ID}_{key}_new",
            new_table_name=f"{DAG_ID}_{key}",
        )
        for key in files_to_download.keys()
    ]

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

slack_at_start >> mkdir >> download_data

for data in zip(download_data):

    data >> Interface

Interface >> import_data

for (import_file, revalidate_remove_geom_record, multi_check, rename_table,) in zip(
    import_data,
    revalidate_remove_null_geometry_records,
    multi_checks,
    rename_tables,
):

    [import_file >> revalidate_remove_geom_record] >> provenance_translation

    [multi_check >> rename_table]

provenance_translation >> multi_checks

rename_tables >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about railtracks metro and tram
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/spoorlijnen.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/spoorlijnen.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=spoorlijnen/metro&x=106434&y=488995&radius=10
"""
