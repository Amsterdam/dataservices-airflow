import operator
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string

from common.path import mk_dir
from common.sql import SQL_GEOMETRY_VALID
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

DAG_ID = "grootstedelijke_projecten"
DATASET = "gebieden"
# owner: Only needed for CloudVPS. On Azure
# each team has its own Airflow instance.
owner = 'team_benk'
DB_TABLE_NAME = f"{DATASET}_{DAG_ID}"
TMP_PATH = f"{SHARED_DIR}/{DAG_ID}"
variables_aardgasvrijezones = Variable.get(DAG_ID, deserialize_json=True)
files_to_download = variables_aardgasvrijezones["files_to_download"]
shp_file_to_use_during_ingest = [file for file in files_to_download if "shp" in file][0]

total_checks = []
count_checks = []
geo_checks = []
check_name = {}


with DAG(
    DAG_ID,
    description="locaties en metadata rondom grootstedelijke projecten.",
    schedule_interval="@monthly",
    default_args=default_args | {"owner": owner},
    # the access_control defines perms on DAG level. Not needed in Azure
    # since each datateam will get its own instance.
    access_control={owner: {"can_dag_read", "can_dag_edit"}},
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(TMP_PATH))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="SWIFT_DEFAULT",
            container="grootstedelijkegebieden",
            object_id=file,
            output_path=f"{TMP_PATH}/{file}",
        )
        for file in files_to_download
    ]

    # 4.Import data into database
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{DB_TABLE_NAME}_new",
        input_file=f"{TMP_PATH}/{shp_file_to_use_during_ingest}",
        s_srs="EPSG:28992",
        promote_to_multi=True,
        auto_detect_type="YES",
        mode="PostgreSQL",
        fid="id",
    )

    # 5. Make geometry valid
    make_geo_valid = PostgresOnAzureOperator(
        task_id="make_geo_valid",
        sql=SQL_GEOMETRY_VALID,
        params={
            "tablename": f"{DB_TABLE_NAME}_new",
            "geo_column": "geometrie",
            "geom_type_number": "3",
        },
    )

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET,
        prefix_table_name=f"{DATASET}_",
        postfix_table_name="_new",
        subset_tables=[DAG_ID],
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id=f"count_check",
            pass_value=2,
            params={"table_name": f"{DB_TABLE_NAME}_new"},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id=f"geo_check",
            params={
                "table_name": f"{DB_TABLE_NAME}_new ",
                "geotype": ["MULTIPOINT", "MULTIPOLYGON"],
                "geo_column": "geometrie",
            },
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks
    check_name[DAG_ID] = total_checks

    # 7. Execute bundled checks on database
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=check_name[DAG_ID])

    # 8. Rename TABLE
    rename_tables = PostgresTableRenameOperator(
        task_id=f"rename_table",
        old_table_name=f"{DB_TABLE_NAME}_new",
        new_table_name=DB_TABLE_NAME,
    )

    # 9. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DB_TABLE_NAME)


slack_at_start >> mkdir >> download_data

for data in zip(download_data):

    data >> import_data

(
    import_data
    >> make_geo_valid
    >> provenance_translation
    >> multi_checks
    >> rename_tables
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains data of projects in the big urban region.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/gebieden.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/gebieden.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=gebieden/grootstedelijke_projecten&x=126156&y=485056&radius=10
"""
