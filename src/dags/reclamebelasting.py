import operator
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import define_temp_db_schema
from common.path import mk_dir
from common.sql import SQL_DROP_TABLE
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

env: object = Env()

DAG_ID: Final = "reclamebelasting"
SCHEMA_NAME: Final = "belastingen"
tmp_database_schema: str = define_temp_db_schema(dataset_name=SCHEMA_NAME)
TABLE_NAME: Final = "reclame"
variables: dict = Variable.get(DAG_ID, deserialize_json=True)
files_to_download: dict = variables["files_to_download"]
zip_file: str = files_to_download["zip_file"]
shp_file: str = files_to_download["shp_file"]
tmp_dir: str = f"{SHARED_DIR}/{DAG_ID}"
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


with DAG(
    DAG_ID,
    default_args=default_args,
    description="""Reclamebelastingjaartarieven per belastinggebied voor (reclame)uitingen
    met oppervlakte >= 0,25 m2 en > 10 weken in een jaar zichtbaar zijn.""",
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id="belastingen"),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = SwiftOperator(
        task_id=f"download_{zip_file}",
        swift_conn_id="SWIFT_DEFAULT",
        container="reclame",
        object_id=zip_file,
        output_path=f"{tmp_dir}/{zip_file}",
    )

    # 4. Extract zip file
    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f"unzip -o {tmp_dir}/{zip_file} -d {tmp_dir}",
    )

    # 5. drop TEMP table on the database
    # PostgresOperator will execute SQL in safe mode.
    drop_if_exists_tmp_tables = PostgresOnAzureOperator(
        task_id="drop_if_exists_tmp_table",
        dataset_name=SCHEMA_NAME,
        sql=SQL_DROP_TABLE,
        params={"schema": tmp_database_schema, "tablename": f"{SCHEMA_NAME}_{TABLE_NAME}_new"},
    )

    # 6. Load data
    load_data = Ogr2OgrOperator(
        task_id=f"import_{shp_file}",
        target_table_name=f"{SCHEMA_NAME}_{TABLE_NAME}_new",
        db_schema=tmp_database_schema,
        dataset_name=SCHEMA_NAME,
        input_file=f"{tmp_dir}/{shp_file}",
        s_srs=None,
        t_srs="EPSG:28992",
        auto_detect_type="YES",
        geometry_name="geometrie",
        mode="PostgreSQL",
        fid="id",
    )

    # 7. RENAME columns based on PROVENANCE
    provenance_trans = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=SCHEMA_NAME,
        prefix_table_name=f"{SCHEMA_NAME}_",
        postfix_table_name="_new",
        subset_tables=["".join(TABLE_NAME)],
        rename_indexes=False,
        pg_schema=tmp_database_schema,
    )

    # Prepare the checks and added them per source to a dictionary
    total_checks.clear()
    count_checks.clear()

    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=1,
            params={
                "table_name": "{tmp_schema}.{dataset}_{table}_new".format(
                    tmp_schema=tmp_database_schema, dataset=SCHEMA_NAME, table=TABLE_NAME
                )
            },
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": "{tmp_schema}.{dataset}_{table}_new".format(
                    tmp_schema=tmp_database_schema, dataset=SCHEMA_NAME, table=TABLE_NAME
                ),
                "geotype": [
                    "POLYGON",
                    "MULTIPOLYGON",
                ],
            },
            pass_value=1,
        )
    )

    check_name[DAG_ID] = count_checks

    # 8. Execute bundled checks on database (in this case just a count check)
    multi_checks = PostgresMultiCheckOperator(
        task_id="count_check", checks=check_name[DAG_ID], dataset_name=SCHEMA_NAME
    )

    # 9. Check for changes to merge in target table
    change_data_capture = PostgresTableCopyOperator(
        task_id="copy_data_to_target",
        dataset_name_lookup=SCHEMA_NAME,
        dataset_name=SCHEMA_NAME,
        source_table_name=f"{SCHEMA_NAME}_{TABLE_NAME}_new",
        source_schema_name=tmp_database_schema,
        target_table_name=f"{SCHEMA_NAME}_{TABLE_NAME}",
        drop_target_if_unequal=True,
    )

# FLOW
(
    slack_at_start
    >> mkdir
    >> download_data
    >> extract_zip
    >> drop_if_exists_tmp_tables
    >> load_data
    >> provenance_trans
    >> multi_checks
    >> change_data_capture
)

dag.doc_md = """
    #### DAG summary
    This DAG contains advertising tax areas and rates.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/belastingen/reclame.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=belastingen/reclame&x=106434&y=488995&radius=10
"""
