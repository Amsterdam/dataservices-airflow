import operator
import pathlib
from typing import Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    quote_string,
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
from swift_operator import SwiftOperator

dag_id: str = "parkeerzones"
variables: Dict = Variable.get(dag_id, deserialize_json=True)
file_to_download: Dict = variables["files_to_download"]
files_to_proces: Dict = variables["files_to_proces"]
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
sql_path: pathlib.Path = pathlib.Path(__file__).resolve().parents[0] / "sql"
total_checks: List = []
count_checks: List = []
geo_checks: List = []
check_name: Dict = {}


SQL_MAKE_VALID_GEOM = """
    UPDATE {{ params.tablename }} SET geometry = st_makevalid(geometry)
    WHERE ST_IsValid(geometry) is false;
    COMMIT;
"""

SQL_ADD_COLOR = """
    ALTER TABLE parkeerzones_parkeerzones_new ADD COLUMN gebiedskleurcode VARCHAR(7);
"""

SQL_UPDATE_COLORS = """
    UPDATE parkeerzones_parkeerzones_new p SET gebiedskleurcode = pmc.color
    FROM parkeerzones_map_color pmc WHERE p.id = pmc.ogc_fid
    AND p.gebiedscode = pmc.gebied_code;
    COMMIT;
    DROP TABLE parkeerzones_map_color;
"""

SQL_DELETE_UNUSED = """
    DELETE FROM {{ params.tablename }} WHERE indicatie_zichtbaar = 'FALSE';
    COMMIT;
"""

with DAG(
    dag_id,
    description="parkeerzones met en zonder uitzonderingen.",
    default_args=default_args,
    user_defined_filters=dict(quote=quote_string),
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id)
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

    # 3. Download data
    download_data = SwiftOperator(
        task_id="download_file",
        # Default swift = Various Small Datasets objectstore
        # swift_conn_id="SWIFT_DEFAULT",
        container=dag_id,
        object_id=file_to_download,
        output_path=f"{tmp_dir}/{file_to_download}",
    )

    # 3. Unzip
    extract_zip = BashOperator(
        task_id="extract_zip_file",
        bash_command=f'unzip -o "{tmp_dir}/{file_to_download}" -d {tmp_dir}',
    )

    # 4.create the SQL for creating the table using ORG2OGR PGDump
    SHP_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{subject}",
            bash_command="ogr2ogr -f 'PGDump' "
            "-t_srs EPSG:28992 -s_srs EPSG:4326 "
            f"-nln {dag_id}_{subject}_new "
            f"{tmp_dir}/{dag_id}_{subject}.sql {tmp_dir}/20190606_Vergunninggebieden/{shp_file} "
            "-nlt PROMOTE_TO_MULTI "
            "-lco GEOMETRY_NAME=geometry "
            "-lco FID=id ",
        )
        for subject, shp_file in files_to_proces.items()
    ]

    # 5. Convert charakterset to UTF8
    convert_to_UTF8 = [
        BashOperator(
            task_id=f"convert_UTF8_{subject}",
            bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}_{subject}.sql > "
            f"{tmp_dir}/UTF8_{dag_id}_{subject}.sql",
        )
        for subject in files_to_proces.keys()
    ]

    # 6. Load data into the table
    load_tables = [
        BashOperator(
            task_id=f"load_table_{subject}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/UTF8_{dag_id}_{subject}.sql",
        )
        for subject in files_to_proces.keys()
    ]

    # 7. Revalidate invalid geometry records
    # the source has some invalid records
    # to do: inform the source maintainer
    revalidate_geometry_records = [
        PostgresOperator(
            task_id=f"revalidate_geom_{subject}",
            sql=SQL_MAKE_VALID_GEOM,
            params=dict(tablename=f"{dag_id}_{subject}_new"),
        )
        for subject in files_to_proces.keys()
    ]

    # 8. Prepare the checks and added them per source to a dictionary
    for subject in files_to_proces.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{subject}",
                pass_value=2,
                params=dict(table_name=f"{dag_id}_{subject}_new"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{subject}",
                params=dict(
                    table_name=f"{dag_id}_{subject}_new",
                    geotype=["POLYGON", "MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[subject] = total_checks

    # 9. Execute bundled checks (step 7) on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{subject}", checks=check_name[subject])
        for subject in files_to_proces.keys()
    ]

    # 10. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 11. Insert color codes to table parkeerzones only
    # first create temp table with updated color codes
    # second add the color codes to parkeerzones
    # update color codes based on temp table
    load_map_colors = BashOperator(
        task_id="load_map_colors",
        bash_command=f"psql {pg_params()} < {sql_path}/parkeerzones_map_color.sql",
    )
    add_color = PostgresOperator(task_id="add_color", sql=SQL_ADD_COLOR)
    update_colors = PostgresOperator(task_id="update_colors", sql=SQL_UPDATE_COLORS)

    # 12. Remove records which are marked as SHOW = False
    delete_unuseds = [
        PostgresOperator(
            task_id=f"delete_show_false_{subject}",
            sql=SQL_DELETE_UNUSED,
            params=dict(tablename=f"{dag_id}_{subject}_new"),
        )
        for subject in files_to_proces.keys()
    ]

    # 13. Rename the table from <tablename>_new to <tablename>
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{subject}",
            old_table_name=f"{dag_id}_{subject}_new",
            new_table_name=f"{dag_id}_{subject}",
        )
        for subject in files_to_proces.keys()
    ]

    # 14. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW. define flow with parallel executing of serial tasks for each file
slack_at_start >> mk_tmp_dir >> download_data >> extract_zip >> SHP_to_SQL

for (shape_to_sql, convert_to_UTF8, load_table, revalidate_geometry_record, multi_check,) in zip(
    SHP_to_SQL,
    convert_to_UTF8,
    load_tables,
    revalidate_geometry_records,
    multi_checks,
):

    [
        shape_to_sql >> convert_to_UTF8 >> load_table >> revalidate_geometry_record >> multi_check
    ] >> provenance_translation


provenance_translation >> load_map_colors >> add_color >> update_colors >> delete_unuseds

for (delete_unused, rename_table) in zip(
    delete_unuseds,
    rename_tables,
):
    [delete_unused >> rename_table] >> grant_db_permissions

grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains auto park zones (parkeerzones) with
    and without restrictions (parkeerzones met uitzonderingen)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/parkeerzones.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/parkeerzones.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=parkeerzones/parkeerzones&x=111153&y=483288&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=parkeerzones/parkeerzones_uitzondering&x=111153&y=483288&radius=10
"""
