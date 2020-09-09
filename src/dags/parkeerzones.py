import operator, pathlib

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator


from swift_operator import SwiftOperator
from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

dag_id = "parkeerzones"
variables = Variable.get(dag_id, deserialize_json=True)
files_to_download = variables["files_to_download"]
files_to_proces = variables["files_to_proces"]
tmp_dir = f"/tmp/{dag_id}"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


SQL_MAKE_VALID_GEOM = """
    UPDATE {{ params.tablename }} SET geometry = st_makevalid(geometry) WHERE 1=1 AND ST_IsValid(geometry) is false; 
    COMMIT;
"""

SQL_ADD_COLOR = """
    ALTER TABLE parkeerzones_parkeerzones_new ADD COLUMN color VARCHAR(7);    
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
    description="parkeerzones met en zonder uitzonderingen",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
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
    download_data = [
        SwiftOperator(
            task_id="download_file",
            # Default swift = Various Small Datasets objectstore
            # swift_conn_id="SWIFT_DEFAULT",
            container=f"{dag_id}",
            object_id=f"{files_to_download}",
            output_path=f"{tmp_dir}/{file}",
        )
        for file in files_to_download
    ]

    # 3. Unzip
    extract_zip = [
        BashOperator(
            task_id="extract_zip_file",
            bash_command=f'unzip -o "{tmp_dir}/{file}" -d {tmp_dir}',
        )
        for file in files_to_download
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 4.create the SQL for creating the table using ORG2OGR PGDump
    SHP_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{subject}",
            bash_command=f"ogr2ogr -f 'PGDump' "
            f"-t_srs EPSG:28992 -s_srs EPSG:4326 "
            f"-nln {dag_id}_{subject}_new "
            f"{tmp_dir}/{dag_id}_{subject}.sql {tmp_dir}/20190606_Vergunninggebieden/{shp_file} "
            f"-nlt PROMOTE_TO_MULTI "
            f"-lco GEOMETRY_NAME=geometry "
            f"-lco FID=id ",
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
        check_name[f"{subject}"] = total_checks

    # 9. Execute bundled checks (step 7) on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{subject}", checks=check_name[f"{subject}"]
        )
        for subject in files_to_proces.keys()
    ]

    # 10. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 11. Insert color codes to table parkeerzones only
    ## first create temp table with updated color codes
    ## second add the color codes to parkeerzones
    ## update color codes based on temp table
    load_map_colors = BashOperator(
        task_id="load_map_colors",
        bash_command=f"psql {pg_params()} < {sql_path}/parkeerzones_map_color.sql",
    )
    add_color = PostgresOperator(task_id="add_color", sql=SQL_ADD_COLOR)
    update_colors = PostgresOperator(task_id="update_colors", sql=SQL_UPDATE_COLORS)

    # 12. Remove records which are marked as SHOW = False
    delete_unused = [
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

    # FLOW. define flow with parallel executing of serial tasks for each file
    for (
        data,
        extract_zip,
        SHP_to_SQL,
        convert_to_UTF8,
        load_table,
        revalidate_geometry_record,
        multi_check,
        delete_unused,
        rename_table,
    ) in zip(
        download_data,
        extract_zip,
        SHP_to_SQL,
        convert_to_UTF8,
        load_tables,
        revalidate_geometry_records,
        multi_checks,
        delete_unused,
        rename_tables,
    ):

        [data >> extract_zip] >> Interface >> SHP_to_SQL

        [
            SHP_to_SQL
            >> convert_to_UTF8
            >> load_table
            >> revalidate_geometry_record
            >> multi_check
        ] >> provenance_translation >> load_map_colors >> add_color >> update_colors >> delete_unused

        [delete_unused >> rename_table]

    slack_at_start >> mk_tmp_dir >> download_data


dag.doc_md = """
    #### DAG summery
    This DAG containts auto park zones (parkeerzones) with and without restrictions (parkeerzones met uitzonderingen)
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
