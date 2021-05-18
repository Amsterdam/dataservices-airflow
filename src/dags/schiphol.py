import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from ogr2ogr_operator import Ogr2OgrOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from sql.schiphol import ADD_THEMA_CONTEXT, DROP_COLS, SET_GEOM, SQL_DROP_TMP_TABLE

dag_id = "schiphol"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
files_to_download = variables["files_to_download"]
tables_to_proces = [table for table in variables["files_to_download"] if table != "themas"]
db_conn = DatabaseEngine()
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="geluidzones, hoogtebeperkingradar,"
    "maatgevendetoetshoogtes en vogelvrijwaringsgebieden",
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
            task_id=f"download_{key}",
            swift_conn_id="OBJECTSTORE_MILIEUTHEMAS",
            container="Milieuthemas",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for key, file in files_to_download.items()
    ]

    # 4. Transform seperator from pipeline to semicolon
    # and set code schema to UTF-8
    change_seperator = [
        BashOperator(
            task_id=f"change_seperator_{key}",
            bash_command=f"sed 's/|/;/g' -i  {tmp_dir}/{file};"
            f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{file} > "
            f"{tmp_dir}/utf-8_{file}",
        )
        for key, file in files_to_download.items()
    ]

    # 5. Import data into DB with ogr2ogr
    # It is not possible to use own id column for ID.
    # To still use a own identifier column the fid is set to fid instead of id.
    to_sql = [
        Ogr2OgrOperator(
            task_id=f"import_{key}",
            target_table_name=f"{dag_id}_{key}_new",
            input_file=f"{tmp_dir}/utf-8_{file}",
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            geometry_name="geometrie",
            auto_detect_type="YES",
            fid="id",
            mode="PostgreSQL",
            db_conn=db_conn,
        )
        for key, file in files_to_download.items()
    ]

    # 6. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 7. Add thema-context to child tables from parent table (themas)
    # except themas itself, which is a dimension table (parent) of veiligeafstanden table
    add_thema_contexts = [
        PostgresOperator(
            task_id=f"add_context_{key}",
            sql=ADD_THEMA_CONTEXT,
            params=dict(tablename=f"{dag_id}_{key}_new", parent_table=f"{dag_id}_themas_new"),
        )
        for key in files_to_download.keys()
        if "vogelvrijwaringsgebied" in key or "geluidzone" in key
    ]

    # 8. Drop unnecessary cols
    drop_cols = [
        PostgresOperator(
            task_id=f"drop_cols_{key}",
            sql=DROP_COLS,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in tables_to_proces
    ]

    # 9. RE-define GEOM type (because ogr2ogr cannot set geom with any .csv source file)
    redefine_geoms = [
        PostgresOperator(
            task_id=f"set_geomtype_{key}",
            sql=SET_GEOM,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in tables_to_proces
    ]

    # 10. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_table_{key}",
            data_schema_name=dag_id,
            data_table_name=f"{dag_id}_{key}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for key in tables_to_proces
    ]

    # 11. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    for key in tables_to_proces:

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=1,
                params=dict(table_name=f"{dag_id}_{key}_new"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params=dict(
                    table_name=f"{dag_id}_{key}_new",
                    geotype=["MULTIPOLYGON"],
                    geo_column="geometrie",
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 12. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in tables_to_proces
    ]

    # 13. Check for changes to merge in target table by using CDC
    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_{key}",
            source_table=f"{dag_id}_{key}_new",
            target_table=f"{dag_id}_{key}",
        )
        for key in tables_to_proces
    ]

    # 14. Clean up; delete temp table
    clean_ups = [
        PostgresOperator(
            task_id=f"clean_up_{key}",
            sql=SQL_DROP_TMP_TABLE,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in tables_to_proces
    ]

    # 15. Drop parent table THEMAS, not needed anymore
    drop_parent_table = PostgresOperator(
        task_id="drop_parent_table",
        sql=[
            f"DROP TABLE IF EXISTS {dag_id}_themas_new CASCADE",
        ],
    )

    # 16. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


# FLOW
slack_at_start >> mk_tmp_dir >> download_data

for (download, change_seperator, import_data) in zip(download_data, change_seperator, to_sql):

    [download >> change_seperator >> import_data] >> Interface

Interface >> add_thema_contexts

for add_thema_context in zip(add_thema_contexts):

    add_thema_context >> provenance_translation

provenance_translation >> drop_cols

for (drop_column, set_geom, multi_check, create_table, change_data_capture, clean_up) in zip(
    drop_cols, redefine_geoms, multi_checks, create_tables, change_data_capture, clean_ups
):

    drop_column >> set_geom >> multi_check >> create_table >> change_data_capture >> clean_up

clean_ups >> drop_parent_table >> grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains data about Schiphol related topics like 'geluidzones,
    hoogtebeperkingradar, maatgevendetoetshoogtes en vogelvrijwaringsgebieden'.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/schiphol.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/schiphol.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=schiphol/schiphol&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=schiphol/schiphol&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=schiphol/schiphol&x=106434&y=488995&radius=10
"""
