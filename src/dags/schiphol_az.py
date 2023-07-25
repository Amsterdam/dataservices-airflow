import operator
import os
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string

from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.schiphol import ADD_THEMA_CONTEXT, DROP_COLS, SET_GEOM, SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "schiphol_az"
DATASET_ID: Final = "schiphol"
variables = Variable.get(DATASET_ID, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{DATASET_ID}"
files_to_download = variables["files_to_download"]
tables_to_proces = [table for table in variables["files_to_download"] if table != "themas"]

total_checks: list[int] = []
count_checks: list[int] = []
geo_checks: list[int] = []
check_name: dict[str, list[int]] = {}


with DAG(
    DAG_ID,
    description="geluidzones, hoogtebeperkingradar,"
    "maatgevendetoetshoogtes en vogelvrijwaringsgebieden",
    default_args=default_args,
    schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
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
            target_table_name=f"{DATASET_ID}_{key}_new",
            input_file=f"{tmp_dir}/utf-8_{file}",
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            geometry_name="geometrie",
            auto_detect_type="YES",
            fid="id",
            mode="PostgreSQL",
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
        PostgresOnAzureOperator(
            task_id=f"add_context_{key}",
            sql=ADD_THEMA_CONTEXT,
            params={"tablename": f"{DATASET_ID}_{key}_new", "parent_table": f"{DATASET_ID}_themas_new"},
        )
        for key in files_to_download.keys()
        if "vogelvrijwaringsgebied" in key or "geluidzone" in key
    ]

    # 8. Drop unnecessary cols
    drop_cols = [
        PostgresOnAzureOperator(
            task_id=f"drop_cols_{key}",
            sql=DROP_COLS,
            params={"tablename": f"{DATASET_ID}_{key}_new"},
        )
        for key in tables_to_proces
    ]

    # 9. RE-define GEOM type (because ogr2ogr cannot set geom with any .csv source file)
    redefine_geoms = [
        PostgresOnAzureOperator(
            task_id=f"set_geomtype_{key}",
            sql=SET_GEOM,
            params={"tablename": f"{DATASET_ID}_{key}_new"},
        )
        for key in tables_to_proces
    ]

    # 10. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_table_{key}",
            data_schema_name=DATASET_ID,
            data_table_name=f"{DATASET_ID}_{key}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for key in tables_to_proces
    ]

    # 11. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
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
                params={"table_name": f"{DATASET_ID}_{key}_new"},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params={
                    "table_name": f"{DATASET_ID}_{key}_new",
                    "geotype": ["MULTIPOLYGON"],
                    "geo_column": "geometrie",
                },
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
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{key}",
            dataset_name_lookup=DATASET_ID,
            source_table_name=f"{DATASET_ID}_{key}_new",
            target_table_name=f"{DATASET_ID}_{key}",
            drop_target_if_unequal=True,
        )
        for key in tables_to_proces
    ]

    # 14. Clean up; delete temp table
    clean_ups = [
        PostgresOnAzureOperator(
            task_id=f"clean_up_{key}",
            sql=SQL_DROP_TMP_TABLE,
            params={"tablename": f"{DATASET_ID}_{key}_new"},
        )
        for key in tables_to_proces
    ]

    # 15. Drop parent table THEMAS, not needed anymore
    drop_parent_table = PostgresOnAzureOperator(
        task_id="drop_parent_table",
        sql=[
            f"DROP TABLE IF EXISTS {DATASET_ID}_themas_new CASCADE",
        ],
    )

    # 16. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)


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
