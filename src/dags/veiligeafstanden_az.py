import operator
import os
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string

from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.veiligeafstanden import ADD_THEMA_CONTEXT, DROP_COLS, SET_GEOM, SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "veiligeafstanden_az"
DATASET_ID: Final = "veiligeafstanden"
variables_bodem = Variable.get("veiligeafstanden", deserialize_json=True)
files_to_download = variables_bodem["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{DATASET_ID}"

total_checks: list[int] = []
count_checks: list[int] = []
geo_checks: list[int] = []
check_name: dict[str, list[int]] = {}


with DAG(
    DAG_ID,
    description="locaties veilige-afstandobjecten zoals Vuurwerkopslag, Wachtplaats,"
    "Bunkerschip, Sluis, Munitieopslag, Gasdrukregel -en meetstation",
    default_args=default_args,
    schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{key}",
            swift_conn_id="OBJECTSTORE_MILIEUTHEMAS",
            container="Milieuthemas",
            object_id=file,
            output_path=f"{tmp_dir}/{key}_{file}",
        )
        for key, file in files_to_download.items()
    ]

    # 4. Transform seperator from pipeline to semicolon and set code schema to UTF-8
    change_seperators = [
        BashOperator(
            task_id=f"change_seperator_{key}",
            bash_command=f"cat {tmp_dir}/{key}_{file} | "
            f"sed 's/|/;/g' > {tmp_dir}/seperator_{key} ;"
            f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/seperator_{key} > "
            f"{tmp_dir}/utf_8_{key}.csv",
        )
        for key, file in files_to_download.items()
    ]

    # 5. Import data
    CSV_to_DB = [
        Ogr2OgrOperator(
            task_id=f"import_{key}",
            target_table_name=f"{DATASET_ID}_{key}_new",
            input_file=f"{tmp_dir}/utf_8_{key}.csv",
            s_srs="EPSG:3857",
            t_srs="EPSG:28992",
            input_file_sep="SEMICOLON",
            auto_detect_type="YES",
            geometry_name="geometrie",
            mode="PostgreSQL",
        )
        for key, file in files_to_download.items()
    ]

    # 6. RE-define GEOM type (because ogr2ogr cannot set geom with .csv import)
    # except themas itself, which is a dimension table (parent) of veiligeafstanden table
    redefine_geoms = [
        PostgresOnAzureOperator(
            task_id=f"re-define_geom_{key}",
            sql=SET_GEOM,
            params={"tablename": f"{DATASET_ID}_{key}_new"},
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 7. Add thema-context to child tables from parent table (themas)
    # except themas itself, which is a dimension table (parent) of veiligeafstanden table
    add_thema_contexts = [
        PostgresOnAzureOperator(
            task_id=f"add_context_{key}",
            sql=ADD_THEMA_CONTEXT,
            params={"tablename": f"{DATASET_ID}_{key}_new", "parent_table": f"{DATASET_ID}_themas_new"},
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    # except themas itself, which is a dimension table (parent) of sound zones tables
    for key in files_to_download.keys():
        if key == "veiligeafstanden":

            total_checks.clear()
            count_checks.clear()
            geo_checks.clear()

            count_checks.append(
                COUNT_CHECK.make_check(
                    check_id=f"count_check_{key}",
                    pass_value=2,
                    params={"table_name": f"{DATASET_ID}_{key}_new"},
                    result_checker=operator.ge,
                )
            )

            geo_checks.append(
                GEO_CHECK.make_check(
                    check_id=f"geo_check_{key}",
                    params={
                        "table_name": f"{DATASET_ID}_{key}_new",
                        "geotype": ["POLYGON", "POINT"],
                        "geo_column": "geometrie",
                    },
                    pass_value=1,
                )
            )

            total_checks = count_checks + geo_checks
            check_name[key] = total_checks

    # 9. Execute bundled checks on database (see checks definition here above)
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{key}",
            checks=check_name[key],
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 10. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_{key}_based_upon_schema",
            data_schema_name=key,
            data_table_name=f"{DATASET_ID}_{key}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 11. Check for changes to merge in target table
    change_data_capture = [
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{key}",
            dataset_name_lookup=DATASET_ID,
            source_table_name=f"{DATASET_ID}_{key}_new",
            target_table_name=f"{DATASET_ID}_{key}",
            drop_target_if_unequal=True,
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 12. Clean up (remove temp table _new)
    clean_ups = [
        PostgresOnAzureOperator(
            task_id="clean_up",
            sql=SQL_DROP_TMP_TABLE,
            params={"tablename": f"{DATASET_ID}_{key}_new"},
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 13. Drop unnecessary cols
    drop_cols = [
        PostgresOnAzureOperator(
            task_id=f"drop_cols_{key}",
            sql=DROP_COLS,
            params={"tablename": f"{DATASET_ID}_{key}_new"},
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 14. Drop parent table THEMAS, not needed anymore
    drop_parent_table = PostgresOnAzureOperator(
        task_id="drop_parent_table",
        sql=[
            f"DROP TABLE IF EXISTS {DATASET_ID}_themas_new CASCADE",
        ],
    )

    # 15. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

# FLOW
slack_at_start >> mkdir >> download_data

for (data, change_seperator, import_data) in zip(download_data, change_seperators, CSV_to_DB):

    [data >> change_seperator >> import_data] >> provenance_translation

provenance_translation >> redefine_geoms

for (
    redefine_geom,
    add_thema_context,
    multi_checks,
    create_tables,
    drop_cols,
    change_data_capture,
    clean_up,
) in zip(
    redefine_geoms,
    add_thema_contexts,
    multi_checks,
    create_tables,
    drop_cols,
    change_data_capture,
    clean_ups,
):

    [
        redefine_geom
        >> add_thema_context
        >> multi_checks
        >> create_tables
        >> drop_cols
        >> change_data_capture
        >> clean_up
    ]


clean_ups >> drop_parent_table >> grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains safety distance objects.
    Source files are located @objectstore.eu, container: Milieuthemas
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    NA
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/veiligeafstanden.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/veiligeafstanden.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=veiligeafstanden/veiligeafstanden&x=111153&y=483288&radius=10
"""
