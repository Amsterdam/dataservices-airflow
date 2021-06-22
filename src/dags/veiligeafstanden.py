import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
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
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.veiligeafstanden import ADD_THEMA_CONTEXT, DROP_COLS, SET_GEOM, SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

dag_id = "veiligeafstanden"
variables_bodem = Variable.get("veiligeafstanden", deserialize_json=True)
files_to_download = variables_bodem["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
db_conn = DatabaseEngine()
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


with DAG(
    dag_id,
    description="locaties veilige-afstandobjecten zoals Vuurwerkopslag, Wachtplaats,"
    "Bunkerschip, Sluis, Munitieopslag, Gasdrukregel -en meetstation",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
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
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

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
            target_table_name=f"{dag_id}_{key}_new",
            input_file=f"{tmp_dir}/utf_8_{key}.csv",
            s_srs="EPSG:3857",
            t_srs="EPSG:28992",
            input_file_sep="SEMICOLON",
            auto_detect_type="YES",
            geometry_name="geometrie",
            mode="PostgreSQL",
            db_conn=db_conn,
        )
        for key, file in files_to_download.items()
    ]

    # 6. RE-define GEOM type (because ogr2ogr cannot set geom with .csv import)
    # except themas itself, which is a dimension table (parent) of veiligeafstanden table
    redefine_geoms = [
        PostgresOperator(
            task_id=f"re-define_geom_{key}",
            sql=SET_GEOM,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 7. Add thema-context to child tables from parent table (themas)
    # except themas itself, which is a dimension table (parent) of veiligeafstanden table
    add_thema_contexts = [
        PostgresOperator(
            task_id=f"add_context_{key}",
            sql=ADD_THEMA_CONTEXT,
            params=dict(tablename=f"{dag_id}_{key}_new", parent_table=f"{dag_id}_themas_new"),
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
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
                    params=dict(table_name=f"{dag_id}_{key}_new"),
                    result_checker=operator.ge,
                )
            )

            geo_checks.append(
                GEO_CHECK.make_check(
                    check_id=f"geo_check_{key}",
                    params=dict(
                        table_name=f"{dag_id}_{key}_new",
                        geotype=["POLYGON", "POINT"],
                        geo_column="geometrie",
                    ),
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
            data_table_name=f"{dag_id}_{key}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 11. Check for changes to merge in target table
    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_{key}",
            source_table=f"{dag_id}_{key}_new",
            target_table=f"{dag_id}_{key}",
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 12. Clean up (remove temp table _new)
    clean_ups = [
        PostgresOperator(
            task_id="clean_up",
            sql=SQL_DROP_TMP_TABLE,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 13. Drop unnecessary cols
    drop_cols = [
        PostgresOperator(
            task_id=f"drop_cols_{key}",
            sql=DROP_COLS,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in files_to_download.keys()
        if key == "veiligeafstanden"
    ]

    # 14. Drop parent table THEMAS, not needed anymore
    drop_parent_table = PostgresOperator(
        task_id="drop_parent_table",
        sql=[
            f"DROP TABLE IF EXISTS {dag_id}_themas_new CASCADE",
        ],
    )

    # 15. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

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
