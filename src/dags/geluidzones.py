import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params,
    quote_string,
    slack_webhook_token,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.geluidzones import ADD_THEMA_CONTEXT, DROP_COLS, SET_GEOM
from swift_operator import SwiftOperator

dag_id = "geluidszones"
variables_bodem = Variable.get("geluidszones", deserialize_json=True)
files_to_download = variables_bodem["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

with DAG(
    dag_id,
    description="geluidszones metro, spoorwegen, industrie en schiphol",
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
            bash_command=f"cat {tmp_dir}/{key}_{file} | sed 's/|/;/g' > {tmp_dir}/seperator_{key} ;"
            f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/seperator_{key} > "
            f"{tmp_dir}/utf_8_{key.replace('-','_')}.csv",
        )
        for key, file in files_to_download.items()
    ]

    # 5. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 6. Create SQL
    # the -sql parameter is added to store the different data subjects, from one source file, in separate tables
    # the OR processes themas en schiphol which already are in separate files, and don't work with type LIKE
    csv_to_SQL = [
        BashOperator(
            task_id=f"generate_SQL_{splitted_tablename}",
            bash_command="ogr2ogr -f 'PGDump' "
            "-t_srs EPSG:28992 "
            f"-nln {dag_id}_{splitted_tablename}_new "
            f"{tmp_dir}/{splitted_tablename}.sql {tmp_dir}/utf_8_{key.replace('-','_')}.csv "
            "-lco SEPARATOR=SEMICOLON "
            "-oo AUTODETECT_TYPE=YES "
            "-lco FID=ID "
            f"-sql \"SELECT * FROM utf_8_{key.replace('-','_')} WHERE TYPE LIKE '%{splitted_tablename}%' OR '{splitted_tablename}' IN ('themas', 'schiphol')\"",
        )
        for key in files_to_download.keys()
        for splitted_tablename in key.split("-")
    ]

    # 7. Create TABLES
    create_tables = [
        BashOperator(
            task_id=f"create_table_{splitted_tablename}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{splitted_tablename}.sql",
        )
        for key in files_to_download.keys()
        for splitted_tablename in key.split("-")
    ]

    # 8. RE-define GEOM type (because ogr2ogr cannot set geom with .csv import)
    # except themas itself, which is a dimension table (parent) of sound zones tables
    redefine_geoms = [
        PostgresOperator(
            task_id=f"re-define_geom_{splitted_tablename}",
            sql=SET_GEOM,
            params=dict(tablename=f"{dag_id}_{splitted_tablename}_new"),
        )
        for key in files_to_download.keys()
        for splitted_tablename in key.split("-")
        if "themas" not in key
    ]

    # 9. Add thema-context to child tables from parent table (themas)
    # except themas itself, which is a dimension table (parent) of sound zones tables
    add_thema_contexts = [
        PostgresOperator(
            task_id=f"add_context_{splitted_tablename}",
            sql=ADD_THEMA_CONTEXT,
            params=dict(
                tablename=f"{dag_id}_{splitted_tablename}_new", parent_table=f"{dag_id}_themas_new"
            ),
        )
        for key in files_to_download.keys()
        for splitted_tablename in key.split("-")
        if "themas" not in key
    ]

    # 10. Rename COLUMNS based on Provenance
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
        for splitted_tablename in key.split("-"):
            if "themas" not in key:

                total_checks.clear()
                count_checks.clear()
                geo_checks.clear()

                count_checks.append(
                    COUNT_CHECK.make_check(
                        check_id=f"count_check_{splitted_tablename}",
                        pass_value=2,
                        params=dict(table_name=f"{dag_id}_{splitted_tablename}_new"),
                        result_checker=operator.ge,
                    )
                )

                geo_checks.append(
                    GEO_CHECK.make_check(
                        check_id=f"geo_check_{splitted_tablename}",
                        params=dict(
                            table_name=f"{dag_id}_{splitted_tablename}_new",
                            geotype=["MULTIPOLYGON"],
                        ),
                        pass_value=1,
                    )
                )

            total_checks = count_checks + geo_checks
            check_name[splitted_tablename] = total_checks

    # 11. Execute bundled checks on database (see checks definition here above)
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{splitted_tablename}",
            checks=check_name[splitted_tablename],
        )
        for key in files_to_download.keys()
        for splitted_tablename in key.split("-")
        if "themas" not in key
    ]

    # 12. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 13. Rename TABLES
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{splitted_tablename}",
            old_table_name=f"{dag_id}_{splitted_tablename}_new",
            new_table_name=f"{dag_id}_{splitted_tablename}",
        )
        for key in files_to_download.keys()
        for splitted_tablename in key.split("-")
        if "themas" not in key
    ]

    # 14. Drop unnecessary cols from tables Metro en Spoorwegen
    drop_cols = [
        PostgresOperator(
            task_id=f"drop_cols_{key}",
            sql=DROP_COLS,
            params=dict(tablename=f"{dag_id}_{key}"),
        )
        for key in ("metro", "spoorwegen")
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

slack_at_start >> mkdir >> download_data

for (data, change_seperator) in zip(download_data, change_seperators):

    [data >> change_seperator] >> Interface

Interface >> csv_to_SQL

for create_SQL, create_table in zip(csv_to_SQL, create_tables):

    [create_SQL >> create_table] >> provenance_translation

provenance_translation >> redefine_geoms

for (
    redefine_geom,
    add_thema_context,
    multi_check,
    rename_table,
) in zip(redefine_geoms, add_thema_contexts, multi_checks, rename_tables):

    [redefine_geom >> add_thema_context >> multi_check >> rename_table] >> Interface2

Interface2 >> drop_cols

for drop_col in zip(drop_cols):
    drop_col >> drop_parent_table


drop_parent_table >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains sound zones related to metro, spoorwegen, industrie and schiphol.
    Source files are located @objectstore.eu, container: Milieuthemas
    Metro, spoorwegen and industrie are in source in one file
    Schipol is separated in its own file
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    NA
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/geluidszones.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/geluidszones.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=geluidszones/schiphol&x=111153&y=483288&radius=10
"""
