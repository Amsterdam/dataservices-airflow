import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.explosieven import REMOVE_COLS, ADD_HYPERLINK_PDF
from swift_operator import SwiftOperator

dag_id = "explosieven"
variables_bodem = Variable.get("explosieven", deserialize_json=True)
files_to_download = variables_bodem["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="(1) bominslagen, (2) gevrijwaardegebieden, (3) verdachtgebieden en (4) uitgevoerde onderzoeken",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
    template_searchpath=["/"],
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
            task_id=f"download_{file}",
            swift_conn_id="OBJECTSTORE_MILIEUTHEMAS",
            container="Bommenkaart",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for files in files_to_download.values()
        for file in files
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Create SQL
    SHP_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{key}",
            bash_command="ogr2ogr -f 'PGDump' "
            "-s_srs EPSG:28992 -t_srs EPSG:28992 "
            f"-nln {key} "
            f"{tmp_dir}/{key}.sql {tmp_dir}/{file} "
            "-lco GEOMETRY_NAME=geometry "
            "-nlt PROMOTE_TO_MULTI "
            "-lco precision=NO "
            "-lco FID=id",
        )
        for key, files in files_to_download.items()
        for file in files
        if "shp" in file
    ]

    # 6. Create TABLE
    create_tables = [
        BashOperator(
            task_id=f"create_table_{key}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{key}.sql",
        )
        for key in files_to_download.keys()
    ]

    # 7. Remove unnecessary cols
    remove_cols = [
        PostgresOperator(
            task_id=f"remove_cols_{key}",
            sql=REMOVE_COLS,
            params=dict(tablename=key),
        )
        for key in files_to_download.keys()
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        rename_indexes=False,
        pg_schema="public",
    )

    # 9. Add PDF hyperlink only for table bominslag and verdachtgebied
    add_hyperlink_pdf = [
        PostgresOperator(
            task_id=f"add_hyperlink_pdf_{table}",
            sql=ADD_HYPERLINK_PDF,
            params=dict(tablename=table),
        )
        for table in ("bominslag", "verdachtgebied")
    ]

    # 10. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 11. Drop Exisiting TABLE
    drop_tables = [
        PostgresOperator(
            task_id=f"drop_existing_table_{key}",
            sql=[
                f"DROP TABLE IF EXISTS {dag_id}_{key} CASCADE",
            ],
        )
        for key in files_to_download.keys()
    ]

    # 12. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=key,
            new_table_name=f"{dag_id}_{key}",
        )
        for key in files_to_download.keys()
    ]

    # Prepare the checks and added them per source to a dictionary
    for key in files_to_download.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=2,
                params=dict(table_name=key),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params=dict(
                    table_name=key,
                    geotype=["POINT", "MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 13. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in files_to_download.keys()
    ]

    # 14. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> download_data

for data in zip(download_data):

    data >> Interface

Interface >> SHP_to_SQL

for (create_SQL, create_table, remove_col, multi_check, drop_table, rename_table,) in zip(
    SHP_to_SQL,
    create_tables,
    remove_cols,
    multi_checks,
    drop_tables,
    rename_tables,
):

    [create_SQL >> create_table >> remove_col] >> provenance_translation

    [multi_check >> drop_table >> rename_table]

provenance_translation >> add_hyperlink_pdf >> Interface2 >> multi_checks

rename_tables >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains explosives related topics
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/explosieven/bominslag/
    https://api.data.amsterdam.nl/v1/explosieven/gevrijwaardgebied/
    https://api.data.amsterdam.nl/v1/explosieven/verdachtgebied/
    https://api.data.amsterdam.nl/v1/explosieven/uitgevoerdonderzoek/
"""
