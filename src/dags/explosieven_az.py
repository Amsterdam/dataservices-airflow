import os
import operator
from functools import partial
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import pg_params
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.explosieven import ADD_HYPERLINK_PDF, REMOVE_COLS
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "explosieven_az"
DATASET_ID: Final = "explosieven"
variables_bodem = Variable.get("explosieven", deserialize_json=True)
files_to_download = variables_bodem["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{DATASET_ID}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=DATASET_ID)

with DAG(
    DAG_ID,
    description="(1) bominslagen, (2) gevrijwaardegebieden, (3) verdachtgebieden en (4) uitgevoerde onderzoeken",
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
            bash_command=f"psql {db_conn_string()} < {tmp_dir}/{key}.sql",
        )
        for key in files_to_download.keys()
    ]

    # 7. Remove unnecessary cols
    remove_cols = [
        PostgresOnAzureOperator(
            task_id=f"remove_cols_{key}",
            sql=REMOVE_COLS,
            params=dict(tablename=key),
        )
        for key in files_to_download.keys()
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        rename_indexes=False,
        pg_schema="public",
    )

    # 9. Add PDF hyperlink only for table bominslag and verdachtgebied
    add_hyperlink_pdf = [
        PostgresOnAzureOperator(
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
        PostgresOnAzureOperator(
            task_id=f"drop_existing_table_{key}",
            sql=[
                f"DROP TABLE IF EXISTS {DATASET_ID}_{key} CASCADE",
            ],
        )
        for key in files_to_download.keys()
    ]

    # 12. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=key,
            new_table_name=f"{DATASET_ID}_{key}",
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
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

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
