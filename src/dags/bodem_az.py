import os
import operator
from functools import partial
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import pg_params
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.bodem_set_geo_datatype import SET_GEOM
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "bodem_az"
DATASET_ID: Final = "bodem"
variables_bodem = Variable.get("bodem", deserialize_json=True)
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
    description="bodem analyse onderzoeken",
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
            task_id=f"download_file_{key}",
            swift_conn_id="OBJECTSTORE_MILIEUTHEMAS",
            container="Milieuthemas",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for key, file in files_to_download.items()
    ]

    # 4. Transform seperator from pipeline to semicolon and set code schema to UTF-8
    change_seperator = [
        BashOperator(
            task_id=f"change_seperator_{key}",
            bash_command=f"cat {tmp_dir}/{file} | sed 's/|/;/g' > {tmp_dir}/seperator_{file} ;"
            f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/seperator_{file} > "
            f"{tmp_dir}/utf-8_{file}",
        )
        for key, file in files_to_download.items()
    ]
    # 5. Create SQL
    csv_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{key}",
            bash_command="ogr2ogr -f 'PGDump' "
            "-t_srs EPSG:28992 "
            f"-nln {key} "
            f"{tmp_dir}/{DATASET_ID}_{key}.sql {tmp_dir}/utf-8_{file} "
            "-lco SEPARATOR=SEMICOLON "
            "-oo AUTODETECT_TYPE=YES "
            "-lco FID=ID",
        )
        for key, file in files_to_download.items()
    ]

    # 6. Create TABLE
    create_tables = [
        BashOperator(
            task_id=f"create_table_{key}",
            bash_command=f"psql {db_conn_string()} < {tmp_dir}/{DATASET_ID}_{key}.sql",
        )
        for key, _ in files_to_download.items()
    ]

    # 7. RE-define GEOM type (because ogr2ogr cannot set geom with .csv import)
    redefine_geom = [
        PostgresOnAzureOperator(
            task_id=f"re-define_geom_{key}",
            sql=SET_GEOM,
            params=dict(tablename=key),
        )
        for key, _ in files_to_download.items()
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        rename_indexes=False,
        pg_schema="public",
    )

    # 9. Drop Exisiting TABLE
    drop_tables = [
        PostgresOnAzureOperator(
            task_id=f"drop_existing_table_{key}",
            sql=[
                f"DROP TABLE IF EXISTS {DATASET_ID}_{key} CASCADE",
            ],
        )
        for key in files_to_download.keys()
    ]

    # 10. Rename TABLE
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
                    geotype=["POINT"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 11. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in files_to_download.keys()
    ]

    # 12. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

for (
    data,
    change_seperator,
    create_SQL,
    create_table,
    redefine_geom,
    multi_check,
    drop_table,
    rename_table,
) in zip(
    download_data,
    change_seperator,
    csv_to_SQL,
    create_tables,
    redefine_geom,
    multi_checks,
    drop_tables,
    rename_tables,
):

    [
        data >> change_seperator >> create_SQL >> create_table >> redefine_geom
    ] >> provenance_translation

    [multi_check >> drop_table >> rename_table]


provenance_translation >> multi_checks

rename_tables >> grant_db_permissions

(slack_at_start >> mkdir >> download_data)

dag.doc_md = """
    #### DAG summary
    This DAG contains soil (bodem) analyses data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/bodem/grond/
    https://api.data.amsterdam.nl/v1/bodem/grondwater/
    https://api.data.amsterdam.nl/v1/bodem/asbest/
"""
