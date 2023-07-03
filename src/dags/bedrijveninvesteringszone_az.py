import operator
import os
from functools import partial
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import pg_params
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.convert_bedrijveninvesteringszones_data import convert_biz_data
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from sql.bedrijveninvesteringszones import UPDATE_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_DGEN"]

DAG_ID: Final = "bedrijveninvesteringszones_az"
DATASET_ID: Final = "bedrijveninvesteringszones"
variables = Variable.get(DATASET_ID, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{DATASET_ID}"
files_to_download = variables["files_to_download"]
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=DATASET_ID)

TABLE_ID: Final = f"{DATASET_ID}_{DATASET_ID}"
TMP_TABLE_POSTFIX: Final = "_new"

with DAG(
    DAG_ID,
    description="tariefen, locaties en overige context bedrijveninvesteringszones.",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    mkdir = mk_dir(Path(tmp_dir))

    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="SWIFT_DEFAULT",
            container="bedrijveninvesteringszones",
            object_id=str(file),
            output_path=f"{tmp_dir}/{file}",
        )
        for files in files_to_download.values()
        for file in files
    ]

    # Dummy operator acts as an interface between parallel tasks to another parallel tasks (i.e.
    # lists or tuples) with different number of lanes (without this intermediar, Airflow will
    # give an error)
    Interface = DummyOperator(task_id="interface")

    SHP_to_SQL = [
        BashOperator(
            task_id="SHP_to_SQL",
            bash_command="ogr2ogr -f 'PGDump' " f"{tmp_dir}/{DATASET_ID}.sql {tmp_dir}/{file}",
        )
        for files in files_to_download.values()
        for file in files
        if "shp" in file
    ]

    SQL_convert_UTF8 = BashOperator(
        task_id="convert_to_UTF8",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{DATASET_ID}.sql > "
        f"{tmp_dir}/{DATASET_ID}.utf8.sql",
    )

    SQL_update_data = [
        PythonOperator(
            task_id="update_SQL_data",
            python_callable=convert_biz_data,
            op_args=[
                f"{DATASET_ID}_{DATASET_ID}_new",
                f"{tmp_dir}/{DATASET_ID}.utf8.sql",
                f"{tmp_dir}/{file}",
                f"{tmp_dir}/{DATASET_ID}_updated_data_insert.sql",
            ],
        )
        for files in files_to_download.values()
        for file in files
        if "xlsx" in file
    ]

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DATASET_ID,
        ind_extra_index=True,
    )

    create_temp_table = PostgresTableCopyOperator(
        task_id="create_temp_table",
        dataset_name_lookup=DATASET_ID,
        source_table_name=TABLE_ID,
        target_table_name=f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
        # Only copy table definitions. Don't do anything else.
        truncate_target=False,
        copy_data=False,
        drop_source=False,
    )

    import_data = BashOperator(
        task_id="import_data",
        bash_command=f"psql {db_conn_string()} < {tmp_dir}/{DATASET_ID}_updated_data_insert.sql",
    )

    update_table = PostgresOnAzureOperator(
        task_id="update_target_table",
        sql=UPDATE_TABLE,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=50,
            params={"table_name": f"{DATASET_ID}_{DATASET_ID}_new "},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": f"{DATASET_ID}_{DATASET_ID}_new",
                "geotype": ["POLYGON"],
            },
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    rename_temp_table = PostgresTableRenameOperator(
        task_id=f"rename_tables_for_{TABLE_ID}",
        new_table_name=TABLE_ID,
        old_table_name=f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
        cascade=True,
    )
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)


slack_at_start >> mkdir >> download_data

for data in download_data:
    data >> Interface

(
    Interface
    >> SHP_to_SQL
    >> SQL_convert_UTF8
    >> SQL_update_data
    >> sqlalchemy_create_objects_from_schema
    >> create_temp_table
    >> import_data
    >> update_table
    >> multi_checks
    >> rename_temp_table
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains data about business investment zones
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    https://data.amsterdam.nl/datasets/bl6Wf85K8CfnwA/bedrijfsinvesteringszones-biz/
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/bedrijveninvesteringszones.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/bedrijveninvesteringszones.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=bedrijveninvesteringszones/bedrijveninvesteringszones&x=106434&y=488995&radius=10
"""
