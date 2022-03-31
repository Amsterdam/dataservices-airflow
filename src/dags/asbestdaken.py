from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params
)
from common.sql import SQL_TABLE_RENAMES
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_permissions_operator import PostgresPermissionsOperator

# from airflow.providers.postgres.operators.postgres import PostgresOperator
from swift_operator import SwiftOperator

SQL_RENAME_COL: Final = """
ALTER TABLE asbestdaken_daken_new RENAME COLUMN identifica TO pandidentificatie
"""

dag_id = "asbestdaken"
dag_config = Variable.get(dag_id, deserialize_json=True)

with DAG(
    dag_id,
    description="heeft status NIET BESCHIKBAAR. Geen active API.",
    default_args=default_args,
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id="asbestdaken"),
) as dag:

    extract_shps = []
    convert_shps = []
    load_dumps = []
    zip_file = dag_config["zip_file"]
    shp_files = dag_config["shp_files"]
    tables = dag_config["tables"]
    rename_tablenames = dag_config["rename_tablenames"]
    tmp_dir = f"{SHARED_DIR}/{dag_id}"

   # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start"
    )

    fetch_zip = SwiftOperator(
        task_id="fetch_zip",
        swift_conn_id="SWIFT_DEFAULT",
        container="asbest",
        object_id=zip_file,
        output_path=f"{tmp_dir}/{zip_file}",
    )

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f'unzip -o "{tmp_dir}/{zip_file}" -d {tmp_dir}',
    )

    for shp_filename, tablename in zip(shp_files, tables):
        extract_shps.append(
            BashOperator(
                task_id=f"extract_{shp_filename}",
                bash_command="ogr2ogr -f 'PGDump' -t_srs EPSG:28992 "
                f"-nln {tablename} "
                f"{tmp_dir}/{tablename}.sql {tmp_dir}/Shape/{shp_filename}",
            )
        )

    for tablename in tables:
        convert_shps.append(
            BashOperator(
                task_id=f"convert_{tablename}",
                bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{tablename}.sql > "
                f"{tmp_dir}/{tablename}.utf8.sql",
            )
        )

        load_dumps.append(
            BashOperator(
                task_id=f"load_{tablename}",
                bash_command=f"psql {pg_params()} < {tmp_dir}/{tablename}.utf8.sql",
            )
        )

    rename_tables = PostgresOperator(
        task_id="rename_tables",
        sql=SQL_TABLE_RENAMES,
        params={"tablenames": rename_tablenames},
    )

    rename_col = PostgresOperator(task_id="rename_col", sql=SQL_RENAME_COL)

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> fetch_zip >> extract_zip >> extract_shps

for extract_shp, convert_shp, load_dump in zip(extract_shps, convert_shps, load_dumps):
    extract_shp >> convert_shp >> load_dump

load_dumps[0] >> rename_col >> rename_tables >> grant_db_permissions
load_dumps[1] >> rename_tables >> grant_db_permissions
