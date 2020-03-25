from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

# from airflow.operators.postgres_operator import PostgresOperator
from swift_operator import SwiftOperator

default_args = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": "2020-03-18",
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": False,  # do not backfill
}

dag_id = "parkeerzones"
dag_config = Variable.get("dag_config", deserialize_json=True)
vsd_config = dag_config["vsd"]


with DAG(dag_id, default_args=default_args,) as dag:

    extract_shps = []
    convert_shps = []
    zip_folder = vsd_config[dag_id]["zip_folder"]
    zip_file = vsd_config[dag_id]["zip_file"]
    shp_files = vsd_config[dag_id]["shp_files"]
    tables = vsd_config[dag_id]["tables"]
    tmp_dir = f"/tmp/{dag_id}"

    fetch_zip = SwiftOperator(
        task_id="fetch_zip",
        container=dag_id,
        object_id=zip_file,
        output_path=f"/tmp/{dag_id}/{zip_file}",
    )

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f'unzip -o "{tmp_dir}/{zip_file}" -d {tmp_dir}',
    )

    for shp_filename, tablename in zip(shp_files, tables):
        extract_shps.append(
            BashOperator(
                task_id=f"extract_{shp_filename}",
                bash_command=f"ogr2ogr -f 'PGDump' -t_srs EPSG:28992 "
                f" -s_srs EPSG:4326 -nln {tablename} "
                f"{tmp_dir}/{tablename}.sql {tmp_dir}/{zip_folder}/",
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

        # rename_tables.append(
        #     PostgresOperator(
        #         task_id=f"rename_table_for_{ds_filename}",
        #         sql=RENAME_TABLES_SQL.format(ds_filename=ds_filename),
        #     )

for extract_shp, convert_shp in zip(extract_shps, convert_shps):
    extract_shp >> convert_shp

fetch_zip >> extract_zip >> extract_shps
