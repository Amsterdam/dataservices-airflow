from environs import Env
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from swift_operator import SwiftOperator

from common import pg_params, default_args

env = Env()


dag_id = "reclamebelasting"
config = Variable.get("dag_config", deserialize_json=True)
generic_config = config["vsd"]["generic"]
dag_config = config["vsd"][dag_id]

with DAG(
    "reclamebelasting", default_args=default_args, description="reclamebelasting",
) as dag:

    zip_file = dag_config["zip_file"]
    shp_file = dag_config["shp_file"]
    sql_table_rename = generic_config["sql_table_rename"]
    tmp_dir = f"/tmp/{dag_id}"

    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    fetch_zip = SwiftOperator(
        task_id="fetch_zip",
        container=dag_id,
        object_id=zip_file,
        output_path=f"/tmp/{dag_id}/{zip_file}",
    )

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f"unzip -o {tmp_dir}/{zip_file} -d {tmp_dir}",
    )

    extract_shp = BashOperator(
        task_id="extract_shp",
        bash_command=f"ogr2ogr -f 'PGDump' -t_srs EPSG:28992 -nln {dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {tmp_dir}/Reclame_tariefgebieden.shp",
    )

    convert_shp = BashOperator(
        task_id="convert_shp",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    import_sql = BashOperator(
        task_id="import_sql",
        bash_command=f"psql {pg_params} < {tmp_dir}/{dag_id}.utf8.sql",
    )

    rename_table = PostgresOperator(
        task_id="rename_table", sql=sql_table_rename, params=dict(tablename=dag_id),
    )

mk_tmp_dir >> fetch_zip >> extract_zip >> extract_shp >> convert_shp >> import_sql >> rename_table
