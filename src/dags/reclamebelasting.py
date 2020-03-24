from datetime import timedelta
from environs import Env
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

# from airflow.utils.dates import days_ago


env = Env()

RENAME_TABLES_SQL = """
    BEGIN;
    ALTER TABLE IF EXISTS {dag_id} RENAME TO {dag_id}_old;
    ALTER TABLE {dag_id}_new RENAME TO {dag_id};
    DROP TABLE IF EXISTS {dag_id}_old;
    ALTER INDEX {dag_id}_new_pk RENAME TO {dag_id}_pk;
    ALTER INDEX {dag_id}_new_wkb_geometry_geom_idx
        RENAME TO {dag_id}_wkb_geometry_geom_idx;
    COMMIT;
"""

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

dag_config = Variable.get("dag_config", deserialize_json=True)
vsd_config = dag_config["vsd"]
pg_params = " ".join(
    [
        "-X",
        "--set",
        "ON_ERROR_STOP",
        "-h",
        env("POSTGRES_HOST"),
        "-p",
        env("POSTGRES_PORT"),
        "-U",
        env("POSTGRES_USER"),
    ]
)

dag_id = "reclamebelasting"

with DAG(
    "reclamebelasting", default_args=default_args, description="reclamebelasting",
) as dag:

    zip_file = vsd_config[dag_id]["zip_file"]
    shp_file = vsd_config[dag_id]["shp_file"]
    tmp_dir = f"/tmp/{dag_id}"

    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    fetch_zip = BashOperator(
        task_id="fetch_zip",
        bash_command=f"swift download reclame {zip_file} -o {tmp_dir}/{zip_file}",
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

    rename_sql = PostgresOperator(
        task_id="rename_sql", sql=RENAME_TABLES_SQL.format(dag_id=dag_id),
    )

mk_tmp_dir >> fetch_zip >> extract_zip >> extract_shp >> convert_shp >> import_sql >> rename_sql
