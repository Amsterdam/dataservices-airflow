import pathlib
from datetime import timedelta
from environs import Env
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# from airflow.operators.postgres_operator import PostgresOperator
from swift_operator import SwiftOperator

env = Env()

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
config = Variable.get("dag_config", deserialize_json=True)
dag_config = config["vsd"][dag_id]
# slack_webhook_token = BaseHook.get_connection("slack").password
slack_webhook_token = env("SLACK_WEBHOOK")
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
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

with DAG(dag_id, default_args=default_args,) as dag:

    extract_shps = []
    convert_shps = []
    load_dumps = []
    rename_cols = []
    zip_folder = dag_config["zip_folder"]
    zip_file = dag_config["zip_file"]
    shp_files = dag_config["shp_files"]
    col_renames = dag_config["col_renames"]
    sql_rename = dag_config["sql_rename"]
    sql_add_color = dag_config["sql_add_color"]
    sql_update_colors = dag_config["sql_update_colors"]
    sql_delete_unused = dag_config["sql_delete_unused"]
    table_renames = dag_config["table_renames"]
    tables = dag_config["tables"]
    rename_tablenames = dag_config["rename_tablenames"]
    tmp_dir = f"/tmp/{dag_id}"

    slack_at_start = SlackWebhookOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id}",
        username="admin",
    )

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
                f"{tmp_dir}/{tablename}.sql {tmp_dir}/{zip_folder}/{shp_filename}",
            )
        )

    for tablename, col_rename in zip(tables, col_renames):
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
                bash_command=f"psql {pg_params} < {tmp_dir}/{tablename}.utf8.sql",
            )
        )

        rename_cols.append(
            PostgresOperator(
                task_id=f"rename_cols_{tablename}",
                sql=sql_rename,
                params=dict(tablename=tablename, col_rename=col_rename),
            )
        )

    rename_tables = PostgresOperator(
        task_id="rename_tables",
        sql=table_renames,
        params=dict(tablenames=rename_tablenames),
    )

    add_color = PostgresOperator(task_id="add_color", sql=sql_add_color)
    update_colors = PostgresOperator(task_id="update_colors", sql=sql_update_colors)

    delete_unused = PostgresOperator(task_id="delete_unused", sql=sql_delete_unused)

    load_map_colors = BashOperator(
        task_id="load_map_colors",
        bash_command=f"psql {pg_params} < {sql_path}/parkeerzones_map_color.sql",
    )

rename_cols[1] >> delete_unused >> rename_tables
rename_cols[0] >> add_color >> load_map_colors >> update_colors >> rename_tables

for extract_shp, convert_shp, load_dump, rename_col in zip(
    extract_shps, convert_shps, load_dumps, rename_cols
):
    extract_shp >> convert_shp >> load_dump >> rename_col

slack_at_start >> fetch_zip >> extract_zip >> extract_shps
