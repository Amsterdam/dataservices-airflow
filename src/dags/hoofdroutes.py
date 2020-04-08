from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow import DAG


from common import pg_params, default_args
from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_COLNAMES,
    SQL_CHECK_GEO,
)
from importscripts.import_hoofdroutes import import_hoofdroutes
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator


dag_id = "hoofdroutes"

with DAG(dag_id, default_args=default_args,) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    tmp_file_prefix = f"{tmp_dir}/{dag_id}"
    colnames = [["id"], ["name"], ["ogc_fid"], ["route"], ["type"], ["wkb_geometry"]]

    import_routes = PythonOperator(
        task_id="import_routes",
        python_callable=import_hoofdroutes,
        op_args=[f"{tmp_file_prefix}.json"],
    )

    extract_geojson = BashOperator(
        task_id="extract_geojson",
        bash_command=f"ogr2ogr -f 'PGDump' -nlt MULTILINESTRING "
        "-t_srs EPSG:28992 -s_srs EPSG:4326 "
        f"-nln {dag_id}_new "
        f"{tmp_file_prefix}.sql {tmp_file_prefix}.json",
    )

    load_table = BashOperator(
        task_id="load_table", bash_command=f"psql {pg_params} < {tmp_file_prefix}.sql",
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=3),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(tablename=f"{dag_id}_new", geotype="ST_MultiLineString"),
    )

    check_colnames = PostgresValueCheckOperator(
        task_id="check_colnames",
        sql=SQL_CHECK_COLNAMES,
        pass_value=colnames,
        params=dict(tablename=f"{dag_id}_new"),
    )

    rename_table = PostgresOperator(
        task_id="rename_table", sql=SQL_TABLE_RENAME, params=dict(tablename=dag_id),
    )

import_routes >> extract_geojson >> load_table >> [
    check_count,
    check_geo,
    check_colnames,
] >> rename_table
