from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

# from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow import DAG
from airflow.models import Variable
from http_fetch_operator import HttpFetchOperator
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator

# from .common import pg_params, default_args, slack_webhook_token
from common import pg_params, default_args


dag_id = "bekendmakingen"
config = Variable.get("dag_config", deserialize_json=True)
generic_config = config["vsd"]["generic"]
dag_config = config["vsd"][dag_id]

with DAG(dag_id, default_args=default_args,) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    tmp_file_prefix = f"{tmp_dir}/{dag_id}"
    sql_drop_table = generic_config["sql_drop_table"]
    sql_table_rename = generic_config["sql_table_rename"]
    sql_check_count = generic_config["sql_check_count"]
    sql_check_geo = generic_config["sql_check_geo"]
    sql_check_colnames = generic_config["sql_check_colnames"]
    geotype = dag_config["geotype"]
    colnames = dag_config["colnames"]
    sql_drop_bbox = dag_config["sql_drop_bbox"]

    wfs_fetch = HttpFetchOperator(
        task_id="wfs_fetch",
        endpoint=dag_config["wfs_endpoint"],
        http_conn_id="geozet_conn_id",
        data=dag_config["wfs_params"],
        tmp_file=f"{tmp_file_prefix}.json",
    )

    extract_wfs = BashOperator(
        task_id="extract_wfs",
        bash_command=f"ogr2ogr -f 'PGDump' -a_srs EPSG:28992 "
        f"-nln {dag_id}_new "
        f"{tmp_file_prefix}.sql {tmp_file_prefix}.json",
    )

    drop_table = PostgresOperator(
        task_id="drop_table",
        sql=sql_drop_table,
        params=dict(tablename=f"{dag_id}_new"),
    )

    load_table = BashOperator(
        task_id="load_table", bash_command=f"psql {pg_params} < {tmp_file_prefix}.sql",
    )

    drop_bbox = PostgresOperator(task_id="drop_bbox", sql=sql_drop_bbox)

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=sql_check_count,
        params=dict(tablename=f"{dag_id}_new", mincount=3),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=sql_check_geo,
        params=dict(tablename=f"{dag_id}_new", geotype=geotype),
    )

    check_colnames = PostgresValueCheckOperator(
        task_id="check_colnames",
        sql=sql_check_colnames,
        pass_value=colnames,
        params=dict(tablename=f"{dag_id}_new"),
    )

    rename_table = PostgresOperator(
        task_id="rename_table", sql=sql_table_rename, params=dict(tablename=dag_id),
    )

    wfs_fetch >> extract_wfs >> drop_table >> load_table >> drop_bbox >> [
        check_count,
        check_geo,
        check_colnames,
    ] >> rename_table

#  ${SCRIPT_DIR}/check_imported_data.py
