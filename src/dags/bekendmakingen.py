from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

# from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow import DAG
from airflow.models import Variable
from http_fetch_operator import HttpFetchOperator
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator

# from .common import pg_params, default_args, slack_webhook_token
from common import pg_params, default_args
from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
    SQL_DROP_TABLE,
    SQL_CHECK_COLNAMES,
)

SQL_DROP_BBOX = """
    BEGIN;
    ALTER TABLE bekendmakingen_new DROP column bbox;
    COMMIT;
"""

dag_id = "bekendmakingen"
dag_config = Variable.get(dag_id, deserialize_json=True)

with DAG(dag_id, default_args=default_args,) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    tmp_file_prefix = f"{tmp_dir}/{dag_id}"
    colnames = [
        ["beschrijving"],
        ["categorie"],
        ["datum"],
        ["id"],
        ["ogc_fid"],
        ["oid_"],
        ["onderwerp"],
        ["overheid"],
        ["plaats"],
        ["postcodehuisnummer"],
        ["straat"],
        ["titel"],
        ["url"],
        ["wkb_geometry"],
    ]

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
        sql=SQL_DROP_TABLE,
        params=dict(tablename=f"{dag_id}_new"),
    )

    load_table = BashOperator(
        task_id="load_table", bash_command=f"psql {pg_params} < {tmp_file_prefix}.sql",
    )

    drop_bbox = PostgresOperator(task_id="drop_bbox", sql=SQL_DROP_BBOX)

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=1000),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(tablename=f"{dag_id}_new", geotype="ST_Point"),
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

    wfs_fetch >> extract_wfs >> drop_table >> load_table >> drop_bbox >> [
        check_count,
        check_geo,
        check_colnames,
    ] >> rename_table

#  ${SCRIPT_DIR}/check_imported_data.py
