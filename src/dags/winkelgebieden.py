import pathlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from postgres_check_operator import PostgresCheckOperator

from common import (
    vsd_default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
)


def quote(instr):
    return f"'{instr}'"


dag_id = "winkgeb"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"

with DAG(
    dag_id,
    default_args=vsd_default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
) as dag:

    tmp_dir = f"/tmp/{dag_id}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    extract_data = BashOperator(
        task_id="extract_data",
        bash_command=f"ogr2ogr -f 'PGDump' "
        f"-t_srs EPSG:28992 -nln {dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {data_path}/{dag_id}/winkgeb2018.TAB",
    )

    convert_data = BashOperator(
        task_id="convert_data",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params} < {tmp_dir}/{dag_id}.utf8.sql",
    )

    add_category = BashOperator(
        task_id="add_category",
        bash_command=f"psql {pg_params} < {sql_path}/add_categorie.sql",
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=75),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(
            tablename=f"{dag_id}_new",
            geotype=["ST_Polygon", "ST_MultiPolygon"],
            check_valid=False,
        ),
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params=dict(tablename=f"{dag_id}"),
    )


(
    slack_at_start
    >> mkdir
    >> extract_data
    >> convert_data
    >> create_table
    >> add_category
    >> [check_count, check_geo]
    >> rename_table
)


"""
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -nln winkgeb_new  ${TMPDIR}/winkgeb.sql ${DATA_DIR}/winkgeb2018.TAB

iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/winkgeb.sql > ${TMPDIR}/winkgeb.utf8.sql

echo "Create tables & import data for winkel gebieden"
psql -X --set ON_ERROR_STOP=on <<SQL
\\i ${TMPDIR}/winkgeb.utf8.sql
BEGIN;
\\i ${DATA_DIR}/add_categorie.sql
COMMIT;
SQL

${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS winkgeb RENAME TO winkgeb_old;
ALTER TABLE winkgeb_new RENAME TO winkgeb;
DROP TABLE IF EXISTS winkgeb_old CASCADE;
ALTER INDEX winkgeb_new_pk RENAME TO winkgeb_pk;
ALTER INDEX winkgeb_new_wkb_geometry_geom_idx RENAME TO winkgeb_wkb_geometry_geom_idx;
COMMIT;
"""
