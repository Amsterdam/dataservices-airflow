import pathlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator

from common import (
    pg_params,
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from common.sql import SQL_CHECK_COUNT, SQL_CHECK_GEO, SQL_CHECK_COLNAMES

from importscripts.convert_biz_data import convert_biz_data

SQL_TABLE_RENAME = """
    ALTER TABLE IF EXISTS biz_data RENAME TO biz_data_old;
    ALTER TABLE biz_data_new RENAME TO biz_data;
    DROP TABLE IF EXISTS biz_data_old CASCADE;
    ALTER VIEW biz_view_new RENAME TO biz_view;
    ALTER INDEX naam_unique_new RENAME TO naam_unique;
    ALTER INDEX biz_data_new_wkb_geometry_geom_idx RENAME TO biz_data_wkb_geometry_geom_idx;
"""

dag_id = "biz"
dag_config = Variable.get(dag_id, deserialize_json=True)
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data"


with DAG(dag_id, default_args=default_args, template_searchpath=["/"]) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    base_filename = dag_config["base_filename"]
    sql_filebase = f"{tmp_dir}/{base_filename}"
    colnames = [
        ["bijdrageplichtigen"],
        ["biz_id"],
        ["biz_type"],
        ["heffing"],
        ["heffingsgrondslag"],
        ["naam"],
        ["verordening"],
        ["website"],
        ["wkb_geometry"],
    ]

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    extract_shp = BashOperator(
        task_id="extract_shp",
        bash_command=f"ogr2ogr -f 'PGDump' "
        f"{sql_filebase}.sql {data_path}/{dag_id}/{base_filename}.shp",
    )
    convert_shp = BashOperator(
        task_id="convert_shp",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {sql_filebase}.sql > "
        f"{sql_filebase}.utf8.sql",
    )

    convert_data = PythonOperator(
        task_id="convert_data",
        python_callable=convert_biz_data,
        op_args=[
            f"{sql_filebase}.utf8.sql",
            f"{data_path}/{dag_id}/BIZ Dataset 2020-01014.xlsx",
            f"{tmp_dir}/biz_data_insert.sql",
        ],
    )

    create_tables = BashOperator(
        task_id="create_tables",
        bash_command=f"psql {pg_params} < {sql_path}/biz_data_create.sql",
    )

    import_data = BashOperator(
        task_id="import_data",
        bash_command=f"psql {pg_params} < {tmp_dir}/biz_data_insert.sql",
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename="biz_view_new", mincount=48),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(
            tablename="biz_view_new", geotype="ST_Polygon", geo_column="geometrie"
        ),
    )

    check_colnames = PostgresValueCheckOperator(
        task_id="check_colnames",
        sql=SQL_CHECK_COLNAMES,
        pass_value=colnames,
        params=dict(tablename=f"{dag_id}_data_new"),
    )

    rename_table = PostgresOperator(task_id="rename_table", sql=SQL_TABLE_RENAME)

    (
        slack_at_start
        >> mkdir
        >> extract_shp
        >> convert_shp
        >> convert_data
        >> create_tables
        >> import_data
        >> [check_count, check_geo, check_colnames,]
        >> rename_table
    )
