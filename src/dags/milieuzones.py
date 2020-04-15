import pathlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
    SQL_CHECK_COLNAMES,
)

from importscripts.import_milieuzones import import_milieuzones

SQL_FIX_TABLES = """
    ALTER TABLE milieuzones_new ADD COLUMN vanafdatum_new DATE;
    UPDATE milieuzones_new SET vanafdatum_new = to_date(vanafdatum, 'DD-MM-YYYY');
    ALTER TABLE milieuzones_new DROP COLUMN vanafdatum;
    ALTER TABLE milieuzones_new RENAME COLUMN vanafdatum_new TO vanafdatum;
    ALTER TABLE milieuzones_new ADD COLUMN color VARCHAR(7);
    UPDATE milieuzones_new SET color = c.color FROM
      ( VALUES('vracht', '#772b90'), ('bestel', '#f7f706'),
              ('taxi','#ec008c'), ( 'brom- en snorfiets', '#3062b7'),
              ('touringcar', '#fa9f1b')) AS c(verkeerstype, color)
      WHERE milieuzones_new.verkeerstype = c. verkeerstype;
"""

dag_id = "milieuzones"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data"


with DAG(dag_id, default_args=default_args, template_searchpath=["/"]) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    colnames = [
        ["color"],
        ["id"],
        ["ogc_fid"],
        ["vanafdatum"],
        ["verkeerstype"],
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

    import_data = PythonOperator(
        task_id="import_data",
        python_callable=import_milieuzones,
        op_args=[
            f"{data_path}/{dag_id}/{dag_id}.json",
            f"{tmp_dir}/{dag_id}.geo.json",
        ],
    )

    extract_geojson = BashOperator(
        task_id="extract_geojson",
        bash_command=f"ogr2ogr -f 'PGDump' "
        f"-t_srs EPSG:28992 -s_srs EPSG:4326 -nln {dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {tmp_dir}/{dag_id}.geo.json",
    )

    create_and_fix_table = PostgresOperator(
        task_id="create_and_fix_table", sql=[f"{tmp_dir}/{dag_id}.sql", SQL_FIX_TABLES],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=5),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(
            tablename=f"{dag_id}_new", geotype="ST_MultiPolygon", check_valid=False
        ),
    )

    check_colnames = PostgresValueCheckOperator(
        task_id="check_colnames",
        sql=SQL_CHECK_COLNAMES,
        pass_value=colnames,
        params=dict(tablename=f"{dag_id}_new"),
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params=dict(tablename=f"{dag_id}"),
    )

(
    slack_at_start
    >> mkdir
    >> import_data
    >> extract_geojson
    >> create_and_fix_table
    >> [check_count, check_geo, check_colnames]
    >> rename_table
)
