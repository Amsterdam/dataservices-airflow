import pathlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator
from http_fetch_operator import HttpFetchOperator
from postgres_files_operator import PostgresFilesOperator

from common import (
    vsd_default_args,
    slack_webhook_token,
    MessageOperator,
    DATAPUNT_ENVIRONMENT,
)

from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
    SQL_CHECK_COLNAMES,
)

dag_id = "vezips"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data" / dag_id


def checker(records, pass_value):
    found_colnames = set(r[0] for r in records)
    return found_colnames == set(pass_value)


with DAG(dag_id, default_args=vsd_default_args, template_searchpath=["/"],) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    colnames = [
        "ogc_fid",
        "wkb_geometry",
        "soortcode",
        "vezip_nummer",
        "vezip_type",
        "standplaats",
        "jaar_aanleg",
        "venstertijden",
        "toegangssysteem",
        "camera",
        "beheerorganisatie",
        "bijzonderheden",
    ]

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    fetch_json = HttpFetchOperator(
        task_id="fetch_json",
        endpoint="open_geodata/geojson.php",
        data=dict(KAARTLAAG="VIS_BFA", THEMA="vis"),
        http_conn_id="ams_maps_conn_id",
        tmp_file=f"{tmp_dir}/{dag_id}.json",
    )

    extract_shp = BashOperator(
        task_id="extract_shp",
        bash_command=f"ogr2ogr -f 'PGDump' -t_srs EPSG:28992 -s_srs EPSG:4326 -nln {dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {tmp_dir}/{dag_id}.json",
    )

    drop_table = PostgresOperator(
        task_id="drop_table", sql="DROP TABLE IF EXISTS vezips_new"
    )
    create_table = PostgresFilesOperator(
        task_id="create_table", sql_files=[f"{tmp_dir}/{dag_id}.sql"]
    )

    alter_table = PostgresOperator(
        task_id="alter_table",
        sql=[
            "ALTER TABLE vezips_new RENAME COLUMN bfa_nummer TO vezip_nummer",
            "ALTER TABLE vezips_new RENAME COLUMN bfa_type TO vezip_type",
        ],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=75),
    )

    check_colnames = PostgresValueCheckOperator(
        task_id="check_colnames",
        sql=SQL_CHECK_COLNAMES,
        pass_value=colnames,
        result_checker=checker,
        params=dict(tablename=f"{dag_id}_new"),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(tablename=f"{dag_id}_new", geotype="ST_Point",),
    )

    rename_table = PostgresOperator(
        task_id="rename_table", sql=SQL_TABLE_RENAME, params=dict(tablename=dag_id),
    )

(
    slack_at_start
    >> fetch_json
    >> extract_shp
    >> drop_table
    >> create_table
    >> alter_table
    >> [check_count, check_colnames, check_geo]
    >> rename_table
)
