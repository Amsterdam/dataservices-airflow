import pathlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator
from swift_operator import SwiftOperator

from common import vsd_default_args, slack_webhook_token

from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
    SQL_CHECK_COLNAMES,
)

PROCESS_TABLE = """
    DELETE FROM overlastgebieden_new WHERE wkb_geometry is null;
    -- insert polygons valid, as duplicate, unpacking them where needed
    INSERT INTO overlastgebieden_new (wkb_geometry, oov_naam, type, url)
    SELECT b.geom wkb_geometry, oov_naam, type, url FROM
    (
        SELECT oov_naam, type, url, (ST_Dump(ST_CollectionExtract(ST_MakeValid(ST_Multi(wkb_geometry)), 3))).geom as geom FROM
        (
            SELECT * FROM overlastgebieden_new WHERE ST_IsValid(wkb_geometry) = false
        ) a
    ) b;
    -- remove invalid polygons (duplicates were inserted in previous statement)
    DELETE FROM overlastgebieden_new WHERE ST_IsValid(wkb_geometry) = false;
"""

dag_id = "overlastgebieden"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data" / dag_id


def checker(records, pass_value):
    found_colnames = set(r[0] for r in records)
    return found_colnames >= set(pass_value)


with DAG(dag_id, default_args=vsd_default_args, template_searchpath=["/"],) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    colnames = [
        "ogc_fid",
        "wkb_geometry",
        "oov_naam",
        "type",
        "url",
    ]
    fetch_shp_files = []

    slack_at_start = SlackWebhookOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id}",
        username="admin",
    )

    for ext in ("dbf", "prj", "shp", "shx"):
        file_name = f"OOV_gebieden_totaal.{ext}"
        fetch_shp_files.append(
            SwiftOperator(
                task_id=f"fetch_shp_{ext}",
                container=dag_id,
                object_id=file_name,
                output_path=f"/tmp/{dag_id}/{file_name}",
            )
        )

    extract_shp = BashOperator(
        task_id="extract_shp",
        bash_command=f"ogr2ogr -f 'PGDump' -t_srs EPSG:28992 -skipfailures -nln {dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {tmp_dir}/OOV_gebieden_totaal.shp",
    )

    convert_shp = BashOperator(
        task_id="convert_shp",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    create_tables = PostgresOperator(
        task_id="create_tables", sql=[f"{tmp_dir}/{dag_id}.utf8.sql", PROCESS_TABLE,],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=110),
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
        params=dict(tablename=f"{dag_id}_new", geotype="ST_Polygon",),
    )

    rename_table = PostgresOperator(
        task_id="rename_table", sql=SQL_TABLE_RENAME, params=dict(tablename=dag_id),
    )

(
    slack_at_start
    >> fetch_shp_files
    >> extract_shp
    >> convert_shp
    >> create_tables
    >> [check_count, check_colnames, check_geo]
    >> rename_table
)


"""
echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py "overlastgebieden/OOV_gebieden_totaal.dbf"
python $SHARED_DIR/utils/get_objectstore_file.py "overlastgebieden/OOV_gebieden_totaal.prj"
python $SHARED_DIR/utils/get_objectstore_file.py "overlastgebieden/OOV_gebieden_totaal.shp"
python $SHARED_DIR/utils/get_objectstore_file.py "overlastgebieden/OOV_gebieden_totaal.shx"

ogr2ogr -f "PGDump" -t_srs EPSG:28992 -skipfailures -nln overlastgebieden_new ${TMPDIR}/overlastgebieden.sql ${TMPDIR}/OOV_gebieden_totaal.shp
iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/overlastgebieden.sql > ${TMPDIR}/overlastgebieden.utf8.sql

echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\\i ${TMPDIR}/overlastgebieden.utf8.sql
-- remove entries without geometry
DELETE FROM overlastgebieden_new WHERE wkb_geometry is null;
-- insert polygons valid, as duplicate, unpacking them where needed
INSERT INTO overlastgebieden_new (wkb_geometry, oov_naam, type, url)
SELECT b.geom wkb_geometry, oov_naam, type, url FROM
(
    SELECT oov_naam, type, url, (ST_Dump(ST_CollectionExtract(ST_MakeValid(ST_Multi(wkb_geometry)), 3))).geom as geom FROM
    (
        SELECT * FROM overlastgebieden_new WHERE ST_IsValid(wkb_geometry) = false
    ) a
) b;
-- remove invalid polygons (duplicates were inserted in previous statement)
DELETE FROM overlastgebieden_new WHERE ST_IsValid(wkb_geometry) = false;
SQL

echo "Check imported data"
${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS overlastgebieden RENAME TO overlastgebieden_old;
ALTER TABLE overlastgebieden_new RENAME TO overlastgebieden;
DROP TABLE IF EXISTS overlastgebieden_old;
ALTER INDEX overlastgebieden_new_pk RENAME TO overlastgebieden_pk;
ALTER INDEX overlastgebieden_new_wkb_geometry_geom_idx RENAME TO overlastgebieden_wkb_geometry_geom_idx;
COMMIT;
"""
