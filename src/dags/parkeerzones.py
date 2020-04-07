import pathlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# from airflow.operators.postgres_operator import PostgresOperator
from swift_operator import SwiftOperator
from common import pg_params, default_args, slack_webhook_token


dag_id = "parkeerzones"
dag_config = Variable.get(dag_id, deserialize_json=True)
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"

SQL_RENAME = """
{% for from_col, to_col in params.col_rename.items() %}
  ALTER TABLE {{ params.tablename }} RENAME COLUMN {{ from_col }} TO {{ to_col }};
{% endfor %}
"""

SQL_ADD_COLOR = """
    ALTER TABLE parkeerzones_new ADD COLUMN color VARCHAR(7);
    DROP TABLE IF EXISTS parkeerzones_map_color;
"""

SQL_UPDATE_COLORS = """
    UPDATE parkeerzones_new p SET color = pmc.color
    FROM parkeerzones_map_color pmc WHERE p.ogc_fid = pmc.ogc_fid
      AND p.gebied_code = pmc.gebied_code;
    DROP TABLE parkeerzones_map_color;
"""

SQL_DELETE_UNUSED = """
    DELETE FROM parkeerzones_uitz_new WHERE show = 'FALSE';
    DELETE FROM parkeerzones_new WHERE show = 'FALSE';
"""

SQL_TABLE_RENAMES = """
    BEGIN;
    {% for tablename in params.tablenames %}
      ALTER TABLE IF EXISTS {{ tablename }} RENAME TO {{ tablename }}_old;
      ALTER TABLE {{ tablename }}_new RENAME TO {{ tablename }};
      DROP TABLE IF EXISTS {{ tablename }}_old;
      ALTER INDEX {{ tablename }}_new_pk RENAME TO {{ tablename }}_pk;
      ALTER INDEX {{ tablename }}_new_wkb_geometry_geom_idx
        RENAME TO {{ tablename }}_wkb_geometry_geom_idx;
    {% endfor %}
    COMMIT;
"""


with DAG(dag_id, default_args=default_args,) as dag:

    extract_shps = []
    convert_shps = []
    load_dumps = []
    rename_cols = []
    zip_folder = dag_config["zip_folder"]
    zip_file = dag_config["zip_file"]
    shp_files = dag_config["shp_files"]
    col_renames = dag_config["col_renames"]
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
                sql=SQL_RENAME,
                params=dict(tablename=tablename, col_rename=col_rename),
            )
        )

    # XXX switch to generic sql
    rename_tables = PostgresOperator(
        task_id="rename_tables",
        sql=SQL_TABLE_RENAMES,
        params=dict(tablenames=rename_tablenames),
    )

    add_color = PostgresOperator(task_id="add_color", sql=SQL_ADD_COLOR)
    update_colors = PostgresOperator(task_id="update_colors", sql=SQL_UPDATE_COLORS)

    delete_unused = PostgresOperator(task_id="delete_unused", sql=SQL_DELETE_UNUSED)

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
