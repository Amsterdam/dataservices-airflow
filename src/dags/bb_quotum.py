from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator
from swift_operator import SwiftOperator
from postgres_files_operator import PostgresFilesOperator
from common import (
    default_args,
    slack_webhook_token,
    MessageOperator,
    DATAPUNT_ENVIRONMENT,
)
from common.sql import (
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
)


SQL_RENAME_COL = """
ALTER TABLE asbest_daken_new RENAME COLUMN identifica TO pandidentificatie
"""


SQL_DROPS = """
    DROP TABLE IF EXISTS {{ params.tablename }}_new CASCADE;
    DROP SEQUENCE IF EXISTS {{ params.tablename }}_new_id_seq CASCADE;
    DROP INDEX IF EXISTS {{ params.tablename }}_new_geo_id;
"""

SQL_TABLE_RENAME = """
    ALTER TABLE IF EXISTS bb_quotum RENAME TO bb_quotum_old;
    ALTER SEQUENCE IF EXISTS bb_quotum_id_seq RENAME TO bb_quotum_old_id_seq;
    ALTER TABLE bb_quotum_new RENAME TO bb_quotum;
    ALTER SEQUENCE bb_quotum_new_id_seq RENAME TO bb_quotum_id_seq;
    DROP TABLE IF EXISTS bb_quotum_old;
    DROP SEQUENCE IF EXISTS bb_quotum_old_id_seq CASCADE;
    ALTER INDEX bb_quotum_new_pkey RENAME TO bb_quotum_pkey;
    ALTER INDEX bb_quotum_new_geo_id RENAME TO bb_quotum_geo_id;
"""

SQL_CHECK_COLS = """
    SELECT COUNT(column_name) FROM information_schema.columns WHERE
     table_schema = 'public' AND table_name = 'bb_quotum_new'
     AND column_name in ('wijk', 'availability_color', 'geo')
"""

dag_id = "bed_and_breakfast"
dag_config = Variable.get(dag_id, deserialize_json=True)

# We are using a file-based sql template, so we need to extend template_searchpath
# to enable Jinja2 to find sql files in /tmp/..

with DAG(dag_id, default_args=default_args, template_searchpath=["/"]) as dag:

    sql_file_base = dag_config["sql_file_base"]
    sql_file_new_base = f"{sql_file_base}_new"
    sql_file = f"{sql_file_base}.sql"
    sql_file_new = f"{sql_file_new_base}.sql"
    tmp_dir = f"/tmp/{dag_id}"
    sql_file_path = f"{tmp_dir}/{DATAPUNT_ENVIRONMENT}/{sql_file}"
    sql_file_new_path = f"{tmp_dir}/{sql_file_new}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    fetch_sql = SwiftOperator(
        task_id="fetch_sql",
        container=dag_id,
        object_id=f"{DATAPUNT_ENVIRONMENT}/{sql_file}",
        output_path=sql_file_path,
    )

    remove_owner_alters = BashOperator(
        task_id="remove_owner_alters",
        bash_command=f'egrep -v "^ALTER TABLE.*OWNER TO" {sql_file_path} '
        f'| egrep -v "^GRANT SELECT ON" > "{sql_file_new_path}"',
    )

    replace_tablename = BashOperator(
        task_id="replace_tablename",
        bash_command=f'perl -pi -e "s/quota_bbkaartlaagexport/bb_quotum_new/g" '
        f"{sql_file_new_path}",
    )

    drop_tables = PostgresOperator(
        task_id="drop_tables", sql=SQL_DROPS, params=dict(tablename=dag_id),
    )

    import_table = PostgresFilesOperator(
        task_id="import_table", sql_files=[sql_file_new_path],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename="bb_quotum_new", mincount=90),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(
            tablename="bb_quotum_new", geotype="ST_MultiPolygon", geo_column="geo"
        ),
    )

    check_cols = PostgresValueCheckOperator(
        task_id="check_cols", sql=SQL_CHECK_COLS, pass_value=[[3]]
    )

    rename_tables = PostgresOperator(task_id="rename_tables", sql=SQL_TABLE_RENAME)

(
    slack_at_start
    >> fetch_sql
    >> remove_owner_alters
    >> replace_tablename
    >> drop_tables
    >> import_table
    >> [check_count, check_geo, check_cols]
    >> rename_tables
)
