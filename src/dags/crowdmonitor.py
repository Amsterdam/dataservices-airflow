#!/usr/bin/env python3

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from common import default_args

dag_id = "crowdmonitor"
table_id = f"{dag_id}_passanten"
view_name = "cmsa_15min_view_v2"
import_step = 300


SQL_CREATE_TEMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.base_table }}_temp;
    CREATE TABLE {{ params.base_table }}_temp
        LIKE {{ params.base_table }} INCLUDING ALL;
    DROP SEQUENCE IF EXISTS {{ params.base_table }}_temp_id_seq CASCADE;
    CREATE SEQUENCE {{ params.base_table }}_temp_id_seq
        OWNED BY {{ params.base_table }}_temp.id;
    ALTER TABLE {{ params.base_table }}_temp
        ALTER COLUMN id SET DEFAULT
        NEXTVAL('{{ params.base_table }}_temp_id_seq');
"""


SQL_RENAME_TEMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.base_table }}_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}
        RENAME TO {{ params.base_table }}_old;
    ALTER TABLE {{ params.base_table }}_temp
        RENAME TO {{ params.base_table }};
"""


def copy_data_from_dbwaarnemingen_to_masterdb():
    waarnemingen_hook = PostgresHook(postgres_conn_id="dbwaarnemingen")
    with waarnemingen_hook.get_cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {view_name} AS total")
        result = cursor.fetchone()
        offset = 0
        while offset < result["total"]:
            copy_data_in_batches(offset=offset, limit=import_step)
            offset += import_step


def copy_data_in_batches(offset, limit):
    waarnemingen_hook = PostgresHook(postgres_conn_id="dbwaarnemingen")
    masterdb_hook = PostgresHook()
    with waarnemingen_hook.get_cursor() as cursor:
        cursor.execute(
            "SELECT sensor, location_name, datum_uur, aantal_passanten "
            f"FROM {view_name} OFFSET {offset} LIMIT {limit}"
        )

        items = []
        for row in cursor.fetchall():
            items.append(
                "({sensor}, "
                "{location_name}, "
                "{datum_uur}, "
                "{aantal_passanten})".format(**row)
            )
        if len(items):
            items_sql = ",".join(items)
            insert_sql = (
                f"INSERT INTO {table_id}_temp "
                "(sensor, location_name, datum_uur, aantal_passanten) "
                f"VALUES {items_sql};"
            )

            try:
                masterdb_hook.run(insert_sql)
            except Exception as e:
                raise Exception("Failed to insert batch data: {}".format(str(e)[0:150]))
            else:
                print("Created {} records.".format(len(items)))


args = default_args.copy()
args["provide_context"] = True

with DAG(dag_id, default_args=args, description="Crowd Monitor",) as dag:
    create_temp_table = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

    copy_data = PythonOperator(
        task_id="copy_data",
        python_callable=copy_data_from_dbwaarnemingen_to_masterdb,
        dag=dag,
    )

    rename_temp_table = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

    create_temp_table >> copy_data >> rename_temp_table
