#!/usr/bin/env python3

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from common import env, default_args

dag_id = "crowdmonitor"
table_id = f"{dag_id}_passanten"
view_name = "cmsa_1h_count_view_v1"
import_step = 1000


SQL_CREATE_TEMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.base_table }}_temp;
    CREATE TABLE {{ params.base_table }}_temp (
        LIKE {{ params.base_table }} INCLUDING ALL);
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


def row2dict(row):
    d = {}
    for column in row.__table__.columns:
        d[column.name] = str(getattr(row, column.name))

    return d


def copy_data_from_dbwaarnemingen_to_masterdb(*args, **kwargs):
    try:
        waarnemingen_engine = create_engine(
            env("AIRFLOW_CONN_POSTGRES_DBWAARNEMINGEN").split("?")[0]
        )
    except SQLAlchemyError as e:
        raise Exception(str(e)) from e

    with waarnemingen_engine.connect() as waarnemingen_connection:
        cursor = waarnemingen_connection.execute(
            f"SELECT COUNT(*) FROM {view_name} AS total"
        )
        result = cursor.fetchone()[0]
        print("Found {} records".format(repr(result)))
        offset = 0

        while offset < result:
            copy_data_in_batches(
                conn=waarnemingen_connection, offset=offset, limit=import_step
            )
            offset += import_step


def copy_data_in_batches(conn, offset, limit):
    cursor = conn.execute(
        f"SELECT sensor, location_name, datum_uur, aantal_passanten FROM {view_name} OFFSET {offset} LIMIT {limit}"
    )

    items = []
    for row in cursor.fetchall():
        items.append(
            "('{sensor}', "
            "'{location_name}',"
            "TIMESTAMP '{datum_uur}', "
            "{aantal_passanten})".format(
                sensor=row[0],
                location_name=row[1],
                datum_uur=row[2].strftime("%Y-%m-%d %H:%M:%S"),
                aantal_passanten=int(row[3]),
            )
        )

    if len(items):
        items_sql = ",".join(items)
        insert_sql = (
            f"INSERT INTO {table_id}_temp "
            "(sensor, naam_locatie, datum_uur, aantal_passanten) "
            f"VALUES {items_sql};"
        )

        masterdb_hook = PostgresHook()
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
