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
import_step = 10000

DST_AMSTERDAM_DATES = {
    "2019": {"start": "2019-03-31 02:00:00+1", "end": "2019-10-27 03:00:00+2"},
    "2020": {"start": "2019-03-29 02:00:00+1", "end": "2019-10-25 03:00:00+2"},
    "2021": {"start": "2019-03-28 02:00:00+1", "end": "2019-10-31 03:00:00+2"},
    "2022": {"start": "2019-03-27 02:00:00+1", "end": "2019-10-30 03:00:00+2"},
    "2023": {"start": "2019-03-26 02:00:00+1", "end": "2019-10-29 03:00:00+2"},
}

SQL_CREATE_TEMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.base_table }}_temp CASCADE;
    CREATE TABLE {{ params.base_table }}_temp (
      id integer PRIMARY KEY,
      sensor character varying,
      naam_locatie character varying,
      periode character varying,
      datum_uur timestamp with time zone,
      aantal_passanten integer
    );
    CREATE SEQUENCE {{ params.base_table }}_temp_id_seq
        OWNED BY {{ params.base_table }}_temp.id;
    ALTER TABLE {{ params.base_table }}_temp
        ALTER COLUMN id SET DEFAULT
        NEXTVAL('{{ params.base_table }}_temp_id_seq');
"""


SQL_RENAME_TEMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.base_table }}_old CASCADE;
    ALTER TABLE IF EXISTS {{ params.base_table }}
        RENAME TO {{ params.base_table }}_old;
    ALTER TABLE {{ params.base_table }}_temp
        RENAME TO {{ params.base_table }};
    ALTER SEQUENCE {{ params.base_table }}_temp_id_seq
        RENAME TO {{ params.base_table }}_id_seq;
   ALTER INDEX IF EXISTS {{ params.base_table }}_periode_idx RENAME TO {{ params.base_table }}_old_periode_idx;
   CREATE INDEX {{ params.base_table }}_periode_idx ON {{ params.base_table }}(periode);
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
        count = 0
        cursor = waarnemingen_connection.execute(
            f"""
SELECT sensor, location_name, datum_uur, aantal_passanten 
FROM {view_name}
WHERE location_name IS NOT NULL"""
        )
        while True:
            fetch_iterator = cursor.fetchmany(size=import_step)
            batch_count = copy_data_in_batch(fetch_iterator)
            count += batch_count
            if batch_count < import_step:
                break
        print(f"Imported: {count}")
        day_count = add_aggregates("dag")
        print(f"Imported day aggregates: {day_count}")
        week_count = add_aggregates("week")
        print(f"Imported week aggregates: {week_count}")


def add_aggregates(periode):
    masterdb_hook = PostgresHook()
    conn = masterdb_hook.get_conn()
    trunc_map = {"dag": "day", "week": "week"}

    timezone_sql = "SET TIMEZONE='Europe/Amsterdam'"
    # We still have to correct for daylight saving time
    masterdb_hook.run(timezone_sql)
    query = f"""
    SELECT sensor
         , naam_locatie
         , date_trunc(%s, datum_uur) AS datum_uur
         , SUM(aantal_passanten) AS aantal_passanten
         FROM {table_id}_temp
         GROUP BY sensor, naam_locatie, date_trunc(%s, datum_uur);
        """
    cursor = conn.cursor()
    cursor.execute(query, (trunc_map[periode], trunc_map[periode]))
    fetch_iterator = cursor.fetchall()
    return copy_data_in_batch(fetch_iterator, periode=periode)


def copy_data_in_batch(fetch_iterator, periode="uur"):
    items = []
    for row in fetch_iterator:
        items.append(
            "('{sensor}', "
            "'{location_name}',"
            "'{periode}',"
            "TIMESTAMP '{datum_uur}', "
            "{aantal_passanten})".format(
                sensor=row[0],
                location_name=row[1],
                periode=periode,
                datum_uur=row[2].strftime("%Y-%m-%d %H:%M:%S"),
                aantal_passanten=int(row[3]),
            )
        )
    result = len(items)
    if result:
        items_sql = ",".join(items)
        insert_sql = (
            f"INSERT INTO {table_id}_temp "
            "(sensor, naam_locatie, periode, datum_uur, aantal_passanten) "
            f"VALUES {items_sql};"
        )

        masterdb_hook = PostgresHook()
        try:
            masterdb_hook.run(insert_sql)
        except Exception as e:
            raise Exception("Failed to insert batch data: {}".format(str(e)[0:150]))
        else:
            print("Created {} records.".format(result))
    return result


args = default_args.copy()
args["provide_context"] = True

with DAG(dag_id, default_args=args, description="Crowd Monitor",) as dag:
    create_temp_tables = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

    copy_data = PythonOperator(
        task_id="copy_data",
        python_callable=copy_data_from_dbwaarnemingen_to_masterdb,
        dag=dag,
    )

    rename_temp_tables = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

    create_temp_tables >> copy_data >> rename_temp_tables
