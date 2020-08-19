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
import_step = 10000

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
    ALTER SEQUENCE IF EXISTS {{ params.base_table }}_id_seq
        RENAME TO {{ params.base_table }}_old_id_seq;
    ALTER TABLE {{ params.base_table }}_temp
        RENAME TO {{ params.base_table }};
    ALTER SEQUENCE {{ params.base_table }}_temp_id_seq
        RENAME TO {{ params.base_table }}_id_seq;
   ALTER INDEX IF EXISTS {{ params.base_table }}_periode_idx RENAME TO {{ params.base_table }}_old_periode_idx;
   CREATE INDEX {{ params.base_table }}_periode_idx ON {{ params.base_table }}(periode);
"""

SQL_ADD_AGGREGATES = """
SET TIME ZONE 'Europe/Amsterdam';
DELETE FROM {{ params.table }} WHERE periode = '{{ params.periode }}';
INSERT into {{ params.table }}(sensor, naam_locatie, periode, datum_uur, aantal_passanten) 
SELECT sensor
     , naam_locatie
	 , '{{ params.periode }}'
     , date_trunc('{{ params.periode_en }}', datum_uur) AS datum_uur
     , SUM(aantal_passanten) AS aantal_passanten
FROM {{ params.table }}
WHERE periode = 'uur'
GROUP BY sensor, naam_locatie, date_trunc('{{ params.periode_en }}', datum_uur);
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
WITH cmsa_1h_v3 AS
 (SELECT v.sensor,
    s.location_name,
    date_trunc('hour'::text, v.timestamp_rounded) AS datum_uur,
    SUM(v.total_count) AS aantal_passanten
   FROM cmsa_15min_view_v3_materialized v
     JOIN peoplemeasurement_sensors s ON s.objectnummer::text = v.sensor::text
  WHERE v.timestamp_rounded > to_date('2019-01-01'::text, 'YYYY-MM-DD'::text)
  GROUP BY v.sensor, s.location_name, (date_trunc('hour'::text, v.timestamp_rounded)))
SELECT sensor, location_name, datum_uur, aantal_passanten
FROM cmsa_1h_v3
"""
        )
        while True:
            fetch_iterator = cursor.fetchmany(size=import_step)
            batch_count = copy_data_in_batch(fetch_iterator)
            count += batch_count
            if batch_count < import_step:
                break
        print(f"Imported: {count}")


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

    add_aggregates_day = PostgresOperator(
        task_id="add_aggregates_day",
        sql=SQL_ADD_AGGREGATES,
        params=dict(table=f"{table_id}_temp", periode="dag", periode_en="day"),
    )

    add_aggregates_week = PostgresOperator(
        task_id="add_aggregates_week",
        sql=SQL_ADD_AGGREGATES,
        params=dict(table=f"{table_id}_temp", periode="week", periode_en="week"),
    )

    rename_temp_tables = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

    create_temp_tables >> copy_data >> add_aggregates_day >> add_aggregates_week >> rename_temp_tables
