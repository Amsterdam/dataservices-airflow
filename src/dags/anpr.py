#!/usr/bin/env python3
import csv
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from http_fetch_operator import HttpFetchOperator

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

DATASTORE_TYPE = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)


dag_id = "anpr"
table_id = "anpr_taxi"
http_conn_id = "taxi_waarnemingen_conn_id" if DATASTORE_TYPE != 'acceptance' else "taxi_waarnemingen_acc_conn_id"
endpoint = "/v0/milieuzone/passage/export-taxi/"
TMP_PATH = "/tmp/{dag_id}/"

args = default_args.copy()
args["provide_context"] = True

SQL_CREATE_TEMP_TABLE = """
    CREATE TABLE IF NOT EXISTS {{ params.base_table }} (
	    ID serial NOT NULL,
	    DATUM date NULL,
	    AANTAL_TAXI_PASSAGES int4 NULL,
	    CONSTRAINT ANPR_TAXI_PK PRIMARY KEY (id));    
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


def import_csv_data(*args, **kwargs):
    sql_header = f"INSERT INTO {table_id}_temp (datum, aantal_taxi_passages) VALUES "
    with open(f"{TMP_PATH}/taxi_passages.csv") as csvfile:
        reader = csv.DictReader(csvfile)
        items = []
        for row in reader:
            
            items.append(
                "{sql_header} ('{date}', '{aantal_taxi_passages}' )".format(
                    sql_header=sql_header, date=row["datum"], aantal_taxi_passages=row["aantal_taxi_passages"]
                )
            )

        if len(items):            
            hook = PostgresHook()
            sql = " {items};".format(items=";".join(items))            
            try:
                hook.run(sql)
            except Exception as e:
                raise Exception("Failed to create data: {}".format(str(e)[0:150]))
            print("Created {} recods".format(len(items)))


with DAG(dag_id, default_args=args, description="aantal taxikentekenplaten per datum",) as dag:

    # 1. starting message on Slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )
    
    # 2. make temp dir
    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {TMP_PATH}")
    
    # 3. download the data into temp directory
    download_data = HttpFetchOperator(
        task_id="download",
        endpoint=f"{endpoint}",
        http_conn_id=f"{http_conn_id}",
        tmp_file=f"{TMP_PATH}/taxi_passages.csv",
        output_type="text",
    )

    create_temp_table = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

    import_data = PythonOperator(
        task_id="import_data", python_callable=import_csv_data, dag=dag,
    )

    rename_temp_table = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLE,
        params=dict(base_table=table_id),
    )

(slack_at_start >> mk_tmp_dir >> download_data >> create_temp_table >> import_data >> rename_temp_table)