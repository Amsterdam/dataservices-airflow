#!/usr/bin/env python3
import csv
import logging
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    DATASTORE_TYPE,
    SHARED_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from postgres_permissions_operator import PostgresPermissionsOperator

logger = logging.getLogger(__name__)

DAG_ID = "anpr"
table_id = "anpr_taxi"
http_conn_id = (
    "taxi_waarnemingen_conn_id"
    if DATASTORE_TYPE != "acceptance"
    else "taxi_waarnemingen_acc_conn_id"
)
endpoint = "/v0/milieuzone/passage/export-taxi/"
TMP_DIR = Path(SHARED_DIR) / DAG_ID


args = default_args.copy()

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


def import_csv_data(**_: Any) -> None:
    """Insert rows for CSV file into DB table."""
    sql_header = f"INSERT INTO {table_id}_temp (datum, aantal_taxi_passages) VALUES "
    with open(TMP_DIR / "taxi_passages.csv") as csvfile:
        reader = csv.DictReader(csvfile)
        items = []
        for row in reader:
            items.append(
                "('{date}', {aantal_taxi_passages})".format(
                    date=row["datum"], aantal_taxi_passages=row["aantal_taxi_passages"]
                )
            )
        if len(items):
            hook = PostgresHook()
            sql = "{header} {items};".format(header=sql_header, items=",".join(items))
            try:
                hook.run(sql)
            except Exception as e:
                raise Exception(f"Failed to create data: {e}"[:150])
            logger.debug("Created %d records.", len(items))


with DAG(
    DAG_ID,
    default_args=args,
    description="aantal geidentificeerde taxikentekenplaten per dag",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. starting message on Slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. make temp dir
    mk_tmp_dir = mk_dir(TMP_DIR, clean_if_exists=True)

    # 3. download the data into temp directory
    download_data = HttpFetchOperator(
        task_id="download",
        endpoint=endpoint,
        http_conn_id=http_conn_id,
        tmp_file=TMP_DIR / "taxi_passages.csv",
        output_type="text",
    )

    create_temp_table = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLE,
        params={"base_table": table_id},
    )

    import_data = PythonOperator(
        task_id="import_data",
        python_callable=import_csv_data,
        dag=dag,
    )

    rename_temp_table = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLE,
        params={"base_table": table_id},
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)


(
    slack_at_start
    >> mk_tmp_dir
    >> download_data
    >> create_temp_table
    >> import_data
    >> rename_temp_table
    >> grant_db_permissions
)
