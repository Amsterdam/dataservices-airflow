from datetime import timedelta
from airflow import DAG
from swift_operator import SwiftOperator

default_args = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": "2020-03-18",
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": False,  # do not backfill
}

with DAG("testdag", default_args=default_args,) as dag:

    swift_task = SwiftOperator(
        task_id="swift_task",
        container="afval",
        object_id="acceptance/afval_cluster.zip",
        output_path="/tmp/blaat/out.zip",
    )
