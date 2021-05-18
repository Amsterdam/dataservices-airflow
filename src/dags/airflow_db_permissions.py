import logging

from airflow import DAG

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)
from postgres_permissions_operator import PostgresPermissionsOperator

dag_id = "airflow_db_permissions"
logger = logging.getLogger(__name__)

with DAG(
    dag_id,
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    catchup=False,
    description="set grants on database objects to database roles based upon schema auth definition, based upon successfully executed dags within specified time window.",
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Add grants (in batch mode)
    # seeks for dags that successfully executed within time window
    # default time window is set on 30 min.
    # beware: currently the logic assumes dag_id == dataset name
    # TODO look for possibility to use other then dag_id as dataset name
    grants = PostgresPermissionsOperator(task_id="grants", batch_ind=True)

# FLOW
slack_at_start >> grants
