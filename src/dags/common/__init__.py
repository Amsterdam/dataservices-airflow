from datetime import timedelta
from environs import Env
from airflow.utils.dates import days_ago

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from log_message_operator import LogMessageOperator
from airflow.hooks.base_hook import BaseHook

env = Env()

slack_webhook_token = env("SLACK_WEBHOOK")
DATAPUNT_ENVIRONMENT = env("DATAPUNT_ENVIRONMENT", "acceptance")

MessageOperator = (
    LogMessageOperator
    if DATAPUNT_ENVIRONMENT == "development"
    else SlackWebhookOperator
)


def pg_params(conn_id="postgres_default"):
    connection_uri = BaseHook.get_connection(conn_id).get_uri().split("?")[0]
    return f"{connection_uri} -X --set ON_ERROR_STOP=1"

def slack_failed_task(context):
    failed_alert = MessageOperator(
        task_id="failed_alert",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message="Failed task",
        username="admin",
    )
    return failed_alert.execute(context=context)


default_args = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": slack_failed_task,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": False,  # do not backfill
}

vsd_default_args = default_args.copy()
vsd_default_args["postgres_conn_id"] = "postgres_default"
