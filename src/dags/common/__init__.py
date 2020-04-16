from datetime import timedelta
from environs import Env
from airflow.utils.dates import days_ago

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from log_message_operator import LogMessageOperator

env = Env()

slack_webhook_token = env("SLACK_WEBHOOK")
DATAPUNT_ENVIRONMENT = env("DATAPUNT_ENVIRONMENT", "acceptance")

MessageOperator = (
    LogMessageOperator
    if DATAPUNT_ENVIRONMENT == "development"
    else SlackWebhookOperator
)


pg_params = " ".join(
    [
        "-1",
        "-X",
        "--set",
        "ON_ERROR_STOP",
        "-h",
        env("POSTGRES_HOST"),
        "-p",
        env("POSTGRES_PORT"),
        "-U",
        env("POSTGRES_USER"),
    ]
)


def slack_failed_task(context):
    failed_alert = MessageOperator(
        task_id="failed_alert",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Failed task {context}",
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
vsd_default_args["postgres_default"] = "blaat"
