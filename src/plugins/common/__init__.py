from datetime import timedelta
from environs import Env
from airflow.utils.dates import days_ago

env = Env()

slack_webhook_token = env("SLACK_WEBHOOK")

default_args = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": False,  # do not backfill
}

pg_params = " ".join(
    [
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
