from inspect import cleandoc
import pendulum
import re
import traceback

from datetime import timedelta, datetime
from environs import Env
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

from requests.exceptions import ConnectionError


from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from log_message_operator import LogMessageOperator
from airflow.hooks.base_hook import BaseHook

env = Env()

slack_webhook_token = env("SLACK_WEBHOOK")
DATAPUNT_ENVIRONMENT = env("DATAPUNT_ENVIRONMENT", "acceptance")


class SlackFailsafeWebhookOperator(SlackWebhookOperator):
    def execute(self, context):
        try:
            super().execute(context)
        except (AirflowException, ConnectionError):
            self.log.warning("Unable to reach Slack!! Falling back to logger.")
            self.log.info(self.message)


MessageOperator = (
    LogMessageOperator
    if DATAPUNT_ENVIRONMENT == "development"
    else SlackFailsafeWebhookOperator
)


def pg_params(conn_id="postgres_default"):
    connection_uri = BaseHook.get_connection(conn_id).get_uri().split("?")[0]
    return f"{connection_uri} -X --set ON_ERROR_STOP=1"


def slack_failed_task(context):
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    exception = context.get("exception")
    if exception is not None:
        formatted_exception = "".join(
            traceback.format_exception(
                etype=type(exception), value=exception, tb=exception.__traceback__
            )
        ).strip()
    message = f"""Failure info:
        dag_id: {dag_id}
        task_id: {task_id}
    """
    failed_alert = MessageOperator(
        task_id="failed_alert",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"{cleandoc(message)}\n\n{formatted_exception}",
        username="admin",
    )
    return failed_alert.execute(context=context)


# set the local time zone, so the start_date DAG param can use it in its context
# as stated in the Airflow docs, pendulum must be used to set the timezone
amsterdam = pendulum.timezone("Europe/Amsterdam")

# set start_date to 'yesterday', and get the year, month and day as seperate integer values
start_date_dag = str(days_ago(1))
YYYY = 0
MM = 0
DD = 0

# # extract the YYYY MM and DD values as integers
get_YYYY_MM_DD_values = re.search("([0-9]{4})-([0-9]{2})-([0-9]{2})", start_date_dag)
if get_YYYY_MM_DD_values:
    YYYY = int(get_YYYY_MM_DD_values.group(1))
    MM = int(get_YYYY_MM_DD_values.group(2))
    DD = int(get_YYYY_MM_DD_values.group(3))

default_args = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": datetime(YYYY, MM, DD, tzinfo=amsterdam),
    "email": "example@airflow.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": slack_failed_task,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,  # do not backfill
}

vsd_default_args = default_args.copy()
vsd_default_args["postgres_conn_id"] = "postgres_default"


def addloopvariables(iterable):
    """Pass through all values from the given iterable.
    Return items of the iterable and booleans
    signalling the first and the last item
    """
    # Get an iterator and pull the first value.
    it = iter(iterable)
    current = next(it)
    yield current, True, False
    current = next(it)
    # Run the iterator to exhaustion (starting from the third value).
    for val in it:
        # Report the *previous* value (more to come).
        yield current, False, False
        current = val
    # Report the last value.
    yield current, False, True
