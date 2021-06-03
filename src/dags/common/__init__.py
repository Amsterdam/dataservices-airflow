import logging
import os
import traceback
from datetime import timedelta
from hashlib import blake2s
from inspect import cleandoc
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, cast

import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import Context
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from environs import Env
from log_message_operator import LogMessageOperator
from requests.exceptions import ConnectionError

env: Env = Env()

# define logger for output to console
logger: logging.Logger = logging.getLogger(__name__)

slack_webhook_token: str = env("SLACK_WEBHOOK")
DATAPUNT_ENVIRONMENT: str = env("DATAPUNT_ENVIRONMENT", "acceptance")

# Defines the environments in which sending of email is enabled.
ELIGIBLE_EMAIL_ENVIRONMENTS: Tuple[str, ...] = tuple(
    cast(
        Iterable[str],
        map(lambda s: s.strip(), env.list("ELIGIBLE_EMAIL_ENVIRONMENTS", ["production"])),
    )
)

SHARED_DIR: str = env("SHARED_DIR", "/tmp")  # noqa: S108


class MonkeyPatchedSlackWebhookHook(SlackWebhookHook):
    """THIS IS TEMPORARY PATCH. IF YOU SEE THIS AFTER MARCH 21 2021 PLEASE POKE NICK.

    Patching default SlackWebhookHook in order to set correct Verify option,
    needed for production use.
    """

    def run(  # noqa: D102 (it's temporary after all)
        self,
        endpoint: Optional[str],
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        **request_kwargs: Any,
    ) -> None:
        if extra_options and "verify" not in extra_options:
            if "CURL_CA_BUNDLE" in os.environ:
                extra_options["verify"] = os.environ.get("CURL_CA_BUNDLE")
            if "REQUESTS_CA_BUNDLE" in os.environ:
                extra_options["verify"] = os.environ.get("REQUESTS_CA_BUNDLE")

        super().run(endpoint, data, headers, extra_options, **request_kwargs)


class SlackFailsafeWebhookOperator(SlackWebhookOperator):  # noqa: D101 (it's temporary after all)
    def execute(self, context: Context) -> None:  # noqa: D102 (it's temporary after all)
        self.hook = MonkeyPatchedSlackWebhookHook(
            self.http_conn_id,
            self.webhook_token,
            self.message,
            self.attachments,
            self.blocks,
            self.channel,
            self.username,
            self.icon_emoji,
            self.icon_url,
            self.link_names,
            self.proxy,
        )
        try:
            self.hook.execute()
        except (AirflowException, ConnectionError):
            self.log.warning("Unable to reach Slack!! Falling back to logger.")
            self.log.info(self.message)


MessageOperator = (
    LogMessageOperator if DATAPUNT_ENVIRONMENT == "development" else SlackFailsafeWebhookOperator
)


def pg_params(conn_id: str = "postgres_default") -> str:
    """Add "stop on error" argument to connection string.

    Args:
        conn_id: database connection that is provided with default parameters
    returns:
        connection string with default params
    """
    connection_uri = BaseHook.get_connection(conn_id).get_uri().split("?")[0]
    return f"{connection_uri} -X --set ON_ERROR_STOP=1"


def slack_failed_task(context: Context) -> None:
    """Fire off a message to Slack in case of a DAG failure.

    This function defines an ``on_failure_callback` function.

    Args:
        context: parameters in dict format
    executes:
        MessageOperator instance
    """
    dag_id: str = context["dag"].dag_id
    task_id: str = context["task"].task_id
    exception: Optional[BaseException] = context.get("exception")
    if exception is not None:
        formatted_exception = "".join(
            traceback.format_exception(
                etype=type(exception), value=exception, tb=exception.__traceback__
            )
        ).strip()
    message: str = f"""Failure info:
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
    failed_alert.execute(context=context)


default_args: Context = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": pendulum.yesterday("Europe/Amsterdam"),
    "email": "example@airflow.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": slack_failed_task,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,  # do not backfill
}

vsd_default_args: Context = default_args.copy()
vsd_default_args["postgres_conn_id"] = "postgres_default"


def addloopvariables(iterable: Iterable) -> Iterable:
    """Pass through all values from the given iterable.

    Return items of the iterable and booleans
    signalling the first and the last item

    Args:
        iterable:
            iterable that holds values

    Yields:
        current item with indication first of last
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


def quote_string(instr: str) -> str:
    """Needed to put quotes on elements in geotypes for SQL_CHECK_GEO."""
    return f"'{instr}'"


def make_hash(composite_values: List[str], digest_size: int = 5) -> int:
    """Construct a hash value.

    The blake2s algorithm is used to generate a single hased value for source
    composite values that uniquely identify a row.
    In case the source itself doesn't provide a single solid identification as key.

    Args:
        composite_values: a list of record values based on
                        which columns uniquely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Returns:
        a hased value of the composite values in int

    Note:
        The default digest size is for now set to 5 bytes, which is equivalent
        to ~ 10 karakters long.
        Because the id column is preferably of type int, the hased value is converted
        from hex to int.
    """
    return int.from_bytes(
        blake2s("|".join(composite_values).encode(), digest_size=digest_size).digest(),
        byteorder="little",
    )
