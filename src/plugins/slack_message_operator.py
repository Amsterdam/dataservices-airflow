import logging
import os
from typing import Final

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import Context
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from requests.exceptions import ConnectionError

# Get creds and channel for Slack posts
SLACK_CHANNEL: Final = os.environ.get("SLACK_CHANNEL")
SLACK_TOKEN: Final = os.environ.get("SLACK_WEBHOOK")
SLACK_ICON_START: Final = ":arrow_forward:"  # Emoij indicator for starting DAG
SLACK_ICON_FAIL: Final = ":red_circle:"  # Emoij indicator for failing DAG

# define logger for output to console
logger: logging.Logger = logging.getLogger(__name__)


class SlackMessageOperator(SlackAPIPostOperator):
    """Class definition for sending Slack message.

    :env var SLACK_CHANNEL: Name of Slack channel to post to.
        If you want to send it to a "user" -> use "@user",
        if "public channel" -> use "#channel",
        if "private channel" -> use "channel".

        NOTE: To post a message in a Slack channel, you need to get a
        webhook for each channel. And the web-app needs to be added to
        that channel.

    :env var SLACK_WEBHOOK: The web-app Slack token. This one is
        defined as environment variable and present in the Airflow
        instance by default. Only one of slack_conn_id and token is required.
        Here we use only token.
    """

    def execute(self, context: Context) -> None:
        """Sends start DAG message to the defined Slack channel.

        Args:
            context: parameters in dict format

        executes:
            SlackHook instance
        """
        # to prevent circular import error, this import is put inside
        # this method.
        from common import OTAP_ENVIRONMENT

        message = "{0} DAG: *{2}* started (*{1}*)".format(
            SLACK_ICON_START, OTAP_ENVIRONMENT, context.get("task_instance").dag_id
        )

        slack_hook = SlackHook(token=SLACK_TOKEN)

        try:
            slack_hook.call("chat.postMessage", json={"channel": SLACK_CHANNEL, "text": message})

        except (AirflowException, ConnectionError):
            logger.error("Unable to reach Slack!! Falling back to logger.")
            logger.error(message)

    def slack_failed_task(self, context: Context) -> None:
        """Sends failure DAG message to the defined Slack channel.

        This function is used in the DAG's `on_failure_callback` parameter.

        Args:
            context: parameters in dict format

        executes:
            SlackHook instance
        """
        # to prevent circular import error, this import is put inside
        # this method.
        from common import OTAP_ENVIRONMENT

        failed_message = """
                    {0} DAG: *{3}* failed (*{1}*)
                    *Task*: {2}
                    *Dag*: {3}
                    *Execution Time*: {4}
                    *Log Url*: {5}
                    """.format(
            SLACK_ICON_FAIL,
            OTAP_ENVIRONMENT,
            context.get("task_instance").task_id,
            context.get("task_instance").dag_id,
            context.get("logical_date"),
            context.get("task_instance").log_url,
        )

        slack_hook = SlackHook(token=SLACK_TOKEN)

        try:
            slack_hook.call(
                "chat.postMessage", json={"channel": SLACK_CHANNEL, "text": failed_message}
            )

        except (AirflowException, ConnectionError):
            logger.error("Unable to reach Slack!! Falling back to logger.")
            logger.error(failed_message)
