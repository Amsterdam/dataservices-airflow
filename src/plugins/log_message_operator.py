import inspect
from typing import Any

from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from environs import Env

env = Env()
SLACK_CHANNEL: str = env("SLACK_CHANNEL")


class LogMessageOperator(BaseOperator):
    """Log messages to default logger.

    To proxy SlackWebhookOperator for local development.
    """

    def __init__(self, *, channel: str = SLACK_CHANNEL, **kwargs: Any) -> None:
        """Initialize LogMessageOperator.

        Args:
            channel: The name of the slack channel to send message to.
                It defaults to environment variable `SLACK_CHANNEL` value.

        executes:
            Initialize logic while instantiating this class.
        """
        self.channel = channel

        # Only pass along arguments that the BaseOperator super class defines.
        # Reason being that by default BaseOperator does not accept arguments it didn't
        # define. Eg arguments that are specific to the class we are proxying, the
        # SlackWebhookOperator.
        sig = inspect.signature(BaseOperator.__init__)
        params = set(sig.parameters.keys())

        # key views are set-like: https://docs.python.org/3.8/library/stdtypes.html#dict-views
        super_kwargs = {k: kwargs[k] for k in params & kwargs.keys()}
        super().__init__(**super_kwargs)

    def execute(self, context: Context) -> None:
        """Log message to default logger."""
        self.log.info(
            "start DAG %s for channel %s", context.get("task_instance").dag_id, self.channel
        )

    def slack_failed_task(self, context: Context) -> None:
        """Sends failure DAG message to standard out.

        This function is used in the DAG's `on_failure_callback` parameter.

        Args:
            context: parameters in dict format

        executes:
            log output.
        """
        self.log.info(
            "DAG %s failed! If not development, it would send message to channel %s",
            context.get("task_instance").dag_id,
            self.channel,
        )
