import inspect
from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults


class LogMessageOperator(BaseOperator):
    """Log messages to default logger.

    To proxy SlackWebhookOperator for local development.
    """

    @apply_defaults  # type: ignore [misc]
    def __init__(self, *, message: str = "", channel: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize LogMessageOperator."""
        self.message = message
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
        self.log.info("%s for channel %s", self.message, self.channel)
