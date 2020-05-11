from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LogMessageOperator(BaseOperator):
    """
    Operator that logs messages. To proxy SlackWebhookOperator for
    local development.
    """

    @apply_defaults
    def __init__(
        self,
        http_conn_id=None,
        webhook_token=None,
        message="",
        attachments=None,
        blocks=None,
        channel=None,
        username=None,
        icon_emoji=None,
        icon_url=None,
        link_names=False,
        proxy=None,
        *args,
        **kwargs
    ):
        self.message = message
        self.channel = channel
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info("%s for channel %s", self.message, self.channel)
