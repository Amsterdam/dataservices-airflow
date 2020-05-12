from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from swift_hook import SwiftHook


class SwiftOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        container: str,
        object_id: str,
        output_path: str,
        swift_conn_id: str = "swift_default",
        *args,
        **kwargs,
    ) -> None:
        self.container = container
        self.object_id = object_id
        self.output_path = output_path
        self.swift_conn_id = swift_conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context):

        self.log.info(
            "Downloading: %s-%s to %s", self.container, self.object_id, self.output_path
        )
        self.hook = SwiftHook(swift_conn_id=self.swift_conn_id)
        self.hook.download(self.container, self.object_id, self.output_path)
