from typing import Any, Optional

from airflow.models.baseoperator import BaseOperator
from swift_hook import SwiftHook


class SwiftOperator(BaseOperator):
    def __init__(
        self,
        container: str,
        object_id: Optional[str] = None,
        output_path: Optional[str] = None,
        action_type: str = "download",
        objects_to_del: Optional[list] = None,
        time_window_in_days: Optional[int] = None,
        swift_conn_id: str = "swift_default",
        *args: Any,
        **kwargs: dict,
    ) -> None:
        self.container = container
        self.object_id = object_id
        self.output_path = output_path
        self.action_type = action_type
        self.objects_to_del = objects_to_del
        self.time_window_in_days = time_window_in_days
        self.swift_conn_id = swift_conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context: Optional[dict[str, Any]] = None) -> None:
        """Dispatcher to call the SwiftHook methods i.e. download, upload or delete.

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.
        """

        if self.action_type == "download":
            self.log.info(
                "Downloading: %s-%s to %s", self.container, self.object_id, self.output_path
            )
            self.hook = SwiftHook(swift_conn_id=self.swift_conn_id)
            self.hook.download(self.container, self.object_id, self.output_path)

        elif self.action_type == "upload":

            self.log.info(
                "Uploading: %s-%s to %s", self.container, self.object_id, self.output_path
            )
            self.hook = SwiftHook(swift_conn_id=self.swift_conn_id)
            self.hook.upload(
                self.container,
                self.output_path,
                self.object_id,
            )

        elif self.action_type == "delete":

            if self.time_window_in_days is not None:
                self.log.info(
                    "Deleting files from %s that are older then today minus %s days",
                    self.container,
                    self.time_window_in_days,
                )
            elif self.objects_to_del is not None:
                self.log.info(
                    "Deleting %s from %s",
                    self.objects_to_del,
                    self.container,
                )
            else:
                self.log.error(
                    "Nothing to delete...no time window or file given",
                )

            self.hook = SwiftHook(swift_conn_id=self.swift_conn_id)
            self.hook.delete(self.container, self.objects_to_del, self.time_window_in_days)

        else:
            self.log.info("No action specified, do nothing")
