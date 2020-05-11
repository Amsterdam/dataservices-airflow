from pathlib import Path
from swiftclient.service import SwiftService, SwiftError
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook


class SwiftOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        container: str,
        object_id: str,
        output_path: str,
        conn_id: str = None,
        *args,
        **kwargs,
    ) -> None:
        self.container = container
        self.object_id = object_id
        self.output_path = output_path
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context):
        Path(self.output_path).parents[0].mkdir(parents=True, exist_ok=True)
        download_options = {
            "out_file": self.output_path,
        }
        options = None
        if self.conn_id is not None:
            options = {}
            connection = BaseHook.get_connection(self.conn_id)
            options["os_username"] = connection.login
            options["os_password"] = connection.password
            options["os_tenant_name"] = connection.host
        with SwiftService(options=options) as swift:
            try:
                for down_res in swift.download(
                    container=self.container,
                    objects=[self.object_id],
                    options=download_options,
                ):
                    if down_res["success"]:
                        self.log.info("downloaded: %s", down_res["object"])
                    else:
                        self.log.error("download failed: %s", down_res["object"])
                        raise AirflowException(
                            f"Failed to fetch file: {down_res['object']}"
                        )

            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException(f"Failed to fetch file: {e.value}")
