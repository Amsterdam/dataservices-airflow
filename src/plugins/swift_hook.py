from pathlib import Path
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from swiftclient.service import SwiftService, SwiftError


class SwiftHook(BaseHook):
    def __init__(self, swift_conn_id="swift_default"):
        self.swift_conn_id = swift_conn_id

    def download(self, container, object_id, output_path):

        Path(output_path).parents[0].mkdir(parents=True, exist_ok=True)
        download_options = {
            "out_file": output_path,
        }
        options = None
        if self.swift_conn_id != "swift_default":
            options = {}
            connection = BaseHook.get_connection(self.swift_conn_id)
            options["os_username"] = connection.login
            options["os_password"] = connection.password
            options["os_tenant_name"] = connection.host
        with SwiftService(options=options) as swift:
            try:
                for down_res in swift.download(
                    container=container, objects=[object_id], options=download_options,
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
