from contextlib import contextmanager
from pathlib import Path
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject


class SwiftHook(BaseHook):
    """A Swift hook to interact with the objectstore.
    If no swift_conn_id is provided, the default
    connection is used. The default connection
    is defined in OS_USERNAME, OS_PASSWORD, OS_TENANT_NAME
    and OS_AUTH_URL, as requested by the SwiftService
    """

    def __init__(self, swift_conn_id="swift_default"):
        self.swift_conn_id = swift_conn_id

    @contextmanager
    def connection(self):
        options = None
        if self.swift_conn_id != "swift_default":
            options = {}
            connection = BaseHook.get_connection(self.swift_conn_id)
            options["os_username"] = connection.login
            options["os_password"] = connection.password
            options["os_tenant_name"] = connection.host
        yield SwiftService(options=options)

    def list_container(self, container):
        with self.connection() as swift:
            try:
                for page in swift.list(container=container):
                    if page["success"]:
                        for item in page["listing"]:
                            yield item
            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException(f"Failed to fetch container listing: {e.value}")

    def download(self, container, object_id, output_path):

        Path(output_path).parents[0].mkdir(parents=True, exist_ok=True)
        download_options = {
            "out_file": output_path,
        }
        with self.connection() as swift:
            try:
                for down_res in swift.download(
                    container=container,
                    objects=[object_id],
                    options=download_options,
                ):
                    if down_res["success"]:
                        self.log.info("downloaded: %s", down_res["object"])
                    else:
                        self.log.error("download failed: %s", down_res["object"])
                        raise AirflowException(f"Failed to fetch file: {down_res['object']}")

            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException(f"Failed to fetch file: {e.value}")

    def upload(self, container, local_file, object_id):
        with self.connection() as swift:
            try:
                for r in swift.upload(
                    container, [SwiftUploadObject(local_file, object_name=object_id)]
                ):
                    if r["success"]:
                        if "object" in r:
                            self.log.info(f"uploaded: {r['object']}")
                        elif "for_object" in r:
                            self.log.info(f"{r['for_object']} segment {r['segment_index']}")
                    else:
                        error = r["error"]
                        if r["action"] == "create_container":
                            self.log.warning(
                                "Warning: failed to create container " "'%s'%s", container, error
                            )
                        elif r["action"] == "upload_object":
                            self.log.error(
                                "Failed to upload object %s to container %s: %s"
                                % (container, r["object"], error)
                            )
                        else:
                            self.log.error("%s" % error)
                        raise AirflowException(f"Failed to upload file: {r['object']} ({error})")

            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException(f"Failed to upload file: {e.value}")
