from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Union

import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.settings import TIMEZONE
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject


class SwiftHook(BaseHook):
    """A Swift hook to interact with the objectstore.

    If no swift_conn_id is provided, the default
    connection is used. The default connection
    is defined in OS_USERNAME, OS_PASSWORD, OS_TENANT_NAME
    and OS_AUTH_URL, as requested by the SwiftService
    """

    def __init__(self, swift_conn_id: str = "swift_default") -> None:  # noqa: D107
        super().__init__()
        self.swift_conn_id = swift_conn_id

    @contextmanager
    def connection(self) -> Iterator[SwiftService]:
        """Setup the objectstore connection.

        Yields:
            Iterator: An objectstore connection

        """
        options = None
        if self.swift_conn_id != "swift_default":
            options = {}
            connection = BaseHook.get_connection(self.swift_conn_id)
            options["os_username"] = connection.login
            options["os_password"] = connection.password
            options["os_tenant_name"] = connection.host
        yield SwiftService(options=options)

    def list_container(self, container: str) -> Iterator[dict]:
        """Returns the items in the objectstore folder (container).

        Args:
            container: The objectstore folder to retreive the files

        Raises:
            AirflowException: Container cannot be listed

        Yields:
            Iterator: Found file in given objectstor folder (container)

        """
        with self.connection() as swift:
            try:
                for page in swift.list(container=container):
                    if page["success"]:
                        yield from page["listing"]
            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException("Failed to fetch container listing") from e

    def download(self, container: str, object_id: str, output_path: Union[Path, str]) -> None:
        """Downloads a file from the given folder (container).

        Args:
            container: Name of container of the objectstore to download to
            object_id: File to download
            output_path: Path to save to

        Raises:
            AirflowException: Download cannot be executed
        """
        opath = Path(output_path)
        opath.parents[0].mkdir(parents=True, exist_ok=True)
        download_options = {
            "out_file": opath.as_posix(),
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
                raise AirflowException("Failed to fetch file") from e

    def upload(self, container: str, local_path: Union[Path, str], object_id: str) -> None:
        """Upload file to given folder (container).

        Args:
            container: Name of container of the objectstore to download to
            local_path: Path to upload to
            object_id: File to upload

        Raises:
            AirflowException: Upload cannot be executed
        """
        lpath = Path(local_path)
        filename = lpath.name
        with self.connection() as swift:
            try:
                for r in swift.upload(
                    container,
                    [
                        SwiftUploadObject(
                            lpath.as_posix(),
                            object_name=object_id,
                            options={
                                "header": [
                                    f'content-disposition: attachment; filename="{filename}"'
                                ]
                            },
                        )
                    ],
                ):
                    if r["success"]:
                        if "object" in r:
                            self.log.info("uploaded: %r", r["object"])
                        elif "for_object" in r:
                            self.log.info("%r segment %r", r["for_object"], r["segment_index"])
                    else:
                        error = r["error"]
                        if r["action"] == "create_container":
                            self.log.warning(
                                "Warning: failed to create container '%s': %s", container, error
                            )
                        elif r["action"] == "upload_object":
                            self.log.error(
                                "Failed to upload object %s to container %s: %s",
                                container,
                                r["object"],
                                error,
                            )
                        else:
                            self.log.error("%s", error)
                        raise AirflowException(f"Failed to upload file: {r['object']} ({error})")

            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException("Failed to upload file") from e

    def identify_files_not_in_timewindow(
        self, container: str, time_window_in_days: int
    ) -> Iterator[str]:
        """Identifies files in given container that falls out of given time window.

        Args:
            container: name of container that contains the files
            time_window_in_days: Maximum retention
            time specified in days. If set all files, older then current day
            and retention span in days, will be deleted.

        Yields:
            Iterator: name of file that have a modification date that is older then given time
                window
        """
        start_date = pendulum.now(TIMEZONE).subtract(days=time_window_in_days)
        contents_of_container = self.list_container(container)
        for file in contents_of_container:
            modification_date = pendulum.from_format(
                file["last_modified"], "YYYY-MM-DDTHH:mm:ss.SSSSSS"
            ).in_tz(TIMEZONE)
            if modification_date < start_date:
                yield file["name"]

    def delete(
        self, container: str, objects: list, time_window_in_days: Optional[int] = None
    ) -> None:
        """Deletes file(s) from the specified objectstore container.

        This is based on:

        - time window (days of retention span), or if not specified then
        - list of files to delete

        Args:
            container: Name of objectstore container to execute delete
            objects: Files to delete
            time_window_in_days: Maximum retention
            time specified in days. If set all files, older then current day
            and retention span in days, will be deleted. These files are
            added to the objects list (the parameter above). Defaults to None.

        Raises:
            AirflowException: Raising error on issues while deleting files.
        """
        files_to_delete = (
            list(self.identify_files_not_in_timewindow(container, time_window_in_days))
            if time_window_in_days
            else objects
        )

        with self.connection() as swift:
            try:
                if not files_to_delete:
                    self.log.info("No files to delete...")
                    return
                del_iter = swift.delete(container=container, objects=files_to_delete)
                for del_res in del_iter:
                    c = del_res.get("container", "")
                    o = del_res.get("object", "")
                    a = del_res.get("attempts")
                    if del_res["success"] and not del_res["action"] == "bulk_delete":
                        rd = del_res.get("response_dict")
                        if rd is not None:
                            t = dict(rd.get("headers", {}))
                            if t:
                                self.log.info(
                                    "Successfully deleted %s/%s in %d attempts "
                                    "(transaction id: %s)",
                                    c,
                                    o,
                                    a,
                                    t,
                                )
                            else:
                                self.log.info("Successfully deleted %s/%s in %d attempts", c, o, a)
            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException("Failed to delete file(s):", objects) from e
