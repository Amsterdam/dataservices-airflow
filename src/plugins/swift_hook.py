import ntpath
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, tzinfo
from dateutil import tz
from pathlib import Path
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject
from typing import Optional, Iterator, Dict

to_zone: Optional[tzinfo] = tz.gettz("Europe/Amsterdam")


class SwiftHook(BaseHook):
    """A Swift hook to interact with the objectstore.
    If no swift_conn_id is provided, the default
    connection is used. The default connection
    is defined in OS_USERNAME, OS_PASSWORD, OS_TENANT_NAME
    and OS_AUTH_URL, as requested by the SwiftService
    """

    def __init__(self, swift_conn_id: str = "swift_default") -> None:
        self.swift_conn_id = swift_conn_id

    @contextmanager
    def connection(self) -> Iterator[SwiftService]:
        """Setup the objectstore connection

        Yields:
            Iterator[SwiftService]:: An objectstore connection

        """
        options = None
        if self.swift_conn_id != "swift_default":
            options = {}
            connection = BaseHook.get_connection(self.swift_conn_id)
            options["os_username"] = connection.login
            options["os_password"] = connection.password
            options["os_tenant_name"] = connection.host
        yield SwiftService(options=options)

    def list_container(self, container: str) -> Iterator[Dict]:
        """Returns the items in the objectstore folder (container)

        Args:
            container (str): The objectstore folder to retreive the files

        Raises:
            AirflowException: Container cannot be listed

        Yields:
            Iterator[str]: Iterator[str]: Found file in given objectstor folder (container)

        """
        with self.connection() as swift:
            try:
                for page in swift.list(container=container):
                    if page["success"]:
                        for item in page["listing"]:
                            yield item
            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException("Failed to fetch container listing") from e

    def download(self, container: str, object_id: str, output_path: str) -> None:
        """Downloads a file from the given folder (container)

        Args:
            container (str): Name of container of the objectstore to download to
            object_id (str): File to download
            output_path (str): Path to save to

        Raises:
            AirflowException: Download cannot be executed
        """
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
                raise AirflowException("Failed to fetch file") from e

    def upload(self, container: str, local_path: str, object_id: str) -> None:  # noqa C901
        """Upload file to given folder (container)

        Args:
            container (str): Name of container of the objectstore to download to
            local_path (str): Path to upload to
            object_id (str): File to upload

        Raises:
            AirflowException: Upload cannot be executed
        """
        filename = ntpath.basename(local_path)
        with self.connection() as swift:
            try:
                for r in swift.upload(
                    container,
                    [
                        SwiftUploadObject(
                            local_path,
                            object_name=object_id,
                            options={
                                "header": [
                                    f'content-disposition: attachment; filename="{filename}"'
                                ]  # noqa
                            },
                        )
                    ],
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
                raise AirflowException("Failed to upload file") from e

    def identify_files_not_in_timewindow(
        self, container: str, time_window_in_days: int
    ) -> Iterator[list]:

        start_date: datetime = datetime.now(timezone.utc).astimezone(to_zone) - timedelta(
            days=time_window_in_days
        )
        contents_of_container = self.list_container(container)
        for file in contents_of_container:
            modification_date = datetime.strptime(
                file["last_modified"], "%Y-%m-%dT%H:%M:%S.%f"
            ).astimezone(to_zone)
            if modification_date < start_date:
                yield file["name"]

    def delete(
        self, container: str, objects: list, time_window_in_days: Optional[int] = None
    ) -> None:
        """Deletes file(s) from the specified objectstore container based on:

        - time window (days of retention span), or if not specified then
        - list of files to delete

        Args:
            container (str): Name of objectstore container to execute delete
            objects (list): Files to delete
            time_window_in_days (Optional[int], optional): Maximum retention
            time specified in days. If set all files, older then current day
            and retention span in days, will be deleted. These files are
            added to the objects list (the parameter above). Defaults to None.

        Raises:
            AirflowException: Raising error on issues while deleting files.
        """

        files_to_delete = (
            [
                file
                for file in self.identify_files_not_in_timewindow(container, time_window_in_days)
            ]
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
                                    "Successfully deleted {0}/{1} in {2} attempts "
                                    "(transaction id: {3})".format(c, o, a, t)
                                )
                            else:
                                self.log.info(
                                    "Successfully deleted {0}/{1} in {2} "
                                    "attempts".format(c, o, a)
                                )
            except SwiftError as e:
                self.log.error(e.value)
                raise AirflowException("Failed to delete file(s):", objects) from e
