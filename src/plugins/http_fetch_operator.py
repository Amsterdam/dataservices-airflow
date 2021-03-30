import os
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, Union

from airflow.exceptions import AirflowFailException
from airflow.hooks.http_hook import HttpHook
from airflow.models import XCOM_RETURN_KEY
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpFetchOperator(BaseOperator):
    """Operator for fetching large amounts of data

    The regular SimpleHttpOperator of Airflow is not convenient for this as it does not persist the
    results.
    """

    template_fields = [
        "endpoint",
        "data",
        "headers",
    ]

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        endpoint: str,
        tmp_file: Union[Path, str],
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        http_conn_id: str = "http_default",
        encoding_schema: Optional[str] = None,
        output_type: Optional[str] = None,
        xcom_tmp_dir_task_ids: Optional[str] = None,
        xcom_tmp_dir_key: str = XCOM_RETURN_KEY,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get an HTTP resource and store it in a (temporary) file.

        Args:
            endpoint: The URL path identifying the resource
            data:
            headers:
            http_conn_id: ID that refers to the base URL of the resource
            encoding_schema:
            tmp_file: File to store the resource in. This is either a full path or just a filename.
                In case of the latter we expect the directory to be communicated to use by
                means of XCom. See also parameter ``xcom_tmp_dir_task_ids``.
            output_type:
            xcom_tmp_dir_task_ids: The task_ids (yes, plural even though we refer to a single
                task, but this is how Airflow does it) of the task form which we expect the path
                of a tmp dir. When this parameter is used, the parameter ``tmp_file`` is expected
                to be a simple file name and not a full path.
            xcom_tmp_dir_key: The key to pull the tmp dir path out of XCom. By default we assume
                this to be `"return_value"`.
            *args:
            **kwargs:
        """
        self.endpoint = endpoint
        self.headers = headers or {}
        self.http_conn_id = http_conn_id
        self.encoding_schema = encoding_schema
        self.data = data or {}
        self.tmp_file = tmp_file  # or make temp file + store path in xcom
        self.output_type = output_type  # default is raw, else specify text i.e.
        self.xcom_tmp_dir_task_ids = xcom_tmp_dir_task_ids
        self.xcom_tmp_dir_key = xcom_tmp_dir_key

        super().__init__(*args, **kwargs)

    def execute(self, context: Dict) -> None:  # noqa
        if self.xcom_tmp_dir_task_ids is None and self.xcom_tmp_dir_key != XCOM_RETURN_KEY:
            raise AirflowFailException(
                "Parameter `xcom_tmp_dir_key` was set without setting parameter "
                "'xcom_tmp_dir_task_ids`. If you want to use XCOM, you should at least set "
                "`xcom_tmp_dir_task_ids`."
            )

        tmp_file: Path = Path(self.tmp_file)

        # ----- TEMPORARY IF REMOVE IT AFTER precariobelasting HAS DATA ON OBJECTSTORE PRD -----
        if self.output_type == "file":
            self.log.info("Output type: 'file'. Current working directory '%s'", os.getcwd())
            with open(self.endpoint) as source:
                data = source.read()

            with open(tmp_file, "wt") as wf:
                wf.write(data)
            return
        # ----- TEMPORARY IF REMOVE IT AFTER precariobelasting HAS DATA ON OBJECTSTORE PRD -----

        if self.xcom_tmp_dir_task_ids is not None:
            self.log.debug(
                "Retrieving tmp dir from XCom: task_ids='%s', key='%s'.",
                self.xcom_tmp_dir_task_ids,
                self.xcom_tmp_dir_key,
            )
            assert len(tmp_file.parts) == 1, "Expected a file name, not a complete path."
            tmp_dir = Path(
                context["ti"].xcom_pull(
                    task_ids=self.xcom_tmp_dir_task_ids, key=self.xcom_tmp_dir_key
                )
            )
            self.log.info("Tmp dir: '%s'.", tmp_dir)
            tmp_file = tmp_dir / tmp_file
        else:
            tmp_file.parents[0].mkdir(parents=True, exist_ok=True)

        self.log.info("Tmp file (for storing HTTP request result): '%s'.", tmp_file)

        http = HttpHook(http_conn_id=self.http_conn_id, method="GET")

        self.log.info("Calling HTTP GET method on endpoint: '%s'.", self.endpoint)
        extra_options: dict = {"stream": True}

        # Part of :fire: fix related to CA certificate usage.
        #  Use correct certificatesm, because airflow HttpHook ignores them.
        if "CURL_CA_BUNDLE" in os.environ:
            extra_options["verify"] = os.environ.get("CURL_CA_BUNDLE")
        if "REQUESTS_CA_BUNDLE" in os.environ:
            extra_options["verify"] = os.environ.get("REQUESTS_CA_BUNDLE")

        # Temporary workarround to cope with unvalid SSL for maps.amsterdam.nl
        # It prevents Airflow to load the data (invalid SSL certificate)
        # TODO: check with maintainer maps.amsterdam.nl when SSL certificate is
        # valid again then remove this workarround
        if "maps.amsterdam.nl" in http.get_connection(self.http_conn_id).host:
            extra_options["verify"] = False

        response = http.run(self.endpoint, self.data, self.headers, extra_options=extra_options)
        # set encoding schema explicitly if given
        if self.encoding_schema:
            response.encoding = self.encoding_schema
        self.log.info("Encoding schema: '%s'", response.encoding)

        # When content is encoded (gzip etc.) we need this
        # response.raw.read = functools.partial(response.raw.read, decode_content=True)
        if self.output_type == "text":
            with open(tmp_file, "wt") as tf:
                tf.write(response.text)
        else:
            with open(tmp_file, "wb") as bf:
                shutil.copyfileobj(response.raw, bf)
