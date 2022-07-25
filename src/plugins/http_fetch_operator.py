import os
import shutil
from pathlib import Path
from typing import Any, Optional, Union

from airflow.exceptions import AirflowFailException
from airflow.hooks.http_hook import HttpHook
from airflow.models import XCOM_RETURN_KEY
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import Context


class HttpFetchOperator(BaseOperator):
    """Operator for fetching large amounts of data.

    The regular SimpleHttpOperator of Airflow is not convenient for this as it does not persist the
    results.
    """

    template_fields = [
        "endpoint",
        "data",
        "headers",
    ]

    # type: ignore[misc]
    def __init__(
        self,
        endpoint: str,
        tmp_file: Union[Path, str],
        data: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
        http_conn_id: str = "http_default",
        encoding_schema: Optional[str] = None,
        output_type: Optional[str] = None,
        xcom_tmp_dir_task_ids: Optional[str] = None,
        xcom_token_task_ids: Optional[str] = None,
        xcom_key: str = XCOM_RETURN_KEY,
        verify: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get an HTTP resource and store it in a (temporary) file.

        Args:
            endpoint: The URL path identifying the resource
            data: HTTP body form data to be used for the request.
            headers: HTTP header to used for the request.
            http_conn_id: ID that refers to the base URL of the resource
            encoding_schema: The schema used for encoding the data like UTF-8.
            tmp_file: File to store the resource in. This is either a full path or just a filename.
                In case of the latter we expect the directory to be communicated to use by
                means of XCom. See also parameter ``xcom_tmp_dir_task_ids``.
            output_type: Type of output: Text or Bytes.
            xcom_tmp_dir_task_ids: The task_ids (yes, plural even though we refer to a single
                task, but this is how Airflow does it) of the task form which we expect the path
                of a tmp dir. When this parameter is used, the parameter ``tmp_file`` is expected
                to be a simple file name and not a full path.
            xcom_token_task_ids: The task_ids (yes, plural even though we refer to a single
                task, but this is how Airflow does it) of the task form which we expect a API token
                value. When this parameter is used, the HTTP header is set to hold a
                bearer token value.
            xcom_key: The key to pull whatever value out of XCom. By default we assume
                this to be `"return_value"`.
            verify: Indicator if the CA certificate must be verified. Defaults to verify.
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
        self.xcom_key = xcom_key
        self.xcom_token_task_ids = xcom_token_task_ids
        self.verify = verify

        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:  # noqa: C901
        """Main execution function.

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.

        Raises:
            AirflowFailException:
                In case no xcom task_ids was added to the operator call.
        """
        if self.xcom_tmp_dir_task_ids is None and self.xcom_key != XCOM_RETURN_KEY:
            raise AirflowFailException(
                "Parameter `xcom_key` was set without setting parameter "
                "'xcom_tmp_dir_task_ids`. If you want to use XCOM, you should at least set "
                "`xcom_tmp_dir_task_ids`."
            )

        tmp_file: Path = Path(self.tmp_file)

        if self.xcom_tmp_dir_task_ids is not None:
            self.log.debug(
                "Retrieving tmp dir from XCom: task_ids='%s', key='%s'.",
                self.xcom_tmp_dir_task_ids,
                self.xcom_key,
            )
            assert len(tmp_file.parts) == 1, "Expected a file name, not a complete path."
            tmp_dir = Path(
                context["ti"].xcom_pull(task_ids=self.xcom_tmp_dir_task_ids, key=self.xcom_key)
            )
            self.log.info("Tmp dir: '%s'.", tmp_dir)
            tmp_file = tmp_dir / tmp_file
        else:
            tmp_file.parents[0].mkdir(parents=True, exist_ok=True)

        if self.xcom_token_task_ids is not None:
            token = context["ti"].xcom_pull(task_ids=self.xcom_token_task_ids, key=self.xcom_key)
            self.log.info("A API token was defined: '%s'", token)
            self.header = self.headers.update({"Authorization": f"Bearer {token}"})

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

        if not self.verify:
            extra_options["verify"] = False

        response = http.run(self.endpoint, self.data, self.headers, extra_options=extra_options)
        # set encoding schema explicitly if given
        if self.encoding_schema:
            response.encoding = self.encoding_schema
        self.log.info("Encoding schema: '%s'", response.encoding)

        # When content is encoded (gzip etc.) we need this
        # response.raw.read = functools.partial(response.raw.read, decode_content=True)
        if self.output_type == "text":
            # The response is streaming,
            # we take this into account by using the iter_lines method
            # to decrease the memory footprint.
            with open(tmp_file, "wt") as tf:
                for line in response.iter_lines(decode_unicode=True):
                    tf.write(f"{line}\n")
        else:
            with open(tmp_file, "wb") as bf:
                shutil.copyfileobj(response.raw, bf)
