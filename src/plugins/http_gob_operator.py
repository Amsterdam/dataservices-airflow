import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Final, Optional

from airflow.exceptions import AirflowException
from airflow.models import XCOM_RETURN_KEY, Variable
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.decorators import apply_defaults
from environs import Env
from http_params_hook import HttpParamsHook
from postgres_on_azure_hook import PostgresOnAzureHook
from schematools import TMP_TABLE_POSTFIX
from schematools.importer.ndjson import NDJSONImporter
from schematools.utils import dataset_schema_from_url
from sqlalchemy.exc import SQLAlchemyError
from urllib3.exceptions import ProtocolError

env = Env()
SCHEMA_URL: Final = env("SCHEMA_URL")

OIDC_TOKEN_ENDPOINT: Final = env("OIDC_TOKEN_ENDPOINT")
OIDC_CLIENT_ID: Final = env("OIDC_CLIENT_ID")
OIDC_CLIENT_SECRET: Final = env("OIDC_CLIENT_SECRET")

# value used by GOB to connect relations: [{"bronwaarde": <value>}, {"bronwaarde": <value>}, ...] (json)
# output format: [<value>, <value>]
# https://github.com/Amsterdam/GOB-API/blob/develop/src/gobapi/graphql_streaming/response_custom.py
GOB_SRC_VALUE = "bronwaarde"


class HttpGobOperator(BaseOperator):
    """Operator for fetching data from Gob."""

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        endpoint: str,
        *args: Any,
        geojson_field: Optional[str] = None,
        lowercase: bool = False,
        flatten: bool = False,
        graphql_query_path: Path,
        batch_size: int = 10000,
        max_records: Optional[int] = None,
        protected: bool = False,
        copy_bufsize: int = 16 * 1024 * 1024,
        http_conn_id: str = "http_default",
        is_through_table: bool = False,
        token_expires_margin: int = 5,
        xcom_table_info_task_ids: Optional[str] = None,
        xcom_table_info_key: str = XCOM_RETURN_KEY,
        **kwargs: Any,
    ) -> None:
        """Initialize the Gob Operator.

        Args:
            endpoint: The GOB Graphql endpoint.
            geojson_field: Parameter for the graphql call to indicate the
                field that contains geojson.
            lowercase: Parameter for the graphql call, lowercases the fields in ndjson output.
            flatten: Parameter needed for graphql call.
            graphql_query_path: Path to the actual graphql query.
            batch_size: Number of ndjson records to be returned per graphql call.
            max_records: Parameter, mainly for testing to limit the max number of records to fetch.
            protected: Indicates that a protected endpoint is used, so a token needs to be
                add to the graphl request.
            copy_bufsize: Size of copy buffer to be used by shutil.copyfileobj.
            http_conn_id: The connection id for the GOB graphl server.
            is_through_table: Indicates that the data to be imported is for a through
                table. In that case an extra transform on field names is needed.
            token_expires_margin: Safety margin (in seconds) to be used
                for the OpenID connect server.
            xcom_table_info_task_ids: The id of the task that is providing the xcom info.
            xcom_table_info_key: Key use to grab the xcom info, defaults to the airflow
                default `return_value`.
        """
        self.geojson = geojson_field
        self.graphql_query_path = graphql_query_path
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.batch_size = batch_size
        self.max_records = max_records
        self.protected = protected
        self.copy_bufsize = copy_bufsize
        self.is_through_table = is_through_table
        self.token_expires_margin = token_expires_margin
        self.xcom_table_info_task_ids = xcom_table_info_task_ids
        self.xcom_table_info_key = xcom_table_info_key
        self.token_expires_time: float = time.time()
        self.access_token: Optional[str] = None

        super().__init__(*args, **kwargs)

    def _fetch_params(self, dataset_table_id: str) -> dict[str, str]:
        params = {
            "condens": f"node,edges,id,{GOB_SRC_VALUE}",
            "schema": dataset_table_id,
        }
        if self.geojson is not None:
            params["geojson"] = self.geojson
        return params

    def _fetch_headers(self, force_refresh: bool = False) -> dict[str, str]:
        headers = {"Content-Type": "application/x-ndjson"}
        if not self.protected:
            return headers
        if (
            self.access_token is None
            or time.time() + self.token_expires_margin > self.token_expires_time
            or force_refresh
        ):
            form_params = {
                "grant_type": "client_credentials",
                "client_id": OIDC_CLIENT_ID,
                "client_secret": OIDC_CLIENT_SECRET,
            }
            http = HttpHook(http_conn_id="oidc_server", method="POST")
            for _i in range(3):
                try:
                    response = http.run(OIDC_TOKEN_ENDPOINT, data=form_params)
                except AirflowException:
                    self.log.exception("Keycloak unreachable")
                    time.sleep(1)
                else:
                    break
            else:
                raise
            token_info = response.json()
            self.access_token = token_info["access_token"]
            self.token_expires_time = time.time() + token_info["expires_in"]

        headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    def _add_batch_params_to_query(self, query: str, cursor_pos: int, batch_size: int) -> str:
        # Simple approach, just string replacement
        # alternative would be to parse query with graphql-core lib, alter AST
        # and render query, seems overkill for now
        return query.replace(
            "active: false",
            f"active: false, first: {batch_size}, after: {cursor_pos}",
        )

    def execute(self, context: dict[str, Any]) -> None:  # noqa: D102
        # When doing `airflow tasks test` from cli, there is a self.params attribute availabe.
        # For full dag runs from the scheduler, there is dag_run["conf"]
        from common import SHARED_DIR

        # Fetch the datset info, provided by the first operator
        dataset_info = context["task_instance"].xcom_pull(
            task_ids=self.xcom_table_info_task_ids, key=self.xcom_table_info_key
        )
        dataset_table_id = dataset_info.dataset_table_id
        if not (params := self.params):
            dag_run = context["dag_run"]
            params = {} if dag_run is None else dag_run.conf or {}
        self.log.debug("PARAMS: %s", params)
        max_records = params.get("max_records", self.max_records)
        cursor_pos = int(
            params.get("cursor_pos", Variable.get(f"{dataset_table_id}.cursor_pos", 0))
        )
        batch_size = params.get("batch_size", self.batch_size)
        with TemporaryDirectory(dir=SHARED_DIR) as temp_dir:
            tmp_file = Path(temp_dir) / "out.ndjson"
            http = HttpParamsHook(http_conn_id=self.http_conn_id, method="POST")

            self.log.info("Calling GOB graphql endpoint")

            # we know the schema, can be an input param (dataset_schema_from_url function)
            # We use the ndjson importer from schematools, give it a tmp tablename
            pg_hook = PostgresOnAzureHook(dataset_name=dataset_info.dataset_id, context=context)
            dataset = dataset_schema_from_url(
                dataset_info.schema_url, dataset_info.dataset_id, prefetch_related=True
            )
            importer = NDJSONImporter(dataset, pg_hook.get_sqlalchemy_engine(), logger=self.log)

            importer.generate_db_objects(
                table_id=dataset_info.table_id,
                db_table_name=f"{dataset_info.db_table_name}{TMP_TABLE_POSTFIX}",
                ind_tables=True,
                ind_extra_index=False,
                limit_tables_to={dataset_info.table_id},
            )
            # For GOB content, cursor value is exactly the same as
            # the record index. If this were not true, the cursor needed
            # to be obtained from the last content record
            records_loaded = 0

            with self.graphql_query_path.open() as gql_file:
                query = gql_file.read()

            if "cursor" not in query:
                raise AirflowException("`cursor` field is required in graphql query.")

            # Sometime GOB-API fail with 500 error, caught by Airflow
            # We retry several times
            while True:

                force_refresh_token = False
                for _i in range(3):
                    try:
                        request_start_time = time.time()
                        headers = self._fetch_headers(force_refresh=force_refresh_token)
                        response = http.run(
                            self.endpoint,
                            self._fetch_params(dataset_table_id),
                            json.dumps(
                                {
                                    "query": self._add_batch_params_to_query(
                                        query, cursor_pos, batch_size
                                    )
                                }
                            ),
                            headers=headers,
                            extra_options={"stream": True},
                        )
                    except AirflowException:
                        self.log.exception("Cannot reach %s", self.endpoint)
                        force_refresh_token = True
                        time.sleep(1)
                    else:
                        break
                else:
                    # Save cursor_pos in a variable
                    Variable.set(f"{dataset_table_id}.cursor_pos", cursor_pos)
                    raise AirflowException("All retries on GOB-API have failed.")

                records_loaded += batch_size
                # No records returns one newline and a Content-Length header
                # If records are available, there is no Content-Length header
                if int(response.headers.get("Content-Length", "2")) < 2:
                    break
                # When content is encoded (gzip etc.) we need this:
                # response.raw.read = functools.partial(response.raw.read, decode_content=True)
                try:
                    with tmp_file.open("wb") as wf:
                        shutil.copyfileobj(response.raw, wf, self.copy_bufsize)

                    request_end_time = time.time()
                    self.log.info(
                        "GOB-API request took %s seconds, cursor: %s",
                        request_end_time - request_start_time,
                        cursor_pos,
                    )
                    last_record = importer.load_file(
                        tmp_file, is_through_table=self.is_through_table
                    )
                except (SQLAlchemyError, ProtocolError, UnicodeDecodeError) as e:
                    # Save last imported file for further inspection
                    shutil.copy(
                        tmp_file,
                        f"{SHARED_DIR}/{dataset_table_id}-{datetime.now().isoformat()}.ndjson",
                    )
                    Variable.set(f"{dataset_table_id}.cursor_pos", cursor_pos)
                    self.log.exception("Database error")
                    raise AirflowException("A database error has occurred.") from e

                self.log.info(
                    "Loading db took %s seconds",
                    time.time() - request_end_time,
                )
                if last_record is None or (
                    max_records is not None and records_loaded >= max_records
                ):
                    break
                cursor_pos = last_record["cursor"]

        # On successfull completion, remove cursor_pos variable
        Variable.delete(f"{dataset_table_id}.cursor_pos")
