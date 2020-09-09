import json
import shutil
from pathlib import Path
import time
from environs import Env
from tempfile import TemporaryDirectory
from typing import Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from schematools.importer.ndjson import NDJSONImporter
from schematools.utils import schema_def_from_url

from http_params_hook import HttpParamsHook

env = Env()
SCHEMA_URL = env("SCHEMA_URL")

OIDC_TOKEN_ENDPOINT = env("OIDC_TOKEN_ENDPOINT")
OIDC_CLIENT_ID = env("OIDC_CLIENT_ID")
OIDC_CLIENT_SECRET = env("OIDC_CLIENT_SECRET")


class HttpGobOperator(BaseOperator):
    """ Operator for fetching data from Gob
    """

    # template_fields = [
    #     "endpoint",
    #     "data",
    #     "headers",
    # ]

    @apply_defaults
    def __init__(
        self,
        endpoint: str,
        dataset: str,
        schema: str,
        *args,
        geojson_field: str = None,
        lowercase: bool = False,
        flatten: bool = False,
        graphql_query_path: str,
        batch_size: int = 2000,
        max_cursor_pos: Optional[int] = None,
        protected: bool = False,
        http_conn_id="http_default",
        **kwargs,
    ) -> None:
        self.dataset = dataset
        self.schema = schema
        self.geojson = geojson_field
        self.graphql_query_path = graphql_query_path
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        self.batch_size = batch_size
        self.max_cursor_pos = max_cursor_pos
        self.protected = protected
        self.db_table_name = f"{self.dataset}_{self.schema}"
        self.token_expires_time = None
        self.access_token = None
        super().__init__(*args, **kwargs)

    def _fetch_params(self):
        params = {
            "condens": "node,edges,id",
            "schema": self.db_table_name,
        }
        for additional_param_name in ("geojson", "lowercase", "flatten"):
            if hasattr(self, additional_param_name):
                params[additional_param_name] = getattr(self, additional_param_name)
        return params

    def _fetch_headers(self):
        headers = {"Content-Type": "application/x-ndjson"}
        if not self.protected:
            return headers
        if self.token_expires_time is None or time.time() - 5 > self.token_expires_time:
            form_params = dict(
                grant_type="client_credentials",
                client_id=OIDC_CLIENT_ID,
                client_secret=OIDC_CLIENT_SECRET,
            )
            http = HttpHook(http_conn_id="oidc_server", method="POST")
            response = http.run(OIDC_TOKEN_ENDPOINT, data=form_params)
            token_info = response.json()
            self.access_token = token_info["access_token"]
            self.token_expires_time = time.time() + token_info["expires_in"]

        headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    def add_batch_params_to_query(self, query, cursor_pos):
        # Simple approach, just string replacement
        # alternative would be to parse query with graphql-core lib, alter AST
        # and render query, seems overkill for now
        return query.replace(
            "active: false",
            f"active: false, first: {self.batch_size}, after: {cursor_pos}",
        )

    def execute(self, context):
        with TemporaryDirectory() as temp_dir:
            tmp_file = Path(temp_dir) / "out.ndjson"
            # self.tmp_file.parents[0].mkdir(parents=True, exist_ok=True)
            http = HttpParamsHook(http_conn_id=self.http_conn_id, method="POST")

            self.log.info("Calling GOB graphql endpoint")

            # we know the schema, can be an input param (schema_def_from_url function)
            # We use the ndjson importer from schematools, give it a tmp tablename
            pg_hook = PostgresHook()
            schema_def = schema_def_from_url(SCHEMA_URL, self.dataset)
            importer = NDJSONImporter(
                schema_def, pg_hook.get_sqlalchemy_engine(), logger=self.log
            )

            importer.generate_tables(
                table_name=self.schema, db_table_name=f"{self.db_table_name}_new",
            )
            # For GOB content, cursor value is exactly the same as
            # the record index. If this were not true, the cursor needed
            # to be obtained from the last content record
            cursor_pos = 0
            while True:
                with self.graphql_query_path.open() as gql_file:
                    # Sometime GOB-API fail with 500 error, caught by Airflow
                    # We retry several times
                    for i in range(3):
                        try:
                            response = http.run(
                                self.endpoint,
                                self._fetch_params(),
                                json.dumps(
                                    dict(
                                        query=self.add_batch_params_to_query(
                                            gql_file.read(), cursor_pos
                                        )
                                    )
                                ),
                                headers=self._fetch_headers(),
                                extra_options={"stream": True},
                            )
                        except AirflowException:
                            self.log.exception("Cannot reach %s", self.endpoint)
                            time.sleep(1)
                        else:
                            break
                    else:
                        raise AirflowException("All retries on GOB-API have failed.")

                # No records returns one newline and a Content-Length header
                # If records are available, there is no Content-Length header
                if int(response.headers.get("Content-Length", "2")) < 2 or (
                    self.max_cursor_pos is not None
                    and cursor_pos >= self.max_cursor_pos
                ):
                    break
                # When content is encoded (gzip etc.) we need this
                # response.raw.read = functools.partial(response.raw.read, decode_content=True)
                # Use a tempfolder
                with tmp_file.open("wb") as wf:
                    shutil.copyfileobj(response.raw, wf)

                last_record = importer.load_file(tmp_file)
                if last_record is None:
                    break
                cursor_pos = last_record["cursor"]
