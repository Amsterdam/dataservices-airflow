import shutil
from pathlib import Path
from environs import Env
from tempfile import TemporaryDirectory

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from schematools.importer.ndjson import NDJSONImporter
from schematools.utils import schema_def_from_url

from http_params_hook import HttpParamsHook

env = Env()
SCHEMA_URL = env("SCHEMA_URL")


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
        schema: str,
        id_fields: tuple,
        geojson_field: str,
        graphql_query: str,
        http_conn_id="http_default",
        *args,
        **kwargs,
    ) -> None:
        self.schema = (schema,)
        self.id_fields = id_fields
        self.geojson_field = geojson_field
        self.graphql_query = graphql_query
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.http_conn_id = http_conn_id
        super().__init__(*args, **kwargs)

    def _fetch_params(self):
        return {
            "condens": "node,edges,id",
            "lowercase": "true",
            "flatten": "true",
            "id": self.id_fields,
            "schema": self.schema,
            "geojson": self.geojson_field,
        }

    def execute(self, context):
        with TemporaryDirectory() as temp_dir:
            tmp_file = Path(temp_dir) / "out.ndjson"
            # self.tmp_file.parents[0].mkdir(parents=True, exist_ok=True)
            http = HttpParamsHook(http_conn_id=self.http_conn_id, method="POST")

            self.log.info("Calling GOB graphql endpoint")
            response = http.run(
                self.endpoint,
                self._fetch_params(),
                self.graphql_query,
                {"Content-Type": "application/x-ndjson"},
                extra_options={"stream": True},
            )
            # When content is encoded (gzip etc.) we need this
            # response.raw.read = functools.partial(response.raw.read, decode_content=True)
            # Use a tempfolder
            with tmp_file.open("wb") as wf:
                shutil.copyfileobj(response.raw, wf)

            # And use the ndjson importer from schematools, give it a tmp tablename
            # we know the schema, can be an input param (schema_def_from_url function)
            pg_hook = PostgresHook()
            schema = schema_def_from_url(SCHEMA_URL, "meetbouten")
            importer = NDJSONImporter(
                schema, pg_hook.get_sqlalchemy_engine(), logger=self.log
            )

            importer.load_file(
                tmp_file,
                table_name=self.schema,
                db_table_name=f"{self.schema}_tmp",
                truncate=True,  # when reexecuting the same task
            )
