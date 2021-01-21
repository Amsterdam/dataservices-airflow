import shutil, os, re
from pathlib import Path
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults


class HttpFetchOperator(BaseOperator):
    """ Operator for fetching large amounts of data
        The regular SimpleHttpOperator of Airflow is not
        convenient for this, it does not store the result
    """

    template_fields = [
        "endpoint",
        "data",
        "headers",
    ]

    @apply_defaults
    def __init__(
        self,
        endpoint: str,
        data=None,
        headers=None,
        http_conn_id="http_default",
        encoding_schema=None,
        tmp_file=None,
        output_type=None,
        *args,
        **kwargs,
    ) -> None:
        self.endpoint = endpoint
        self.headers = headers or {}
        self.http_conn_id = http_conn_id
        self.encoding_schema = encoding_schema
        self.data = data or {}
        self.tmp_file = tmp_file  # or make temp file + store path in xcom
        self.output_type = output_type  # default is raw, else specify text i.e.
        super().__init__(*args, **kwargs)

    def execute(self, context):

        # ---------- TEMPORARY IF REMOVE IT AFTER precariobelasting HAS DATA ON OBJECTSTORE PRD -----------
        if self.output_type != "file":
        # ---------- TEMPORARY IF REMOVE IT AFTER precariobelasting HAS DATA ON OBJECTSTORE PRD -----------

            Path(self.tmp_file).parents[0].mkdir(parents=True, exist_ok=True)
            http = HttpHook(http_conn_id=self.http_conn_id, method="GET")


            self.log.info("Calling HTTP Fetch method")
            self.log.info(self.endpoint)
            response = http.run(
                self.endpoint, self.data, self.headers, extra_options={"stream": True}
            )
            # set encoding schema explictly if given
            if self.encoding_schema:
                response.encoding = self.encoding_schema
            self.log.info(f"Encoding schema is {response.encoding}")

            # When content is encoded (gzip etc.) we need this
            # response.raw.read = functools.partial(response.raw.read, decode_content=True)
            if self.output_type == "text":
                with open(self.tmp_file, "wt") as wf:
                    wf.write(response.text)
            else:
                with open(self.tmp_file, "wb") as wf:
                    shutil.copyfileobj(response.raw, wf)

        # ---------- TEMPORARY ELSE REMOVE IT AFTER precariobelasting HAS DATA ON OBJECTSTORE PRD -----------
        else:
            print(os.getcwd())
            with open(self.endpoint) as source:
                data = source.read()

            with open(self.tmp_file, "wt") as wf:
                wf.write(data)

        # ---------- TEMPORARY ELSE REMOVE IT AFTER precariobelasting HAS DATA ON OBJECTSTORE PRD -----------
