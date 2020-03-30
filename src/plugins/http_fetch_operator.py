from pathlib import Path
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults


class HttpFetchOperator(BaseOperator):
    """ Operator for fetching large amounts of data
        The regular SimpleHttpOperator of Airflow is not
        convenient for this, it does not store the result
    """

    @apply_defaults
    def __init__(
        self,
        endpoint: str,
        data=None,
        headers=None,
        http_conn_id="http_default",
        tmp_file=None,
        *args,
        **kwargs
    ) -> None:
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.headers = headers or {}
        self.http_conn_id = http_conn_id
        self.data = data or {}
        self.tmp_file = tmp_file  # or make temp file + store path in xcom
        super().__init__(*args, **kwargs)

    def execute(self, context):
        Path(self.tmp_file).parents[0].mkdir(parents=True, exist_ok=True)
        http = HttpHook(http_conn_id=self.http_conn_id, method="GET")

        self.log.info("Calling HTTP Fetch method")
        response = http.run(self.endpoint, self.data, self.headers)
        with open(self.tmp_file, "w") as wf:
            wf.write(response.text)
