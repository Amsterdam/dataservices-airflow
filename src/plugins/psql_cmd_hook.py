import subprocess
from airflow.hooks.base_hook import BaseHook

# XXX move to common, also used in PostgresFilesOperator
pg_params = " ".join(["-1", "-X", "--set", "ON_ERROR_STOP",])


class PsqlCmdHook(BaseHook):
    def __init__(self, conn_id="postgres_default", *args, **kwargs):
        self.conn_id = conn_id

    def run(self, sql_files):
        self.log.info("Running sql files: %s", sql_files)
        connection_uri = BaseHook.get_connection(self.conn_id).get_uri().split("?")[0]
        paths = " ".join(f'"{f}"' for f in sql_files)
        subprocess.run(
            f'cat {paths} | psql "{connection_uri}" {pg_params}', shell=True, check=True
        )
