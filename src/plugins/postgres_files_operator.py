from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator

pg_params = " ".join(
    [
        "-1",
        "-X",
        "--set",
        "ON_ERROR_STOP",
    ]
)


class PostgresFilesOperator(BashOperator):
    def __init__(self, sql_files, conn_id="postgres_default", *args, **kwargs):
        """Connection uri has a 'cursor' argument, psql does not recognize it
        and it is not needed here, so we chop it off
        """
        connection_uri = BaseHook.get_connection(conn_id).get_uri().split("?")[0]
        paths = " ".join(f'"{f}"' for f in sql_files)
        bash_command = f'cat {paths} | psql "{connection_uri}" {pg_params}'
        super().__init__(*args, bash_command=bash_command, **kwargs)
