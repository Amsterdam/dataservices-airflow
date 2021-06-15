import subprocess

from airflow.hooks.base_hook import BaseHook

# XXX move to common, also used in PostgresFilesOperator
pg_params = " ".join(
    [
        "-1",
        "-X",
        "--set",
        "ON_ERROR_STOP",
    ]
)


class PsqlCmdHook(BaseHook):
    """
    Excutes sql files on a postgres DB triggered by a bash cli cmd (subprocess.run)
    """

    def __init__(self, db_target_schema=None, conn_id="postgres_default", *args, **kwargs):
        self.conn_id = conn_id
        self.db_target_schema = db_target_schema

    def run(self, sql_files):
        paths = " ".join(f'"{f}"' for f in sql_files)
        connection_uri = BaseHook.get_connection(self.conn_id).get_uri().split("?")[0]

        if self.db_target_schema:
            self.recreate_schema(self.db_target_schema, connection_uri)

        self.log.info("Running sql files: %s", sql_files)
        subprocess.run(
            f'cat {paths} | psql "{connection_uri}" {pg_params}', shell=True, check=True
        )

    def recreate_schema(self, db_target_schema, connection_uri):
        """
        If a schema is defined at the creation of the instance, it will create the schema (if not exists)
        """

        self.log.info(f"Creating the DB target schema '{db_target_schema}' if not present")
        subprocess.run(
            f'echo "CREATE SCHEMA IF NOT EXISTS {db_target_schema}" | psql "{connection_uri}" {pg_params}',
            shell=True,
            check=True,
        )
