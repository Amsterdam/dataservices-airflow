import subprocess
from functools import partial
from typing import Optional

from airflow.hooks.base import BaseHook
from common.db import pg_params


class PsqlCmdHook(BaseHook):
    """Excutes sql files on a postgres DB triggered by a bash cli cmd (subprocess.run)."""

    def __init__(
        self,
        dataset_name: Optional[str] = None,
        db_target_schema=None,
        conn_id="postgres_default",
        db_search_path=None,
        *args,
        **kwargs,
    ):
        """Initialize.

        args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            db_search_path: List of one or more database schema names that needs to be
                present in the search path. I.e. for locating geometry datatypes.
                Defaults to None.

        returns:
            class instance.
        """
        self.conn_id = conn_id
        self.db_target_schema = db_target_schema
        self.dataset_name = dataset_name
        self.db_search_path = db_search_path

    def run(self, sql_files):
        paths = " ".join(f'"{f}"' for f in sql_files)

        # prefill pg_params method with dataset name so
        # it can be used for the database connection as a user.
        # only applicable for Azure connections.
        db_conn_string = partial(pg_params, dataset_name=self.dataset_name, db_search_path=self.db_search_path, pg_params=True)

        if self.db_target_schema:
            self.recreate_schema(self.db_target_schema, db_conn_string())

        self.log.info("Running sql files: %s", sql_files)
        subprocess.run(f"cat {paths} | psql {db_conn_string()}", shell=True, check=True)

    def recreate_schema(self, db_target_schema, connection_uri):
        """
        If a schema is defined at the creation of the instance, it will create the schema (if not exists)
        """

        self.log.info(f"Creating the DB target schema '{db_target_schema}' if not present")
        subprocess.run(
            f'echo "CREATE SCHEMA IF NOT EXISTS {db_target_schema}" | psql {connection_uri}',
            shell=True,
            check=True,
        )
