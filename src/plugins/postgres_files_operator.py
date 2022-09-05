from typing import Optional

from airflow.operators.bash import BashOperator
from airflow.utils.context import Context
from common.db import pg_params


class PostgresFilesOperator(BashOperator):
    def __init__(self, sql_files: list, dataset_name: Optional[str] = None, *args, **kwargs):
        """Import files to Postgres database.

        Connection uri has a 'cursor' argument, psql does not recognize it
        and it is not needed here, so we chop it off.

        args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
        """
        self.sql_files = sql_files
        self.dataset_name = dataset_name
        self.args = args
        self.kwargs = kwargs
        # set for now bash_command to None, just to get context.
        super().__init__(*self.args, bash_command=None, **self.kwargs)

    def execute(self, context: Context):
        """Execute."""
        paths = " ".join(f'"{f}"' for f in self.sql_files)
        bash_command = (
            f'cat {paths} | psql "{pg_params(dataset_name=self.dataset_name, context=context)}"'
        )
        super().__init__(*self.args, bash_command=bash_command, **self.kwargs)
