from typing import Any, Optional

from airflow.operators.bash import BashOperator
from common.db import pg_params


class PostgresFilesOperator(BashOperator):
    """Class definition."""

    def __init__(
        self, sql_files: list, dataset_name: Optional[str] = None, *args: Any, **kwargs: Any
    ):
        """Import files to Postgres database.

        Connection uri has a 'cursor' argument, psql does not recognize it
        and it is not needed here, so we chop it off.

        Args:
            sql_files: SQL files to execute.
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
        # TODO: Cannot use the DAG context in the pg_params function
        # since it requires initialization of the instance first. But the latter
        # is depending on the context itself by the `bash_command` init parameter.
        # For now, we explictly set the `dataset_name`` so no context is needed.
        self.cntx = "cannot provide context"
        paths = " ".join(f'"{f}"' for f in self.sql_files)
        self.bash_command = (
            f'cat {paths} | psql "{pg_params(dataset_name=self.dataset_name, context=self.cntx)}"'
        )
        super().__init__(*self.args, bash_command=self.bash_command, **self.kwargs)
