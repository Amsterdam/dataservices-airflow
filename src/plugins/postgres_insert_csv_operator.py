import csv
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from airflow.models import BaseOperator
from postgres_on_azure_hook import PostgresOnAzureHook
from airflow.utils.decorators import apply_defaults
from psycopg2 import sql
from psycopg2.extras import execute_batch


@dataclass
class FileTable:
    """Simple mapping of CSV file to DB table."""

    file: Path
    table: str


class PostgresInsertCsvOperator(BaseOperator):
    """Bulk insert rows from CSV files into tables.

    Which CSV files to insert into which tables is defined using a simple mapping
    :class:`FileTable`. As these mappings are stored in a tuple we can specify multiple
    bulk inserts without having to spawn multiple instances of this operator.

    If :attr:`FileTable.file` is a relative path, we will try to resolve it relative to a base
    directory. What that base directory is, is expected to be communicated to us by means of
    XCom.

    For instance, assume that a task created a temporary directory for use by the DAG for the
    duration of its execution. And that the task has stored the path of that directory in XCom.
    Then we should be able to retrieve it by specifying the ``base_dir_task_id`` argument with
    the task ID of that task.

    If you do not want to rely on XCom for the specification of a base dir, you should use
    absolute paths for :attr:`FileTable.file`.

    The CSV file are expected to be in the :class:`csv.unix_dialect` format with a header. The
    column names in the header of the CSV file should match those of the column names in the DB
    table. Their order does not matter. But their number obviously do (otherwise they don't
    match).

    .. warning:: Currently this operator assumes that an empty string represents a NULL value!
    """

    @apply_defaults
    def __init__(
        self,
        data: tuple[FileTable, ...],
        dataset_name: Optional[str] = None,
        base_dir_task_id: Optional[str] = None,
        page_size: int = 1000,
        postgres_conn_id: str = "postgres_default",
        autocommit: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize PostgresInsertCsvOperator.

        Args:
            data: files with associated table names
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            base_dir_task_id: Task ID of task that specifies a base dir (eg tmp dir) to resolve
                relative :attr:`FileTable.file` paths against.
            page_size: Determines how many rows can be inserted in one go. See
                https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_batch for
                more information.
            postgres_conn_id: The PostgreSQL connection id.
            autocommit: What to set the connection's autocommit setting to before executing
                the query.
            *args:
            **kwargs:
        """
        super().__init__(*args, **kwargs)
        self.data = data
        self.dataset_name = dataset_name
        self.base_dir_task_id = base_dir_task_id
        self.page_size = page_size
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit

    def execute(self, context: dict[str, Any]) -> None:
        base_dir = Path()
        if self.base_dir_task_id is not None:
            base_dir = Path(context["task_instance"].xcom_pull(task_ids=self.base_dir_task_id))
            self.log.info("Setting base_dir to '%s'.", base_dir)
        hook = PostgresOnAzureHook(dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id)
        with closing(hook.get_conn()) as conn:
            if hook.supports_autocommit:
                self.log.debug("Setting autocommit to '%s'.", self.autocommit)
                hook.set_autocommit(conn, self.autocommit)
            with closing(conn.cursor()) as cursor:
                for ft in self.data:
                    file = ft.file if ft.file.is_absolute() else base_dir / ft.file
                    self.log.info("Processing file '%s' for table '%s'.", file, ft.table)
                    with file.open(newline="") as csv_file:
                        reader = csv.DictReader(csv_file, dialect=csv.unix_dialect)
                        assert (
                            reader.fieldnames is not None
                        ), f"CSV file '{file}' needs to have a header."
                        cursor.execute(
                            sql.SQL(
                                """
                                PREPARE csv_ins_stmt AS INSERT INTO {table} ({columns})
                                                        VALUES ({values})
                                """
                            ).format(
                                table=sql.Identifier(ft.table),
                                columns=sql.SQL(", ").join(map(sql.Identifier, reader.fieldnames)),
                                values=sql.SQL(", ").join(
                                    sql.SQL(f"${pos}")
                                    for pos in range(1, len(reader.fieldnames) + 1)
                                ),
                            )
                        )
                        execute_batch(
                            cursor,
                            sql.SQL("EXECUTE csv_ins_stmt ({params})").format(
                                params=sql.SQL(", ").join(
                                    sql.Placeholder(col) for col in reader.fieldnames
                                )
                            ),
                            # Nasty hack to treat empty string values as None
                            (
                                {k: v if v != "" else None for k, v in row.items()}
                                for row in reader
                            ),
                            self.page_size,
                        )
                        cursor.execute("DEALLOCATE csv_ins_stmt")
            if not hook.get_autocommit(conn):
                self.log.debug("Committing transaction.")
                conn.commit()
        for output in hook.conn.notices:
            self.log.info(output)
