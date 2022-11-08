import re
from typing import Any, Optional

from airflow.models.taskinstance import Context
from airflow.utils.decorators import apply_defaults
from check_helpers import check_safe_name
from postgres_on_azure_hook import PostgresOnAzureHook
from postgres_on_azure_operator import PostgresOnAzureOperator
from psycopg2 import sql


class PostgresTableRenameOperator(PostgresOnAzureOperator):
    """Rename a table."""

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        old_table_name: str,
        new_table_name: str,
        dataset_name: Optional[str] = None,
        postgres_conn_id: str = "postgres_default",
        task_id: str = "rename_table",
        cascade: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize.

        params:
            old_table_name: Table to be renamed.
            new_table_name: Table to rename to.
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            postgres_conn_id: Connection string to referentie database. Defaults to name
                `postgres_default`.
            task_id: Name of task that will be used for parent class PostgresOperator.
                Defaults to `rename_table`.
            cascade: Option to do a DROP table with the CASCADE option added. Defaults
                to False.

        Executes:
            SQL ALTER TABLE statements to rename tables within context.
        """
        check_safe_name(old_table_name)
        check_safe_name(new_table_name)
        super().__init__(task_id=task_id, sql="", postgres_conn_id=postgres_conn_id, **kwargs)
        self.old_table_name = old_table_name
        self.new_table_name = new_table_name
        self.cascade = cascade
        self.dataset_name = dataset_name

    def execute(self, context: Context) -> None:
        """Main execution logic. Iherts from PostgresOnAzureOperator."""
        # First get all index names, so it's known which indices to rename
        hook = PostgresOnAzureHook(
            dataset_name=self.dataset_name,
            context=context,
            postgres_conn_id=self.postgres_conn_id,
            schema=self.database,
        )

        # Start a list to hold rename information
        table_renames = [
            (
                self.old_table_name,
                self.new_table_name,
                f"{self.new_table_name}_old",
            )
        ]

        # Find the cross-tables for n-m relations, we assume they have
        # a name that start with f"{old_table_name}_"

        with hook.get_cursor() as cursor:
            # the underscore must be escaped because of it's special meaning in a like the
            # exclamation mark was used as an escape chacater because a backslash was not
            # interpreted as an escape
            cursor.execute(
                """
                    SELECT tablename AS name FROM pg_tables
                    WHERE schemaname = 'public' AND tablename like %s ESCAPE '!'
                """,
                (f"{self.old_table_name}!_%",),
            )

            cross_tables = cursor.fetchall()
            cursor.execute(
                """
                    SELECT indexname AS name FROM pg_indexes
                    WHERE schemaname = 'public' AND indexname LIKE %s ESCAPE '!'
                    ORDER BY indexname
                """,
                (f"%{self.old_table_name}!_%",),
            )
            indexes = cursor.fetchall()
        renames = []
        for row in cross_tables:
            old_table_name = row["name"]
            new_table_name = old_table_name.replace("_new", "")
            backup_table_name = f"{new_table_name}_old"
            renames.append((old_table_name, new_table_name, backup_table_name))

        idx_renames = [
            (
                row["name"],
                row["name"].replace(self.old_table_name, self.new_table_name),
            )
            for row in indexes
        ]

        # Define the SQL to execute by the super class.
        # This supports executing multiple statements in a single transaction:
        self.sql = []

        cascade = " CASCADE" if self.cascade else None
        for sql_statements in (
            sql.SQL("ALTER TABLE IF EXISTS {new_table_name} RENAME TO {backup_table_name}"),
            sql.SQL("ALTER TABLE IF EXISTS {old_table_name} RENAME TO {new_table_name}"),
            sql.SQL(" ".join(["DROP TABLE IF EXISTS {backup_table_name}", f"{cascade}"])),
        ):

            for old_table_name, new_table_name, backup_table_name in table_renames + renames:
                lookup = {
                    "old_table_name": sql.Identifier(old_table_name),
                    "new_table_name": sql.Identifier(new_table_name),
                    "backup_table_name": sql.Identifier(backup_table_name),
                }
                self.sql.append(sql_statements.format(**lookup))

        for old_name, new_name in idx_renames:
            self.sql.append(
                sql.SQL("DROP INDEX IF EXISTS {new_name}").format(
                    new_name=sql.Identifier(new_name)
                )
            )
            self.sql.append(
                sql.SQL("ALTER INDEX IF EXISTS {old_name} RENAME TO {new_name}").format(
                    old_name=sql.Identifier(old_name), new_name=sql.Identifier(new_name)
                )
            )

        # execute SQL statements
        super().execute(context)


def _get_complete_word_pattern(word: str) -> str:
    """Create a search pattern that looks for whole words only."""
    return fr"{re.escape(word)}"
