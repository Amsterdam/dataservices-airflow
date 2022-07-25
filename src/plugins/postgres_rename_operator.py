import re
from typing import Any

from airflow.models.taskinstance import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from check_helpers import check_safe_name


class PostgresTableRenameOperator(PostgresOperator):
    """Rename a table."""

    # type: ignore [misc]
    def __init__(
        self,
        old_table_name: str,
        new_table_name: str,
        postgres_conn_id: str = "postgres_default",
        task_id: str = "rename_table",
        cascade: bool = False,
        **kwargs: Any,
    ) -> None:
        check_safe_name(old_table_name)
        check_safe_name(new_table_name)
        super().__init__(task_id=task_id, sql="", postgres_conn_id=postgres_conn_id, **kwargs)
        self.old_table_name = old_table_name
        self.new_table_name = new_table_name
        self.cascade = cascade

    def execute(self, context: Context) -> None:
        # First get all index names, so it's known which indices to rename
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

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

        cascade = " CASCADE" if self.cascade else ""
        for sql in (
            "ALTER TABLE IF EXISTS {new_table_name} RENAME TO {backup_table_name}",
            "ALTER TABLE IF EXISTS {old_table_name} RENAME TO {new_table_name}",
            "DROP TABLE IF EXISTS {backup_table_name}{cascade}",
        ):

            for old_table_name, new_table_name, backup_table_name in table_renames + renames:
                lookup = {
                    "old_table_name": old_table_name,
                    "new_table_name": new_table_name,
                    "backup_table_name": backup_table_name,
                    "cascade": cascade,
                }
                # FIXME Don't use string formatting! Use `psycopg2.sql` instead.
                self.sql.append(sql.format(**lookup))

        for old_name, new_name in idx_renames:
            self.sql.append(f"ALTER INDEX IF EXISTS {old_name} RENAME TO {new_name}")

        super().execute(context)


def _get_complete_word_pattern(word: str) -> str:
    """Create a search pattern that looks for whole words only."""
    return fr"{re.escape(word)}"
