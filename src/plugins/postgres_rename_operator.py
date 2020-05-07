import re

from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

from check_helpers import check_safe_name


class PostgresTableRenameOperator(PostgresOperator):
    """Rename a table"""

    @apply_defaults
    def __init__(
        self,
        old_table_name: str,
        new_table_name: str,
        postgres_conn_id="postgres_default",
        task_id="rename_table",
        **kwargs,
    ):
        check_safe_name(old_table_name)
        check_safe_name(new_table_name)
        super().__init__(
            task_id=task_id, sql=[], postgres_conn_id=postgres_conn_id, **kwargs
        )
        self.old_table_name = old_table_name
        self.new_table_name = new_table_name

    def execute(self, context):
        # First get all index names, so it's known which indices to rename
        hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        with hook.get_cursor() as cursor:
            cursor.execute(
                "SELECT indexname FROM pg_indexes"
                " WHERE schemaname = 'public' AND indexname like %s"
                " ORDER BY indexname;",
                (f"%{self.old_table_name}%",),
            )
            indexes = list(cursor.fetchall())

        index_renames = [
            (
                row["indexname"],
                re.sub(
                    pattern=_get_complete_word_pattern(self.old_table_name),
                    repl=self.new_table_name,
                    string=row["indexname"],
                    count=1,
                ),
            )
            for row in indexes
        ]

        backup_table = f"{self.new_table_name}_old"

        # Define the SQL to execute by the super class.
        # This supports executing multiple statements in a single transaction:
        self.sql = [
            f"ALTER TABLE IF EXISTS {self.new_table_name} RENAME TO {backup_table}",
            f"ALTER TABLE {self.old_table_name} RENAME TO {self.new_table_name}",
            f"DROP TABLE IF EXISTS {backup_table}",
        ] + [
            f"ALTER INDEX {old_index} RENAME TO {new_index}"
            for old_index, new_index in index_renames
        ]

        return super().execute(context)


def _get_complete_word_pattern(word):
    """Create a search pattern that looks for whole words only."""
    return r"{word}".format(word=re.escape(word))
