from typing import Any, Dict

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults
from check_helpers import check_safe_name


class PostgresTableCopyOperator(PostgresOperator):
    """Copy table to another table, create target table structure if needed from source """

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        source_table_name: str,
        target_table_name: str,
        postgres_conn_id: str = "postgres_default",
        task_id: str = "copy_table",
        **kwargs: Any,
    ) -> None:
        check_safe_name(source_table_name)
        check_safe_name(target_table_name)
        super().__init__(
            task_id=task_id,
            sql=[],
            postgres_conn_id=postgres_conn_id,
            **kwargs,
        )
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name

    def execute(self, context: Dict[str, Any]) -> None:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

        # Start a list to hold copy information
        table_copies = [
            (
                self.source_table_name,
                self.target_table_name,
            )
        ]

        # Find the cross-tables for n-m relations, we assume they have
        # a name that start with f"{source_table_name}_"

        with hook.get_cursor() as cursor:
            # the underscore must be escaped because of it's special meaning in a like
            # the exclamation mark was used as an escape chacater because
            # a backslash was not interpreted as an escape
            cursor.execute(
                """
                    SELECT tablename AS name FROM pg_tables
                    WHERE schemaname = 'public' AND tablename like %s ESCAPE '!'
                """,
                (f"{self.source_table_name}!_%",),
            )

            cross_tables = cursor.fetchall()

        copies = []
        for row in cross_tables:
            source_table_name = row["name"]
            target_table_name = source_table_name.replace("_new", "")
            copies.append((source_table_name, target_table_name))

        # Define the SQL to execute by the super class.
        # This supports executing multiple statements in a single transaction
        self.sql = []

        for source_table_name, target_table_name in table_copies + copies:
            lookup = dict(
                source_table_name=source_table_name,
                target_table_name=target_table_name,
            )
            for sql in (
                "CREATE TABLE IF NOT EXISTS {target_table_name} (LIKE {source_table_name} "
                "INCLUDING CONSTRAINTS INCLUDING INDEXES)",
                "TRUNCATE TABLE {target_table_name} CASCADE",
                "INSERT INTO {target_table_name} SELECT * FROM {source_table_name}",
                "DROP TABLE IF EXISTS {source_table_name} CASCADE",
            ):

                self.sql.append(sql.format(**lookup))

        super().execute(context)
