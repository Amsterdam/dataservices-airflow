from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

from check_helpers import check_safe_name


class PostgresTableDropOperator(PostgresOperator):
    """Drop a table and associated n-m cross tables"""

    @apply_defaults
    def __init__(
        self,
        table_name: str,
        postgres_conn_id="postgres_default",
        task_id="rename_table",
        **kwargs,
    ):
        check_safe_name(table_name)
        super().__init__(
            task_id=task_id, sql=[], postgres_conn_id=postgres_conn_id, **kwargs
        )
        self.table_name = table_name

    def execute(self, context):
        # First get all index names, so it's known which indices to rename
        hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )

        # Start a list to hold rename information
        table_drops = [self.table_name]

        # Find the cross-tables for n-m relations, we assume they have
        # a name that start with f"{table_name}_"

        with hook.get_cursor() as cursor:

            cursor.execute(
                """
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public' AND tablename like %s
                """,
                (f"{self.table_name}_%",),
            )

            cross_tables = [row["tablename"] for row in cursor.fetchall()]

        # Define the SQL to execute by the super class.
        # This supports executing multiple statements in a single transaction:
        self.sql = [
            f"DROP TABLE IF EXISTS {table_name} CASCADE"
            for table_name in table_drops + cross_tables
        ]

        return super().execute(context)
