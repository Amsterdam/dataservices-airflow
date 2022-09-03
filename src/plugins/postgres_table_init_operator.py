from typing import Any, Callable, Optional, cast

from airflow.models.xcom import XCOM_RETURN_KEY
from postgres_on_azure_hook import PostgresOnAzureHook
from postgres_on_azure_operator import PostgresOnAzureOperator
from xcom_attr_assigner_mixin import XComAttrAssignerMixin


class PostgresTableInitOperator(PostgresOnAzureOperator, XComAttrAssignerMixin):
    """Drop or truncated a table and associated n-m cross tables."""

    def __init__(
        self,
        dataset_name: Optional[str] = None,
        table_name: Optional[str] = None,
        nested_db_table_names: Optional[list[str]] = None,
        postgres_conn_id: str = "postgres_default",
        task_id: str = "table_init",
        drop_table: bool = False,
        xcom_task_ids: Optional[str] = None,
        xcom_key: str = XCOM_RETURN_KEY,
        xcom_attr_assigner: Callable[[Any, Any], None] = lambda o, x: None,
        *args: Any,
        **kwargs: Any,
    ):
        """Initialize PostgresTableInitOperator.

        Args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            table_name: Name of the table that needs to be initialized (dropped).
            nested_db_table_names: Optional list of names of nested tables that need
                to be processed.
            sub_table_prefix: Prefix of associated
            task_id: Task ID
            drop_table: Indicates if table needs to be dropped. If false, table will be truncated.
            postgres_conn_id: The PostgreSQL connection id.
            xcom_task_ids: The id of the task that is providing the xcom info.
            xcom_attr_assigner: Callable tha can be provided to assign new values
                to object attributes.
            xcom_key: Key use to grab the xcom info, defaults to the airflow
                default `return_value`.
            *args:
            **kwargs:
        """
        super().__init__(task_id=task_id, sql=[], postgres_conn_id=postgres_conn_id, **kwargs)
        self.table_name = table_name
        self.nested_db_table_names: list[str] = (
            nested_db_table_names if nested_db_table_names is not None else []
        )
        self.drop_table = drop_table
        self.dataset_name = dataset_name

        self.xcom_task_ids = xcom_task_ids
        self.xcom_key = xcom_key
        self.xcom_attr_assigner = xcom_attr_assigner

    def execute(self, context: dict[str, Any]) -> None:  # noqa: D102
        # First get all index names, so it's known which indices to rename
        hook = PostgresOnAzureHook(
            dataset_name=self.dataset_name,
            context=context,
            postgres_conn_id=self.postgres_conn_id,
            schema=self.database)

        # Use the mixin class _assign to assign new values, if provided.
        self._assign(context)

        # Start a list to hold rename information
        table_drops = [cast(str, self.table_name)]

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
        init_operation = "DROP TABLE IF EXISTS" if self.drop_table else "TRUNCATE TABLE"
        self.sql = [
            f"{init_operation} {table_name} CASCADE"
            for table_name in table_drops + cross_tables + self.nested_db_table_names
        ]

        super().execute(context)
