from environs import Env
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from schematools.utils import schema_def_from_url, to_snake_case
from typing import Optional, List, Any, Dict

env = Env()
SCHEMA_URL = env("SCHEMA_URL")


class SwapSchemaOperator(BaseOperator):
    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        dataset_name: str,
        from_pg_schema: str = "pte",
        to_pg_schema: str = "public",
        postgres_conn_id: str = "postgres_default",
        subset_tables: Optional[List] = None,
        *args: Any,
        **kwargs: Dict,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dataset_name = dataset_name
        self.from_pg_schema = from_pg_schema
        self.to_pg_schema = to_pg_schema
        self.subset_tables = subset_tables

    def execute(self, context: Optional[Dict] = None) -> None:
        """Moves database objects (in this case tables) to other schema owner

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.

        Executes:
            SQL alter statement to change the schema owner of the table so the
            table is moved to the defined schema (a.k.a. schema swapping)

        """
        dataset = schema_def_from_url(SCHEMA_URL, self.dataset_name)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        sqls = []
        dataset_id = to_snake_case(dataset.id)
        tables = dataset.tables

        if self.subset_tables:
            subset_tables = [to_snake_case(table) for table in self.subset_tables]
            tables = [table for table in tables if table["id"] in subset_tables]

        for table in tables:
            table_id = to_snake_case(table.id)
            sqls.append(
                f"""
                DROP TABLE IF EXISTS {self.to_pg_schema}.{dataset_id}_{table_id};
                ALTER TABLE {self.from_pg_schema}.{table_id} SET SCHEMA {self.to_pg_schema};
                ALTER TABLE {table_id}
                    RENAME TO {dataset_id}_{table_id}; """
            )
        pg_hook.run(sqls)
