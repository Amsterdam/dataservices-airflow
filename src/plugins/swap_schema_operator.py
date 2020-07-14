from environs import Env
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from schematools.utils import schema_def_from_url, to_snake_case

env = Env()
SCHEMA_URL = env("SCHEMA_URL")


class SwapSchemaOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        dataset_name,
        from_pg_schema="pte",
        to_pg_schema="public",
        postgres_conn_id="postgres_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dataset_name = dataset_name
        self.from_pg_schema = from_pg_schema
        self.to_pg_schema = to_pg_schema

    def execute(self, context=None):
        dataset = schema_def_from_url(SCHEMA_URL, self.dataset_name)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        sqls = []
        dataset_id = to_snake_case(dataset.id)
        for table in dataset.tables:
            table_id = to_snake_case(table.id)
            sqls.append(
                f"""
                DROP TABLE IF EXISTS {self.to_pg_schema}.{dataset_id}_{table_id};
                ALTER TABLE {self.from_pg_schema}.{table_id} SET SCHEMA {self.to_pg_schema};
                ALTER TABLE {table_id}
                    RENAME TO {dataset_id}_{table_id}; """
            )
        pg_hook.run(sqls)
