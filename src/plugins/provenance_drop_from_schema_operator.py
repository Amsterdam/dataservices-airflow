from typing import Final

from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from environs import Env
from schematools.utils import dataset_schema_from_url, to_snake_case

env = Env()
SCHEMA_URL: Final = env("SCHEMA_URL")


class ProvenanceDropFromSchemaOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        dataset_name,
        pg_schema="public",
        additional_table_names=None,
        postgres_conn_id="postgres_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dataset_name = dataset_name
        self.pg_schema = pg_schema
        self.additional_table_names = additional_table_names

    def execute(self, context=None):
        dataset = dataset_schema_from_url(SCHEMA_URL, self.dataset_name)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        table_names = self.additional_table_names or []
        for table in dataset.tables:
            table_names.append(table.id)
            provenance_tablename = table.get("provenance")
            if provenance_tablename is not None:
                table_names.append(provenance_tablename)

        sqls = [
            f"DROP TABLE IF EXISTS {self.pg_schema}.{to_snake_case(table_name)} CASCADE"
            for table_name in table_names
        ]

        pg_hook.run(sqls)
