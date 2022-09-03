from typing import Final

from airflow.models.baseoperator import BaseOperator
from postgres_on_azure_hook import PostgresOnAzureHook
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
        pg_schema:str="public",
        additional_table_names:list=None,
        postgres_conn_id:str="postgres_default",
        *args,
        **kwargs,
    ):
        """Initialize.

        args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.

        """
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dataset_name = dataset_name
        self.pg_schema = pg_schema
        self.additional_table_names = additional_table_names

    def execute(self, context=None):
        dataset = dataset_schema_from_url(SCHEMA_URL, self.dataset_name)
        pg_hook = PostgresOnAzureHook(dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id)

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
