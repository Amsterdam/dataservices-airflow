from typing import Optional
from postgres_on_azure_hook import PostgresOnAzureHook
from postgres_on_azure_operator import PostgresOnAzureOperator


class PostgresXcomOperator(PostgresOnAzureOperator):
    """Regular PostgresOperator does not return a value,
    so cannot do Xcom
    """
    def __init__(self, dataset_name:Optional[str] = None, *args, **kwargs):
        """Initialize.

        args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.

        """
        self.dataset_name = dataset_name
        self.args = args
        self.kwargs = kwargs
        super().__init__(dataset_name=self.dataset_name, *self.args, **self.kwargs)

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        self.hook = PostgresOnAzureHook(dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id, schema=self.database)
        return self.hook.get_first(self.sql, parameters=self.parameters)
