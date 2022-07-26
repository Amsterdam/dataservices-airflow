from typing import TYPE_CHECKING, Iterable, List, Mapping, Optional, Sequence, Union

from postgres_on_azure_hook import PostgresOnAzureHook

from airflow.models import BaseOperator
from airflow.utils.db import merge_conn

class PostgresUpdateAzureTokenOperator(BaseOperator):
    """
    Fetch an Azure AD token for the managed identity and update
    the password field in the referenced Connection with it.
    """

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        generated_postgres_conn_id: str = "postgres_azure",
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.generated_postgres_conn_id = generated_postgres_conn_id
        self.database = database
        self.hook: Optional[PostgresOnAzureHook] = None

    def execute(self, context: 'Context'):
        from airflow.models.connection import Connection

        self.hook = PostgresOnAzureHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )

        # by fetching the connection, the get_iam_token method of
        # the PostgresOnAzureHook will be called and conn will contain
        # the updated token in the password field
        conn = self.hook.get_conn()

        # generate new connection based on current connection because
        # the new connection should not have the iam field
        # (as that will trigger the AWS code of the vanilla postgres operator)
        conn_with_token = Connection(
            conn_id=self.generated_postgres_conn_id, conn_type='postgres',
            login=conn.login,
            password=conn.password,
            schema=conn.schema,
            host=conn.host,
            port=conn.port
            )

        merge_conn(conn_with_token)
