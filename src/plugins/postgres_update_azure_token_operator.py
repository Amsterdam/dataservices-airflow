from typing import TYPE_CHECKING, Iterable, List, Mapping, Optional, Sequence, Union

from postgres_on_azure_hook import PostgresOnAzureHook

from airflow import settings
from airflow.models import BaseOperator
from airflow.models import Connection
from airflow.utils.db import provide_session


class PostgresUpdateAzureTokenOperator(BaseOperator):
    """
    Fetch an Azure AD token for the managed identity and update
    the password field in the referenced Connection with it.
    """

    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.hook: Optional[PostgresOnAzureHook] = None

    def execute(self, context: 'Context'):
        self.set_password()

    @provide_session
    def set_password(self, session=None):
        self.hook = PostgresOnAzureHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )

        # by fetching the connection, the get_iam_token method of
        # the PostgresOnAzureHook will be called and conn will contain
        # the updated token in the password field
        conn = self.hook.get_conn()
        
        # save the connection
        session.add(conn)
        session.commit()
