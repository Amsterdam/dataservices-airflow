import os
from typing import Tuple

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from azure.identity import DefaultAzureCredential


class PostgresOnAzureHook(PostgresHook):
    def get_iam_token(self, conn: Connection) -> Tuple[str, str, int]:
        """
        Override PostgresHook get_iam_token with Azure logic
        """

        # Make sure that USER_ASSIGNED_MANAGED_IDENTITY is set to the client id of
        # the managed identity. Then set the connection using an env var like this:
        # AIRFLOW_CONN_POSTGRES_AZURE:
        # "postgresql://mid-airflow-generic1-ont-weu-01@dev-bbn1-00-dbhost:replacedbymidtoken@dev-bbn1-00-dbhost.postgres.database.azure.com:5432\
        # /dataservices?cursor=dictcursor&iam=true"

        # host = conn.host        # <server_name>.postgres.database.azure.com'
        # server_name = host.partition('.')[0]

        # mid_db_username is something like: `mid_airflow_benkbbn1_ont_weu_01`.
        # Beware! This DB user needs to be created in the referentie DB as an AAD related user.

        login = conn.login  # <mid_db_username>@<server_name>
        password = self.get_token_with_msi()

        if conn.port is None:
            port = 5432
        else:
            port = conn.port

        return login, password, port

    def get_token_with_msi(self):
        mid_client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")

        if not mid_client_id:
            credential = DefaultAzureCredential(managed_identity_client_id=mid_client_id)
            scope = "https://ossrdbms-aad.database.windows.net/.default"
            token = credential.get_token(scope).token
            return token
        else:
            raise UserWarning("USER_ASSIGNED_MANAGED_IDENTITY env var was not set")
