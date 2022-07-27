import os
from typing import Tuple

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from azure.identity import DefaultAzureCredential
import logging

_LOGGER = logging.getLogger(__name__)

class PostgresOnAzureHook(PostgresHook):
    def get_iam_token(self, conn: Connection) -> Tuple[str, str, int]:
        """
        Override PostgresHook get_iam_token with Azure logic
        """

        # This class uses DefaultAzureCredential which will pick up the managed identity
        # using the AZURE_TENANT_ID and AZURE_CLIENT_ID environment variables.
        # Then set the connection using an env var like this:
        # AIRFLOW_CONN_POSTGRES_AZURE:
        # "postgresql://mid-airflow-generic1-ont-weu-01@dev-bbn1-00-dbhost:replacedbymidtoken@dev-bbn1-00-dbhost.postgres.database.azure.com:5432\
        # /dataservices?cursor=dictcursor&iam=true"

        # host = conn.host        # <server_name>.postgres.database.azure.com'
        # server_name = host.partition('.')[0]

        # mid_db_username is something like: `mid_airflow_benkbbn1_ont_weu_01`.
        # Beware! This DB user needs to be created in the referentie DB as an AAD related user.

        login = conn.login  # <mid_db_username>@<server_name>
        password = self.get_token_with_msi()

        _LOGGER.info(conn.get_uri())
        _LOGGER.info(f"username: {login} password: {password}")

        if conn.port is None:
            port = 5432
        else:
            port = conn.port

        return login, password, port

    def get_token_with_msi(self):
        credential = DefaultAzureCredential()
        scope = "https://ossrdbms-aad.database.windows.net/.default"
        token = credential.get_token(scope).token
        return token
