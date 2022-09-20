import os
from typing import TYPE_CHECKING, Any, Optional

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from sqlalchemy import create_engine

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Engine

class PostgresOnAzureHook(PostgresHook):
    """Postgres connection hook for Azure."""

    @apply_defaults  # type: ignore[misc]
    def __init__(
        self, context: Context, dataset_name: Optional[str] = None, *args: Any, **kwargs: Any
    ) -> None:
        """Initialize.

        Args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            context: The execution context. It is used when no dataset_name is given.
                Based on the DAG id - extracted from the execution context - the dataset_name
                is derived. Assuming that the DAG id equals the dataset name as defined in
                Amsterdam schema.
        """
        self.dataset_name = dataset_name
        self.context = context
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        """Runs after __init__."""
        # Due to circular import moved into method.
        # Reason: The PostgresOnAzureHook itself is also imported from
        # within `common.db`.        #
        from common.db import define_dataset_name_for_azure_dbuser

        # If Azure.
        # To cope with a different logic for defining the Azure referentie db user.
        # If CloudVPS is not used anymore, then this extra route can be removed.
        if os.environ.get("AZURE_TENANT_ID") is not None:
            # define the dataset name as part of the db user.
            self.dataset_name = define_dataset_name_for_azure_dbuser(
                dataset_name=self.dataset_name, context=self.context
            )

    def get_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """Override PostgresHook get_iam_token with Azure logic.

        NOTE:
        This method only gets executed if in the connection string the parameter `iam=true`
        is added. Applicable for Azure connections.

        This class uses `DefaultAzureCredential` which will pick up the managed identity
        using the `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` environment variables.
        Then set the connection like this:
        "postgresql://EM4W-DATA-dataset-ot-covid_19-rw@<hostname>:<token>@\
            <hostname>.postgres.database.azure.com:5432/<db_name>?cursor=dictcursor&iam=true"

        The AAD group needs to be registered in the database as an AAD related user.
        See https://docs.microsoft.com/en-us/azure/postgresql/single-server/\
            how-to-configure-sign-in-azure-ad-authentication#authenticate-with-azure-ad-as-a-group-member
        for reference

        Args:
            conn: Name of database connection as defined as airflow_conn_XXX.

        Returns:
            database user, password (token) and port number.
        """
        # Due to circular import moved into method.
        # Reason: The PostgresOnAzureHook itself is also imported from
        # within `common.db`.
        from common.db import generate_dbuser_azure, get_azure_token_with_msi

        username = generate_dbuser_azure(self.dataset_name)
        login = conn.login.replace(
            "AAD-GROUP-NAME-REPLACED-BY-AIRFLOW", username
        )  # must be <group_name>@<server_name>
        password = get_azure_token_with_msi()

        if conn.port is None:
            port = 5432
        else:
            port = conn.port

        return login, password, port


    def get_sqlalchemy_engine(self, split_dictcursor:bool = False, engine_kwargs: Any =None) -> 'Engine':
        """Override of `get_sqlalchemy_engine` method in DbApiHook.

        When doing an execute() on get_sqlalchemy_engine() the
        psycopg2.extensions.parse_dsn() will raise and invalid dsn
        option `cursor`. Therefor the cursor URL parameter is removed.
        This is probably caused since the upgrade of SQLalchemy to 1.4.27.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: An sqlalchemy_engine object; the created engine.
        """
        if split_dictcursor:
            get_uri = self.get_uri().split('?cursor=dictcursor')[0]
        else:
            get_uri = self.get_uri()

        if engine_kwargs is None:
            engine_kwargs = {}
        return create_engine(get_uri, **engine_kwargs)
