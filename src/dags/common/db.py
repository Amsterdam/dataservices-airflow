import os
from typing import Any, Final, Optional

import cx_Oracle
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

# from airflow.models.baseoperator import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.context import Context
from azure.identity import DefaultAzureCredential
from geoalchemy2 import WKTElement
from postgres_on_azure_hook import PostgresOnAzureHook
from schematools.utils import to_snake_case
from shapely import wkt
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

TARGET_DB_SCHEMA: Final = "public"


class DatabaseEngine:
    """Construct the elements of the SQLAlchemy database engine."""

    def __init__(
        self,
        context: Context,
        dataset_name: Optional[str] = None,
        postgres_conn_id: str = "postgres_default",
    ) -> None:
        """Initialize DatabaseEngine.

        params:
            context: Based on the calling DAG, its ID will
                be used as a surrogate dataset_name as part
                of the database user name.
                Only used if dataset_name is None.
            dataset_name: Name of the dataset to proces.
                Will be used as part of the database
                user name.
                Example: `EM4W-DATA-dataset-ot-<dataset>-rw`
            postgres_conn_id: The connection ID to the
                database. Defaults to `postgres_default`.
                This value will be looked up as an environment
                variable called `AIRFLOW_CONN_POSTGRES_DEFAULT`
                or as a secret key in Azure Key Vault.
        """
        self.dataset_name = dataset_name
        self.postgres_conn_id = postgres_conn_id
        self.context = context

        self.connection = PostgresOnAzureHook(
            dataset_name=self.dataset_name, context=self.context
        ).get_connection(conn_id=self.postgres_conn_id)

        if os.environ.get("AZURE_TENANT_ID") is not None:

            # Define the db user to be used.
            self.dataset_name = define_dataset_name_for_azure_dbuser(
                dataset_name=self.dataset_name, context=self.context
            )
            self.token_info = PostgresOnAzureHook(
                dataset_name=self.dataset_name, context=self.context
            ).get_iam_token(conn=self.connection)
            self.user = self.token_info[0]
            self.password = self.token_info[1]
            # the temp database schema has the same name as the dataset.
            self.temp_db_schema = to_snake_case(self.dataset_name)

        else:
            self.user = self.connection.login
            self.password = self.connection.password
            self.temp_db_schema = TARGET_DB_SCHEMA

        self.host = self.connection.host
        self.port = self.connection.port
        self.db = self.connection.schema
        self.target_db_schema = TARGET_DB_SCHEMA

    def get_engine(self) -> Engine:
        """Create SQLAlchemy engine."""
        try:
            return create_engine(
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
            )
        except SQLAlchemyError as e:
            raise AirflowException(str(e)) from e

    def get_postgreshook_instance(self) -> PostgresOnAzureHook:
        """Create PostgresOnAzureHook instance."""
        return PostgresOnAzureHook(dataset_name=self.dataset_name, context=self.context)

    def fetch_pg_env_vars(self) -> dict[str, str]:
        """Get the Postgres default DSN connection info as a dictionary.

        Returns:
            A dictionary of connection string elements.
        """
        return {
            "PGHOST": self.host,
            "PGPORT": str(self.port),
            "PGDATABASE": self.db,
            "PGUSER": self.user,
            "PGPASSWORD": self.password,
        }


def define_temp_db_schema(dataset_name: str) -> Any:
    """Define the sketch database schema to use.

    On Azure the `public` database schema does not
    allow for DDL commands. Therefor for each database user
    a specific database schema is used to execute DDL SQL
    for temporary data transformations (if needed).

    Params:
        dataset_name: Name of the dataset to proces.
            Will be used as part of the database
            schema name for DDL actions.
            Schema name is the same as the dataset.

    Returns:
        The name of the sketch database schema to use.
    """
    return DatabaseEngine(dataset_name=dataset_name, context="context").temp_db_schema


def get_ora_engine(oracle_conn_id: str = "oracle_default") -> Engine:
    """Get the oracle connection parameters.

    Some KPN's Oracle databases are provided in a container that cannot
        be accessed by a SID.
    A service_name parameter must be provided in the connection string to
        make the connection to the source DB.
    The OracleHook provides a method get_conn() that can be used to setup
        the connection by service_name.
    More info:
    https://airflow.apache.org/docs/apache-airflow-providers-oracle/stable/_modules/airflow/providers/oracle/hooks/oracle.html#OracleHook.get_conn
    """  # noqa: E501
    conn_instance = OracleHook(oracle_conn_id=oracle_conn_id)
    return conn_instance


def wkt_loads_wrapped(data: str, source_srid: int, geom_type_family:str) -> WKTElement:
    """Loading WKT (well known text) geometry definition.

    This function translates a single geometry to multi
    and processes an Oracle LOB datatype (if present).

    Params:
        data: Geometry data from source.
        source_srid: SRID of the source geometry data.
        geom_type_family: Geometry type at hand.

    Returns:
        A WKTElement object of the geometry data.
    """
    if isinstance(data, cx_Oracle.LOB):
        data = data.read()

    if data is not None:
        p = wkt.loads(data)

        if geom_type_family == 'polygon':

            # needed to identify geometry type `collection`
            geoms = list(getattr(p, 'geoms', ''))

            if isinstance(p, Polygon):
                p = MultiPolygon([p])

            elif isinstance(p, MultiPolygon):
                pass

            # if geometry type `collection` just get one object
            elif len(geoms) > 1:

                for geo in geoms:

                    if isinstance(geo, MultiPolygon):
                        p = geo
                        break

                    if isinstance(geo, Polygon):
                        p = MultiPolygon([geo])
                        break

                    else: # not a valid geometry, skip
                        continue

                # if all geometies in `collection` are checked and no
                # valid geometry candidate type found, then set to None.
                if not isinstance(p, MultiPolygon) or isinstance(p, Polygon):
                    p = None

            else: # not a valid geometry
                p = None

        else:
            p = p

        p = WKTElement(p.wkt, srid=source_srid) if p else None
        return p


def define_dataset_name_for_azure_dbuser(
    context: Optional[Context] = None, dataset_name: Optional[str] = None
) -> str:
    """Defining the dataset name as part of the user Azure referentie database.

    Each dataset will have its own database user on Azure referentie
    database. The dataset name will be part of the user name. Like so:
    `EM4W-DATA-dataset-[ot|ap]-[name-of-dataset-in-snake-case]`

    If the dataset name is ommitted then the DAG id will be
    used. Assuming that the DAG is equals the dataset name.

    Params:
        dataset_name: Name of the dataset as known in the Amsterdam schema.
            Since the DAG name can be different from the dataset name, the latter
            can be explicity given. Only applicable for Azure referentie db connection.
            Defaults to None. If None, it will use the execution context to get the
            DAG id as surrogate. Assuming that the DAG id equals the dataset name
            as defined in Amsterdam schema.
        context: The context of run-time process that calls this function.
            Can be used to derive the DAG id as surrogate for database user
            if the dataset_name parameter is ommited.

    Returns:
        Name of database user to login on Azure referentie database.
    """
    if dataset_name is None:
        if context is not None:
            dataset_name = context["task_instance"].dag_id
        else:
            # both dataset_name and context are None
            raise AirflowException(
                """cannot define dataset name as part of Azure db user,
                both dataset_name and context are None."""
            )

    return dataset_name


def get_azure_token_with_msi() -> str:
    """Retreive Azure token for access referentie database.

    By using the Managed Idenity (MI) of Airflow instance on Azure AKS
    an access token is retrieved. The MI has to be a member of the Azure AD
    group that is used as a database user to login into the referentie database.

    Returns:
        Access token for database user based on the Managed Idenity (MI) of Airflow instance.
    """
    # database scope definition for getting Azure access token based on MI of Airflow.
    AZURE_RDBMS_RESOURCE_ID: Final = "https://ossrdbms-aad.database.windows.net/.default"

    credential = DefaultAzureCredential()
    scope = AZURE_RDBMS_RESOURCE_ID
    access_token = credential.get_token(scope).token

    return str(access_token)


def generate_dbuser_azure(dataset_name: str) -> str:
    """Generating database users for access referentie database on Azure.

    Database users names have the convention:
    `EM4W-DATA-dataset-[ot|ap]-[name-of-dataset-in-snake-case]`

    TODO: There is extra complexity where a seperate db user is present
    for datasets that contain tables that fall in more then one dataset layer.
    Like: https://schemas.data.amsterdam.nl/datasets/wegenbestand/zoneZwaarVerkeer\
        /breedOpgezetteWegen/
    However, there is one DAG that processes the data. Since there is one source and one topic
    (wegenbestand). Because each dataset correspondents to one user, there will be 2 separate
    users: wegenbestand and zoneZwaarVerkeer. This is needs to be figured out in more dept how to
    go about in the DAG.

    Params:
        dataset_name: Name of the dataset as known in the Amsterdam schema.
            The dataset name refers to an Azure AD group where the Airflow Managed Idenity (MI)
            is member of. Only applicable to Azure connections.

    Returns:
        database user name for accessing Azure referentie database

    """
    # depending on the Azure environment, the db user name will be containing `ot` or `ap`.
    otap_env: Optional[str] = os.environ.get("AZURE_OTAP_ENVIRONMENT")
    db_user: str = ""

    if otap_env is None:
        raise ValueError(
            "AZURE_OTAP_ENVIRONMENT is not set. \
                Cannot determine db user environment context. please check your values."
        )

    elif "ont" in otap_env or "tst" in otap_env:
        db_user = f"EM4W-DATA-dataset-ot-{to_snake_case(dataset_name)}-rw"

    elif "acc" in otap_env or "prd" in otap_env:
        db_user = f"EM4W-DATA-dataset-ap-{to_snake_case(dataset_name)}-rw"

    return db_user


def pg_params(
    dataset_name: Optional[str] = None,
    conn_id: str = "postgres_default",
    context: Optional[Context] = None,
    pg_params: bool = False,
) -> Any:
    """Add "stop on error" argument to connection string.

    This function will check if the environment where Airflow is running
    is Azure. Based on existence ENV VAR `AZURE_TENANT_ID`.
    If so, a different prepare of the connection string is executed.

    In Azure the connection to the referentie database will be set up by an
    AAD accesstoken and not basic authentication. The Managed Idenity (MI) of
    Airflow instance will be used to retrieve the token. The MI membership to
    the AAD group that represent the database user, ensures that Airflow can
    access the needed database table.

    On Azure the connection string is picked up from Key Vault. Its value is
    set like this:
    `postgresql://AAD-GROUP-NAME-REPLACED-BY-AIRFLOW@<databasehostname>:\
    PASSWORD-REPLACED-BY-AIRFLOW@<iphost>:<port>/<dbname>`

    The <AAD-GROUP-NAME-REPLACED-BY-AIRFLOW> will be used to replace it by
    the dataset_name as part of the username. And <PASSWORD-REPLACED-BY-AIRFLOW>
    by a token.

    Params:
        dataset_name: Name of the dataset as known in the Amsterdam schema.
            Since the DAG name can be different from the dataset name, the latter
            can be explicity given. Only applicable for Azure referentie db connection.
            Defaults to None. If None, it will use the execution context to get the
            DAG id as surrogate. Assuming that the DAG id equals the dataset name
            as defined in Amsterdam schema.
        conn_id: The database connection that is present .
            This is used on CloudVPS not on Azure.
        context: Execution context of the calling DAG / task.
        pg_params: Add extra postgress parameters when connection is
            used with a bash cmd. Defaults to False.

    Returns:
        A connection string with default params.
    """
    # If Azure
    # To cope with a different logic for defining the Azure referentie db user.
    # If CloudVPS is not used anymore, then this extra route can be removed.
    if os.environ.get("AZURE_TENANT_ID") is not None:

        if dataset_name is None and context is None:
            # since the pg_params cannot be execute with context, the dataset name is needed.
            raise AirflowException("please specify the dataset_name when calling pg_params.")

        # get connection string
        connection_uri = BaseHook.get_connection(conn_id).get_uri()

        # specifiy database user to login.
        # it refers to an AAD group where the Airflow Managed Idenity (MI) is member of.
        dataset_name = define_dataset_name_for_azure_dbuser(
            dataset_name=dataset_name, context=context
        )
        username = generate_dbuser_azure(dataset_name=dataset_name)

        # set correct db user and password in connection string
        if username is not None:
            connection_uri = connection_uri.replace("AAD-GROUP-NAME-REPLACED-BY-AIRFLOW", username)
            connection_uri = connection_uri.replace(
                "PASSWORD-REPLACED-BY-AIRFLOW", get_azure_token_with_msi()
            )
        else:
            raise AirflowException("please check dataset_name when calling pg_params.")

        return connection_uri

    # Else CloudVPS
    connection_uri = BaseHook.get_connection(conn_id).get_uri().split("?")[0]
    if pg_params:
        parameters = " ".join(
            [
                "-X",
                "--set",
                "ON_ERROR_STOP=1",
            ]
        )
        return f'"{connection_uri}" {parameters}'
    return connection_uri
