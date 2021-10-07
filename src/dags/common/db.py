from typing import Dict

import cx_Oracle
import dsnparse
from airflow import AirflowException
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class DatabaseEngine:
    """Construct the elements of the SQLAlchemy database engine."""

    def __init__(self, postgres_conn_id: str = "postgres_default"):
        """Initialize DatabaseEngine."""
        self.connection = PostgresHook().get_connection(postgres_conn_id)
        self.user = self.connection.login
        self.password = self.connection.password
        self.host = self.connection.host
        self.port = self.connection.port
        self.db = self.connection.schema


def get_postgreshook_instance(postgres_conn_id: str = "postgres_default") -> PostgresHook:
    """Return a postgreshook instance.

    So it can be used to get connection i.e.
    """
    connection = PostgresHook(postgres_conn_id=postgres_conn_id)
    return connection


def get_engine(postgres_conn_id: str = "postgres_default") -> Engine:
    """Construct the SQLAlchemy database engine."""
    connection = PostgresHook().get_connection(postgres_conn_id)
    user = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    db = connection.schema

    try:
        return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    except SQLAlchemyError as e:
        raise AirflowException(str(e)) from e


def get_ora_engine(oracle_conn_id: str = "oracle_default") -> Engine:
    """Get the oracle connection parameters.

    Some KPN's Oracle databases are provided in a container that cannot
        be accessed by a SID. A service_name parameter must be provided
        in the connection string to make the connection to the source DB.
    The Airflow OracleHook provides a method get_conn() that can be used to setup
        the connection. However in contrast to a sqlalchemy connection it cannot
        deal with processing Oracle's CLOB datatypes in combination with pandas.
        Therefor the sqlalchemy create_engine is still used to build up
        the connection.
    """
    connection = OracleHook().get_connection(oracle_conn_id)
    user = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    sid_ = (
        connection.extra_dejson.get("sid")
        if connection.extra_dejson.get("sid")
        else connection.schema
    )
    service_name_ = connection.extra_dejson.get("service_name")
    if service_name_:
        dns = cx_Oracle.makedsn(host, port, service_name=service_name_)
    else:
        dns = cx_Oracle.makedsn(host, port, sid=sid_)

    try:
        uri = f"oracle+cx_oracle://{user}:{password}@{dns}"
        return create_engine(uri, auto_convert_lobs=True)
    except SQLAlchemyError as e:
        raise AirflowException(str(e)) from e


def fetch_pg_env_vars(postgres_conn_id: str = "postgres_default") -> Dict[str, str]:
    """Get the Postgres Default DSN connection info as a dict."""
    # Need to get rid of trailing '&'
    # moved from to here due to circular import error
    from . import env

    stripped_env = env("AIRFLOW_CONN_POSTGRES_DEFAULT")[:-1]
    pg_conn_info = dsnparse.parse(stripped_env)
    return {
        "PGHOST": pg_conn_info.host,
        "PGPORT": str(pg_conn_info.port),
        "PGDATABASE": pg_conn_info.paths[0],
        "PGUSER": pg_conn_info.username,
        "PGPASSWORD": pg_conn_info.password,
    }
