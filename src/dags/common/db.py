import dsnparse
from airflow import AirflowException
from airflow.hooks.oracle_hook import OracleHook
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from airflow.hooks.postgres_hook import PostgresHook


class DatabaseEngine:
    """Construct the elements of the SQLAlchemy database engine"""

    def __init__(self, postgres_conn_id="postgres_default"):
        self.connection = PostgresHook().get_connection(postgres_conn_id)
        self.user = self.connection.login
        self.password = self.connection.password
        self.host = self.connection.host
        self.port = self.connection.port
        self.db = self.connection.schema


def get_engine(postgres_conn_id="postgres_default") -> Engine:
    """Construct the SQLAlchemy database engine"""
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


def get_ora_engine(oracle_conn_id="oracle_default") -> Engine:
    connection = OracleHook().get_connection(oracle_conn_id)
    user = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    db = connection.schema

    try:
        uri = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{db}?encoding=UTF-8&nencoding=UTF-8"  # noqa: E501
        return create_engine(uri, auto_convert_lobs=True)
    except SQLAlchemyError as e:
        raise AirflowException(str(e)) from e


def fetch_pg_env_vars(postgres_conn_id="postgres_default"):
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
