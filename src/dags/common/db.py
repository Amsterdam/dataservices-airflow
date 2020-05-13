from airflow import AirflowException
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from airflow.hooks.postgres_hook import PostgresHook


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
