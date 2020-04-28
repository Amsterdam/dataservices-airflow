import os

from airflow import AirflowException
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


def get_engine() -> Engine:
    """Construct the SQLAlchemy database engine"""
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    try:
        return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    except SQLAlchemyError as e:
        raise AirflowException(str(e)) from e
