import datetime
import logging
import os
from typing import Iterable

import numpy as np
import pandas as pd
import sqlalchemy
from airflow.utils.context import Context
from common.db import DatabaseEngine
from sqlalchemy import Integer

LOGLEVEL: str = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log: logging.Logger = logging.getLogger(__name__)


def iter_valid_weeks(year: int) -> Iterable[int]:
    """Gives valid week in a year.

    Can be used to get max week number in a year.

    Params:
        year: The year to use to test if week number exists.

    Yields:
        A valid week number if found for that year.
    """
    for week in range(55):
        try:
            datetime.date.fromisocalendar(year, week, 1)
            yield week
        except ValueError:
            pass


def strip(text: str) -> str:
    """Removes leading and trailing whitespaces in data.

    Params:
        text: Data cell value from csv file to strip.

    Returns:
        Whitespaces stripped value.

    """
    try:
        return text.strip()
    except AttributeError:
        return text


def data_import_handhaving(csv_file: str, db_table_name: str, **context: Context) -> None:
    """Reads, converts and import csv data to table database.

    Params:
        csv_file: Name of handhaving file.csv to import.
        db_table_name: Name of database to import data to.

    Executes:
        SQL insert statement.

    Notes:
        Handhaving numbers for week 53, year 2021 must be
        assigned to year 2021.
        The column ois_week_number is not needed anymore
        so it's dropped before proces.

    """
    df: pd.DataFrame = pd.read_csv(
        csv_file,
        sep=";",
        names=[
            "organisatie",
            "type_interventie",
            "aantal",
            "week_nummer",
            "jaar",
            "ois_week_nummer",
        ],
        converters={
            "organisatie": strip,
            "type_interventie": strip,
        },
        header=0,
    )

    # loop trough all rows and calculate it's max iso week
    for index, row in df.iterrows():
        df.at[index, "max_week"] = max(iter_valid_weeks(row["jaar"]))

    # use max iso week to test if week_numer exceeds it
    # in that case, the week belongs to the previous year
    # according to iso weeks
    conditions = [df["week_nummer"] > df["max_week"]]
    # values for condition (== previous year)
    values = [df["jaar"] - 1]

    # apply outcome to column 'jaar'
    df["jaar"] = np.select(conditions, values, default=df["jaar"])

    # Remove unnecessary cols
    # TODO: ask OOV to remove ois_week_nummer
    # and ask OOV to set the correct year in the source
    df = df.drop(["ois_week_nummer"], axis=1)
    df = df.drop(["max_week"], axis=1)

    # group by the dimension columns and sum up the numbers
    df = df.groupby(
        ["organisatie", "type_interventie", "week_nummer", "jaar"], as_index=False
    ).sum()

    df.index.name = "id"

    engine: sqlalchemy.engine.Engine = DatabaseEngine(context=context).get_engine()
    df.to_sql(
        db_table_name,
        engine,
        dtype={"id": Integer(), "aantal": Integer(), "week_nummer": Integer(), "jaar": Integer()},
        if_exists="replace",
    )
