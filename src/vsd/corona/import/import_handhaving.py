import argparse
import datetime
import logging
import os
from typing import Iterable

import numpy as np
import pandas as pd
import sqlalchemy
from common.db import get_engine
from sqlalchemy import Integer

LOGLEVEL: str = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log: logging.Logger = logging.getLogger(__name__)


def iter_valid_weeks(year: int) -> Iterable[int]:
    """gives valid week in a year
    Can be used to get max week number in a year

    Args:
        year: the year to use to test if week number exists

    Yields:
        a valid week number if found for that year
    """
    for week in range(55):
        try:
            datetime.date.fromisocalendar(year, week, 1)
            yield week
        except ValueError:
            pass


def strip(text: str) -> str:
    """removes leading and trailing whitespaces in data

    Args:
        text: Data cell value from csv file to strip

    Returns:
        whitespaces stripped value

    """
    try:
        return text.strip()
    except AttributeError:
        return text


def main() -> None:
    """Reads, converts and import csv data to table database

    Executes:
        SQL insert statement

    Notes:
        Handhaving numbers for week 53, year 2021 must be
        assigned to year 2021.
        The column ois_week_number is not needed anymore
        so it's dropped before proces.

    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=str, help="CSV file to process")
    args: argparse.Namespace = parser.parse_args()
    df: pd.DataFrame = pd.read_csv(
        args.input_csv,
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

    engine: sqlalchemy.engine.Engine = get_engine()
    df.to_sql(
        "corona_handhaving_new",
        engine,
        dtype={"id": Integer(), "aantal": Integer(), "week_nummer": Integer(), "jaar": Integer()},
    )


if __name__ == "__main__":
    main()
