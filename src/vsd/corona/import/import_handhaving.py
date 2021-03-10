import argparse
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import Integer

from common.db import get_engine
import logging

LOGLEVEL: str = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log: logging.Logger = logging.getLogger(__name__)


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
    df.index.name = "id"

    df = df.drop(['ois_week_nummer'], axis = 1)

    # list of conditions
    conditions = [
    (df['jaar'] == 2021) & (df['week_nummer'] == 53),
    (1 == 1)
    ]

    # list of the values we want to assign for each condition
    values = ['2020', df['jaar']]

    # apply outcome to column 'jaar'
    df['jaar'] = np.select(conditions, values)

    # group by the dimension columns and sum up the numbers
    df = df.groupby(['organisatie', 'type_interventie', 'week_nummer', 'jaar']).sum()

    engine: sqlalchemy.engine.Engine = get_engine()
    df.to_sql(
        "corona_handhaving_new",
        engine,
        dtype={
            "id": Integer(),
            "aantal": Integer(),
            "week_nummer": Integer(),
            "jaar": Integer()
        },
    )


if __name__ == "__main__":
    main()
