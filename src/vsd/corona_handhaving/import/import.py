import argparse
import os
import pandas as pd
from sqlalchemy import Integer

from common.db import get_engine
import logging

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)


def strip(text):
    """ removes leading and trailing whitespaces in data """
    try:
        return text.strip()
    except AttributeError:
        return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=str, help="CSV file to process")
    args = parser.parse_args()
    df = pd.read_csv(
        args.input_csv,
        sep=";",
        names=["organisatie", "type_interventie", "aantal", "week_nummer"],
        converters={"organisatie": strip, "type_interventie": strip,},
    )
    df.index.name = "id"
    engine = get_engine()
    df.to_sql(
        "corona_handhaving_new",
        engine,
        dtype={"id": Integer(), "aantal": Integer(), "week_nummer": Integer()},
    )


if __name__ == "__main__":
    main()
