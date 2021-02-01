import json
import logging
import os
import argparse

import pandas as pd
from sqlalchemy import Integer, Date, String
from common.db import get_engine

from provenance_rename_operator import ProvenanceRenameOperator

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)

subset_amterdam = list()


def data_parser(file):
1

import json

2

import logging

3

import os

4

import argparse

5

​

6

import pandas as pd

7

from sqlalchemy import Integer, Date, String

8

from common.db import get_engine

9

​

10

from provenance_rename_operator import ProvenanceRenameOperator

11

​
    """ loading data and parsing json """
    log.info(f"read file {file}")
    with open(file, "r") as data:
        for row in data:
            jsonized = json.loads(row)
            yield from jsonized


def data_iter(file):
    """ getting data and filtering on Amsterdam """
    data = data_parser(file)
    for row in data:
        # selection only Amsterdam
        if row["Municipality_code"] == "GM0363":
            yield row


def main():
    """ starting import """

    # get argument
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json", type=str, help="JSON file to process")
    args = parser.parse_args()

    # transform data into dataframe
    for row in data_iter(args.input_json):
        subset_amterdam.append(row)
        result = pd.DataFrame(subset_amterdam)

    log.info(f"Starting import {args.input_json}")

    # set primary id
    result.index.name = "id"
    engine = get_engine()
    # lower all column names
    result.columns = result.columns.str.lower()

    result.to_sql(
        "corona_gevallen_new",
        engine,
        dtype={
            "id": Integer(),
            "date_of_publication": Date(),
            "municipality_code": String(),
            "municipality_name": String(),
            "province": String(),
            "total_reported": Integer(),
            "hospital_admission": Integer(),
            "deceased": Integer(),
        },
    )
    log.info("Data loaded into DB")

    ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name="corona",
        prefix_table_name="corona_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    ).execute()
    log.info("Renamed columns based on provenance.")


if __name__ == "__main__":
    main()
