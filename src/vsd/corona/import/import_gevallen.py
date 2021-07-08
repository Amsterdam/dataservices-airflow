import argparse
import json
import logging
import os
from typing import Final

import pandas as pd
from common.db import get_engine
from provenance_rename_operator import ProvenanceRenameOperator
from sqlalchemy import Date, Integer, String

LOGLEVEL: Final = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)

subset_amterdam = list()


def data_parser(file):
    """loading data and parsing json"""
    log.info(f"read file {file}")
    with open(file) as data:
        for row in data:
            jsonized = json.loads(row)
            yield from jsonized


def data_iter(file):
    """getting data and filtering on Amsterdam"""
    data = data_parser(file)
    for row in data:
        # selection only Amsterdam
        if row["Municipality_code"] == "GM0363":
            yield row


def main():
    """starting import"""

    # get argument
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json", type=str, help="JSON file to process")
    args = parser.parse_args()

    # transform data into dataframe
    for row in data_iter(args.input_json):
        subset_amterdam.append(row)
        result = pd.DataFrame(subset_amterdam)

    # lower all column names
    result.columns = result.columns.str.lower()

    # aggregating numbers per group
    result = (
        result.groupby(
            ["date_of_publication", "municipality_code", "municipality_name", "province"]
        )
        .agg(
            total_reported=("total_reported", "sum"),
            hospital_admission=("hospital_admission", "sum"),
            deceased=("deceased", "sum"),
        )
        .reset_index()
    )

    log.info(f"Starting import {args.input_json}")

    engine = get_engine()

    result.to_sql(
        "corona_gevallen_new",
        engine,
        dtype={
            "index": Integer(),
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
    ).execute(None)
    log.info("Renamed columns based on provenance.")


if __name__ == "__main__":
    main()
