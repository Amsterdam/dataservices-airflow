import json
import logging
import os
from typing import Any, Final

import pandas as pd
from airflow.utils.context import Context
from common.db import DatabaseEngine
from sqlalchemy import Date, Integer, String

LOGLEVEL: Final = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)

subset_amterdam: list = []


def data_load(file: str) -> list[Any]:
    """Loading data into a Python dictionary.

    Params:
        file: Name of the file to read into Python dictionary.

    Returns:
        List of data records filtered to scope gemeente Amsterdam.
    """
    log.info("read file %s", file)
    with open(file) as data:
        data_dict = json.load(data)
        output = [record for record in data_dict if record["Municipality_code"] == "GM0363"]
    return output


def data_import_gevallen_opnames(
    source_data_gevallen: str, source_data_ziekenhuis: str, db_table_name: str, **context: Context
) -> None:
    """Starting import.

    In case of RIVM data we must combine data from
    two source files.

    Params:
        source_data_gevallen: Path to Corona occurrences source file.
        source_data_ziekenhuis: Path to Corona hospital admissions source file.
        db_table_name: Target table to write data to.

    Executes:
        Insert SQL statement to load the data into the database.
    """
    # read data as Python list
    gevallen = data_load(source_data_gevallen)
    ziekenhuis = data_load(source_data_ziekenhuis)

    # read data into pandas
    gevallen_df = pd.DataFrame(gevallen)
    ziekenhuis_df = pd.DataFrame(ziekenhuis)

    # lower all column name
    gevallen_df.columns = gevallen_df.columns.str.lower()
    ziekenhuis_df.columns = ziekenhuis_df.columns.str.lower()

    # group data first before joining
    result_gevallen = (
        gevallen_df.groupby(
            ["date_of_publication", "municipality_code", "municipality_name", "province"]
        )
        .agg(
            total_reported=("total_reported", "sum"),
            deceased=("deceased", "sum"),
            date=("date_of_publication", "max"),
        )
        .reset_index()
    )

    result_ziekenhuis = (
        ziekenhuis_df.groupby(["date_of_statistics", "municipality_code", "municipality_name"])
        .agg(hospital_admission=("hospital_admission", "sum"), date=("date_of_statistics", "max"))
        .reset_index()
    )

    # join the two datasets
    result = result_gevallen.join(
        result_ziekenhuis.set_index("date"), on="date", lsuffix="", rsuffix="_ignore"
    )

    # drop unused cols
    result = result.drop(
        columns=[
            "date",
            "date_of_statistics",
            "municipality_code_ignore",
            "municipality_name_ignore",
        ]
    )

    log.info("Starting import")
    engine = DatabaseEngine(context=context).get_engine()

    result.to_sql(
        db_table_name,
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
        if_exists="replace",
    )

    log.info("Data loaded into DB")
