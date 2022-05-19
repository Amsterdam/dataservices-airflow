import json
import logging
from datetime import datetime
from math import ceil
from typing import Any, Generator

import requests
from common import make_hash
from common.db import get_engine
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

# max_rows represents the numer of records retured per page
# and therefore also the number of records processed.
# The source max is 1_000.
max_rows_to_batch: int = 100

# the SRID for geometry data as present in source.
# This is used to let sqlalchemy know what the SRID is
# during insert of the data. 28992 = new RD (rijksdriehoek)
SRID = 28992

engine = get_engine()
session = Session(bind=engine)
logger = logging.getLogger(__name__)


def create_date_ranges(delta_in_months: int = 12) -> tuple:
    """Calculate the date window for the data selection.

    The source API support only 1_000 record output. So a date window
    is used to get range of current date minus defined months.

    :param delta_in_months: Holds the number of months to look back. Defaults to 12.

    :return: The start and end date of the date window.
    """
    end_date_range = datetime.now()
    start_date_range = datetime.now() - relativedelta(months=delta_in_months)
    return start_date_range.strftime("%Y-%m-%d"), end_date_range.strftime("%Y-%m-%d")


# the date window to collect data
start_date, end_date = create_date_ranges()


def set_request(max_rows: int = max_rows_to_batch, start_record_position: int = 1) -> str:
    """Construct the request variables.

    For different purposes like getting the number of total records
    to process or get the records content, the request is constructed
    depening on purpose of the calling function.

    :param max_rows: The maximum number of rows to return per page. Defaults
     to what is globally defined in max_rows_to_batch.

    :param start_record_position: The start record to get from request. If there
      are more results then the number definied in max_rows_to_batch, then the results
      will be chunked into pages. For the next page, you specifiy the (next)
      start_record_position. For instance 150 results, 100 max, then for the second page
      the start_record_position will be 101.

    :return: Request URL
    """
    api_url = (
        "https://repository.overheid.nl/sru?"
        f"&query=c.product-area==lokalebekendmakingen"
        f"%20AND%20dt.modified%3E={start_date}"
        f"%20AND%20dt.modified%3C={end_date}"
        f"%20AND%20dt.creator=%22Amsterdam%22&maximumRecords={max_rows}"
        f"&httpAccept=application/json"
        f"&startRecord={start_record_position}"
    )
    return api_url


def num_of_records_to_process() -> Any:
    """Get number of records to process.

    The number of records outputed by the source API
    is given as a element. This is used in combination with
    the set maximum number of records per page to ingest all
    data. The source has a limit of 1_000 per page.

    :return: Number of total records to process.
    """
    # set the max rows to zero since, we are at this moment only
    # interested in the amount of records not the actual records itself.
    max_rows = 1
    request_url = set_request(max_rows)
    response = requests.get(request_url).text
    result = json.loads(response)
    num_of_records = result["searchRetrieveResponse"]["numberOfRecords"]
    return num_of_records


def get_data_records() -> Generator:
    """Get data records per page.

    The number of records per page is defined in the global variable
    max_rows_to_batch

    :yield: Data records per page
    """
    total_num_records = num_of_records_to_process()
    total_num_iterations = ceil(total_num_records / max_rows_to_batch)
    # If there are more results then the number definied in max_rows_to_batch, then
    # the results will be chunked into pages. For the next page, you specifiy
    # the (next) start_record_position. For instance 150 results, 100 max, then for the
    # second page the start_record_position will be 101.
    start_record_position = 1

    for iter_num in range(total_num_iterations + 1):
        logger.info("starting batch: %d, total batches: %d.", iter_num, total_num_iterations)

        if iter_num != 0 and start_record_position < total_num_records:
            start_record_position = max_rows_to_batch * iter_num + 1
            if start_record_position > total_num_records:
                # end of data reached. Stop loop.
                logger.info("Import all batches are done. Closing up.")
                break

        request_url = set_request(start_record_position=start_record_position)
        response = requests.get(request_url).text
        result = json.loads(response)

        for records in result["searchRetrieveResponse"]["records"].values():
            batch_of_rows = []

            for record in records:
                record_dict = {}

                for record_content in record["recordData"].values():
                    for orginalData in record_content["originalData"].values():

                        owmskern_section = orginalData["owmskern"]
                        owmsmantel_section = orginalData["owmsmantel"]
                        tpmeta_section = orginalData["tpmeta"]

                        record_dict["id"] = make_hash(owmskern_section["identifier"], 6)

                        if geometry := tpmeta_section.get("geometrie"):
                            record_dict["geometry"] = f"SRID={SRID};{geometry}"
                        else:
                            record_dict["geometry"] = None
                            logger.info(
                                "Abort ingestion of %s because no geometry present.",
                                owmskern_section["identifier"],
                            )
                            break  # if no geometry do not process

                        record_dict["officielebekendmakingen_id"] = owmskern_section["identifier"]
                        record_dict["categorie"] = owmskern_section["type"]["$"]

                        if type(owmsmantel_section["subject"]) == list:
                            for item in owmsmantel_section["subject"]:
                                list_items = []
                                list_items.append(item["$"])
                                record_dict["onderwerp"] = ",".join(list_items)
                        else:
                            record_dict["onderwerp"] = owmsmantel_section["subject"]["$"]

                        record_dict["titel"] = owmskern_section["title"]
                        record_dict["beschrijving"] = owmsmantel_section["description"]
                        record_dict["url"] = tpmeta_section["bronIdentifier"]
                        record_dict["postcodehuisnummer"] = tpmeta_section.get(
                            "postcodeHuisnummer", None
                        )
                        record_dict["plaats"] = tpmeta_section.get("afgeleideGemeente", None)
                        record_dict["straat"] = tpmeta_section.get("straat", None)
                        record_dict["datum_tijdstip"] = tpmeta_section[
                            "geldigheidsperiode_startdatum"
                        ]
                        record_dict["datum_tijdstip"] = tpmeta_section[
                            "datumTijdstipWijzigingWork"
                        ]
                        record_dict["overheidsorganisatie"] = owmskern_section["creator"]["$"]

                        batch_of_rows.append(record_dict)

        yield batch_of_rows


def import_data_batch(tablename: str) -> None:
    """Batched INSERT statements via the ORM "bulk", using dictionaries.

    :tablename: Name of the target table to ingest the data in.

    :executes: SQL insert bulk statement
    """
    Base = declarative_base()

    class Bekendmakingen(Base):  # type: ignore
        __tablename__ = tablename
        __table_args__ = ({"autoload": True, "autoload_with": engine},)

    for batch in get_data_records():

        session.bulk_insert_mappings(Bekendmakingen, batch)
        session.commit()
