import datetime
import logging
import operator
import os
import pathlib
import re
import subprocess  # noqa: S404
from datetime import timedelta
from typing import Any, Final, Iterable, Iterator, Union
from xmlrpc.client import boolean

import dateutil.parser
import psycopg2
import shapefile
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import SHARED_DIR, default_args
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from psycopg2 import Date, Time, sql
from shapely.geometry import Polygon
from swift_hook import SwiftHook

logger = logging.getLogger(__name__)

dag_id = "parkeervakken"
postgres_conn_id = "parkeervakken_postgres"
TMP_TABLE_PREFIX = "temp"
# CONFIG = Variable.get(DAG_ID, deserialize_json=True)

TABLES: Final = {
    "BASE": f"{dag_id}_{dag_id}",
    "BASE_TEMP": f"{dag_id}_{dag_id}_temp",
    "REGIMES": f"{dag_id}_{dag_id}_regimes",
    "REGIMES_TEMP": f"{dag_id}_{dag_id}_regimes_temp",
}
WEEK_DAYS: Final = ["ma", "di", "wo", "do", "vr", "za", "zo"]


SQL_CREATE_TEMP_TABLES: Final = """
    -- ################## PARKEERVAKKEN #####################
    DROP TABLE IF EXISTS {{ params.base_table }}_temp;
    CREATE TABLE {{ params.base_table }}_temp (
        LIKE {{ params.base_table }} EXCLUDING INDEXES);
    ALTER TABLE {{ params.base_table }}_temp
        ALTER COLUMN geometry TYPE geometry(Polygon,28992)
        USING ST_Transform(geometry, 28992);

    -- ############## PARKEERVAKKEN_REGIMES ###################
    DROP TABLE IF EXISTS {{ params.base_table }}_regimes_temp;
    CREATE TABLE {{ params.base_table }}_regimes_temp (
        LIKE {{ params.base_table }}_regimes EXCLUDING INDEXES);
    DROP SEQUENCE IF EXISTS {{ params.base_table }}_regimes_temp_id_seq
       CASCADE;
    CREATE SEQUENCE {{ params.base_table }}_regimes_temp_id_seq
        OWNED BY {{ params.base_table }}_regimes_temp.id;
    ALTER TABLE {{ params.base_table }}_regimes_temp
        ALTER COLUMN id SET DEFAULT
        NEXTVAL('{{ params.base_table }}_regimes_temp_id_seq');
    ALTER TABLE {{ params.base_table }}_regimes_temp
        ADD COLUMN IF NOT EXISTS e_type_description VARCHAR;
    ALTER TABLE {{ params.base_table }}_regimes_temp ALTER COLUMN id
        SET DEFAULT nextval('{{ params.base_table }}_regimes_temp_id_seq'::regclass);
"""

SQL_RENAME_TEMP_TABLES: Final = """
    -- ################## PARKEERVAKKEN #####################
    DROP TABLE IF EXISTS {{ params.base_table }}_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}
        RENAME TO {{ params.base_table }}_old;
    DROP TABLE IF EXISTS {{ params.base_table }}_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}_temp
        RENAME TO {{ params.base_table }};
    ALTER INDEX IF EXISTS {{ params.base_table }}_temp_pkey1
        RENAME TO {{ params.base_table }}_pkey;
    CREATE INDEX {{ params.base_table }}_geometry_idx
        ON {{ params.base_table }} USING gist(geometry);
     ALTER TABLE {{ params.base_table }} DROP CONSTRAINT IF EXISTS
        {{ params.base_table }}_pkey;
    ALTER TABLE {{ params.base_table }} ADD CONSTRAINT
        {{ params.base_table }}_pkey PRIMARY KEY (id);

    -- ############## PARKEERVAKKEN_REGIMES ###################
    DROP TABLE IF EXISTS {{ params.base_table }}_regimes_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}_regimes
        RENAME TO {{ params.base_table }}_regimes_old;
    DROP TABLE IF EXISTS {{ params.base_table }}_regimes_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}_regimes_temp
        RENAME TO {{ params.base_table }}_regimes;
    ALTER INDEX IF EXISTS {{ params.base_table }}_regimes_temp_pkey1
        RENAME TO {{ params.base_table }}_regimes_pkey;
    CREATE INDEX {{ params.base_table }}_regimes_parent_id_idx
        ON {{ params.base_table }}_regimes(parent_id);
    DROP SEQUENCE IF EXISTS {{ params.base_table }}_regimes_id_seq;
    ALTER SEQUENCE IF EXISTS {{ params.base_table }}_regimes_temp_id_seq
        RENAME TO {{ params.base_table }}_regimes_id_seq;
    ALTER TABLE {{ params.base_table }}_regimes DROP CONSTRAINT IF EXISTS
        {{ params.base_table }}_regimes_pkey;
    ALTER TABLE {{ params.base_table }}_regimes ADD CONSTRAINT
        {{ params.base_table }}_regimes_pkey PRIMARY KEY (id);
"""

TMP_DIR: Final = f"{SHARED_DIR}/{dag_id}"

E_TYPES: Final = {
    "E1": "Parkeerverbod",
    "E2": "Verbod stil te staan",
    "E3": "Verbod fietsen en bromfietsen te plaatsen",
    "E4": "Parkeergelegenheid",
    "E5": "Taxistandplaats",
    "E6": "Gehandicaptenparkeerplaats",
    "E6a": "Gehandicaptenparkeerplaats algemeen",
    "E6b": "Gehandicaptenparkeerplaats op kenteken",
    "E7": "Gelegenheid bestemd voor het onmiddellijk laden en lossen van goederen",
    "E8": "Parkeergelegenheid alleen bestemd voor de voertuigcategorie of groep voertuigen die "
    "op het bord is aangegeven",
    "E9": "Parkeergelegenheid alleen bestemd voor vergunninghouders",
    "E10": (
        "Parkeerschijf-zone met verplicht gebruik van parkeerschijf, tevens parkeerverbod indien "
        "er langer wordt geparkeerd dan de parkeerduur die op het bord is aangegeven"
    ),
    "E11": "Einde parkeerschijf-zone met verplicht gebruik van parkeerschijf",
    "E12": "Parkeergelegenheid ten behoeve van overstappers op het openbaar vervoer",
    "E13": "Parkeergelegenheid ten behoeve van carpoolers",
}


def import_data(shp_file: str, ids: list) -> list[str]:
    """Import Shape File into database."""
    parkeervakken_sql = []
    regimes_sql = []
    duplicates = []
    logger.info("Processing: %s", shp_file)
    with shapefile.Reader(shp_file, encodingErrors="ignore") as shape:
        for row in shape:
            if int(row.record.PARKEER_ID) in ids:
                # Exclude dupes
                duplicates.append(row.record.PARKEER_ID)
                continue
            ids.append(int(row.record.PARKEER_ID))

            regimes = create_regimes(row=row)
            soort = "FISCAAL"
            if len(regimes) == 1 and isinstance(regimes, list):
                soort = regimes[0]["soort"]

            parkeervakken_sql.append(create_parkeervaak(row=row, soort=soort))
            regimes_sql += [
                (
                    row.record.PARKEER_ID,
                    mode["soort"],
                    mode["e_type"],
                    E_TYPES.get(mode["e_type"], ""),
                    mode["bord"],
                    mode["begin_tijd"].strftime("%H:%M"),
                    mode["eind_tijd"].strftime("%H:%M"),
                    mode["opmerking"],
                    "{" + ",".join(mode["dagen"]) + "}",
                    f"{mode['kenteken']}" if mode["kenteken"] else None,
                    f"{mode['begin_datum']}" if mode["begin_datum"] else None,
                    f"{mode['eind_datum']}" if mode["eind_datum"] else None,
                    row.record.AANTAL,
                )
                for mode in regimes
            ]

    # enitity: Parkeervakken (define cols for record inserts)
    table_col_names_parkeervakken = [
        "id",
        "buurtcode",
        "straatnaam",
        "soort",
        "type",
        "aantal",
        "geometry",
        "e_type",
    ]
    col_names_parkeervakken = sql.SQL(", ").join(
        sql.Identifier(n) for n in table_col_names_parkeervakken
    )
    query_base_parkeervakken = sql.SQL(
        """INSERT INTO {table_name} ({col_names})
            VALUES (%s,%s,%s,%s,%s,%s,ST_GeometryFromText(%s, 28992),%s)"""
    ).format(table_name=sql.Identifier(TABLES["BASE_TEMP"]), col_names=col_names_parkeervakken)

    # enitity: Parkeervakkenregimes (define cols for record inserts)
    table_col_names_parkeervakkenregimes = [
        "parent_id",
        "soort",
        "e_type",
        "e_type_description",
        "bord",
        "begin_tijd",
        "eind_tijd",
        "opmerking",
        "dagen",
        "kenteken",
        "begin_datum",
        "eind_datum",
        "aantal",
    ]
    col_names_parkeervakkenregimes = sql.SQL(", ").join(
        sql.Identifier(n) for n in table_col_names_parkeervakkenregimes
    )
    place_holders = sql.SQL(", ").join(
        sql.Placeholder() * len(table_col_names_parkeervakkenregimes)
    )
    query_base_parkeervakkenregimes = sql.SQL(
        "INSERT INTO {table_name} ({col_names}) VALUES ({values})"
    ).format(
        table_name=sql.Identifier(TABLES["REGIMES_TEMP"]),
        col_names=col_names_parkeervakkenregimes,
        values=place_holders,
    )

    hook = PostgresHook()
    conn = hook.get_conn()
    cursor = conn.cursor()
    with cursor as cur:
        if len(parkeervakken_sql):
            try:
                psycopg2.extras.execute_batch(cur, query_base_parkeervakken, parkeervakken_sql)
                conn.commit()
            except Exception as e:
                raise Exception(f"Failed to create parkeervakken: {str(e)[0:150]}")
        if len(regimes_sql):
            try:
                psycopg2.extras.execute_batch(cur, query_base_parkeervakkenregimes, regimes_sql)
                conn.commit()
            except Exception as e:
                raise Exception(f"Failed to create regimes: {str(e)[0:150]}")

        logger.info(
            "Created: %s parkeervakken and %s regimes", len(parkeervakken_sql), len(regimes_sql)
        )
    return duplicates


def download_latest_export_file(
    swift_conn_id: str, container: str, name_regex: str, *args: Iterable, **kwargs: Iterable
) -> Any:
    """Find latest export filename."""
    hook = SwiftHook(swift_conn_id=swift_conn_id)
    latest = None

    name_reg = re.compile(name_regex)
    for x in hook.list_container(container=container):
        if x["content_type"] != "application/zip":
            # Skip non-zip files.
            continue

        if name_reg.match(x["name"]) is None:
            # Search for latest file matching regex
            continue

        if latest is None:
            latest = x

        if dateutil.parser.parse(x["last_modified"]) > dateutil.parser.parse(
            latest["last_modified"]
        ):
            latest = x

    if latest is None:
        raise AirflowException("Failed to fetch objectstore listing.")
    zip_path = os.path.join(TMP_DIR, latest["name"])
    hook.download(container=container, object_id=latest["name"], output_path=zip_path)

    try:
        subprocess.run(  # noqa: S603 S607 S607
            ["unzip", "-o", zip_path, "-d", TMP_DIR], stderr=subprocess.PIPE, check=True
        )
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Failed to extract zip: {e.stderr}")

    logger.info("downloading %s", latest["name"])
    return latest["name"]


def run_imports(*args: Iterable, **kwargs: Iterable) -> None:
    """Run imports for all files in zip that match date."""
    ids: list[str] = []
    for shp_file in source.glob("*.shp"):
        duplicates = import_data(str(shp_file), ids)

        if len(duplicates):
            logger.warning("Duplicates found: %s", ", ".join(duplicates))


args = default_args.copy()

# TODO: Re-design this DAG: Separate ingestion logic form this DAG definition
# clean up code, make use of standard ways to do task in this DAG, make more
# doc strings and comments of the functions used.
with DAG(
    dag_id,
    default_args=args,
    description="Parkeervakken",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    source = pathlib.Path(TMP_DIR)

    # temporary fix: Older downloads must be cleaned up before new download get executed
    # to avoid importing old data since there is no removal of old data yet.
    mk_tmp_dir = BashOperator(
        task_id="mk_tmp_dir", bash_command=f"rm -rf {TMP_DIR} && mkdir -p {TMP_DIR}"
    )

    download_and_extract_zip = PythonOperator(
        task_id="download_and_extract_zip",
        python_callable=download_latest_export_file,
        op_kwargs={
            "swift_conn_id": "objectstore_parkeervakken",
            "container": "tijdregimes",
            "name_regex": r"^nivo_\d+\.zip",
        },
    )

    download_and_extract_nietfiscaal_zip = PythonOperator(
        task_id="download_and_extract_nietfiscaal_zip",
        python_callable=download_latest_export_file,
        op_kwargs={
            "swift_conn_id": "objectstore_parkeervakken",
            "container": "Parkeervakken",
            "name_regex": r"^\d+\_nietfiscaal\.zip",
        },
    )

    create_temp_tables = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLES,
        params={"base_table": f"{dag_id}_{dag_id}"},
    )

    run_import_task = PythonOperator(
        task_id="run_import_task",
        python_callable=run_imports,
        dag=dag,
    )

    count_check = PostgresMultiCheckOperator(
        task_id="count_check",
        checks=[
            COUNT_CHECK.make_check(
                check_id="non_zero_check",
                pass_value=10,
                params={"table_name": f"{dag_id}_{dag_id}_temp"},
                result_checker=operator.ge,
            )
        ],
    )

    rename_temp_tables = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLES,
        params={"base_table": f"{dag_id}_{dag_id}"},
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

(
    mk_tmp_dir
    >> download_and_extract_zip
    >> download_and_extract_nietfiscaal_zip
    >> create_temp_tables
    >> run_import_task
    >> count_check
    >> rename_temp_tables
    >> grant_db_permissions
)


# Internals
def create_parkeervaak(row: shapefile.ShapeRecord, soort: Union[None, str] = None) -> tuple:
    """Creating a parkeervak record."""
    geometry = "''"
    if row.shape.shapeTypeName == "POLYGON":
        geometry = str(Polygon(row.shape.points))

    sql = (
        row.record.PARKEER_ID,
        row.record.BUURTCODE,
        row.record.STRAATNAAM.replace("'", "\\'"),
        row.record.SOORT or soort,
        row.record.TYPE or "",
        row.record.AANTAL,
        geometry,
        row.record.E_TYPE or "",
    )
    return sql


def create_regimes(row: shapefile.ShapeRecord) -> Union[list[Any], dict[Any, Any]]:
    """Creating a parkeervak regime record."""
    output = []

    base_data = {
        "parent_id": row.record.PARKEER_ID or "",
        "soort": "FISCAAL",
        "e_type": "",
        "bord": "",
        "begin_tijd": datetime.time(0, 0),
        "eind_tijd": datetime.time(23, 59),
        "opmerking": row.record.OPMERKING or "",
        "dagen": WEEK_DAYS,
        "kenteken": None,
        "begin_datum": None,
        "eind_datum": None,
    }

    modes = get_modes(row)
    if len(modes) == 0:
        # No time modes, but could have full override.
        parkeervak_with_no_modes = base_data.copy()
        parkeervak_with_no_modes.update(
            {
                "soort": row.record.SOORT or "FISCAAL",
                "bord": row.record.BORD or "",
                "e_type": row.record.E_TYPE or "",
                "kenteken": row.record.KENTEKEN,
            }
        )
        return [parkeervak_with_no_modes]

    # get index positions of the non-TVM modes so it
    # can be used to idenity the very first and very last
    # record. That is important so the surrounding time range
    # records can be added dependent on the start and end times.
    # group 1: are the modes that belong to the record with begintijd1.
    # group 2: are the modes that belong to the record with begintijd2 (if present).
    # group 1 and 2 can be a separate group with their own surrounding times slots.
    indexes_of_no_tvm_modes_group1 = [
        idx
        for idx, mode in enumerate(modes)
        if not mode.get("ind_tvm") and not mode.get("ind_separate_group")
    ]
    indexes_of_no_tvm_modes_group2 = [
        idx
        for idx, mode in enumerate(modes)
        if not mode.get("ind_tvm") and mode.get("ind_separate_group")
    ]

    for index, mode in enumerate(modes):

        # since there can be multiple modes
        # per row with each different days
        # we need to run per mode the days
        # calculation.
        days = days_from_row(row)

        # start off with start time and end time for first non-TVM mode found.
        if not mode.get("ind_tvm") and indexes_of_no_tvm_modes_group1:
            if index == min(indexes_of_no_tvm_modes_group1):
                mode_start = datetime.time(0, 0)
                mode_end = datetime.time(23, 59)
        if not mode.get("ind_tvm") and indexes_of_no_tvm_modes_group2:
            if index == min(indexes_of_no_tvm_modes_group2):
                mode_start = datetime.time(0, 0)
                mode_end = datetime.time(23, 59)

        # fictional first time range record for mode (defaults to 00:00 - xxx)
        if not mode.get("ind_tvm"):
            if mode.get("begin_tijd", datetime.time(0, 0)) > mode_start:
                # Time bound. Start of the day mode.
                sod_mode = base_data.copy()
                sod_mode["dagen"] = days
                sod_mode["begin_tijd"] = mode_start
                sod_mode["eind_tijd"] = remove_a_minute(mode["begin_tijd"])
                output.append(sod_mode)

            # time range record for mode as found in source
            mode_data = base_data.copy()
            mode_data.update(mode)
            mode_data["dagen"] = days
            output.append(mode_data)
            mode_start = add_a_minute(mode["eind_tijd"])

            # check if the non-TVM mode is the last in its group
            # and if so then add the fictional last record.
            ind_add_fictional_time_range_record = False

            if indexes_of_no_tvm_modes_group1:
                if index == max(indexes_of_no_tvm_modes_group1):
                    ind_add_fictional_time_range_record = True
            if indexes_of_no_tvm_modes_group2:
                if index == max(indexes_of_no_tvm_modes_group2):
                    ind_add_fictional_time_range_record = True

            # fictional last time range record for mode (defaults to xxx - 23:59)
            if (
                mode.get("eind_tijd", datetime.time(23, 59)) < mode_end
                and ind_add_fictional_time_range_record
            ):
                # Time bound. End of the day mode.
                eod_mode = base_data.copy()
                eod_mode["dagen"] = days
                eod_mode["begin_tijd"] = add_a_minute(mode["eind_tijd"])
                output.append(eod_mode)

        if mode.get("ind_tvm"):

            mode_start = datetime.time(0, 0)
            mode_end = datetime.time(23, 59)

            # start of the TVM (tijdelijke verkeersmaatregel) record
            # the TVM is cut up in days between start and end date;
            # so each day will be added as a record.
            # The first record will hold the very first start time
            # when the TVM is applicable. The last record will hold
            # the end time of the time at the TVM will be valid.
            sod_mode = base_data.copy()
            sod_mode["dagen"] = days
            sod_mode["begin_tijd"] = mode.get("begin_tijd", mode_start)
            sod_mode["eind_tijd"] = (
                mode.get("eind_tijd", mode_end)
                if mode.get("eind_datum") == mode.get("begin_datum")
                else mode_end
            )
            sod_mode["begin_datum"] = mode.get("begin_datum")
            sod_mode["eind_datum"] = (
                mode.get("eind_datum")
                if mode.get("eind_datum") == mode.get("begin_datum")
                else mode.get("begin_datum")
            )
            output.append(sod_mode)

            # adding the in-between-dates of the TVM; each day will be an own record.
            if mode.get("eind_datum") > mode.get("begin_datum"):
                days_to_add = daterange(mode["begin_datum"], mode["eind_datum"])
                # skip the first result since that is the begin_datum
                # and already added as a record by `sod_mode` above.
                next(days_to_add)
                for day in days_to_add:
                    in_between_data = base_data.copy()
                    in_between_data["dagen"] = days
                    in_between_data["begin_datum"] = day
                    in_between_data["eind_datum"] = day
                    in_between_data["begin_tijd"] = mode_start
                    in_between_data["eind_tijd"] = mode_end
                    output.append(in_between_data)
                # delete the last result since that is the eind_datum
                # and will already added as a record by `eod_mode` below.
                del output[-1]

            # end of the TVM (tijdelijke verkeersmaatregel) record.
            if mode.get("eind_datum") > mode.get("begin_datum"):
                # Time bound. End of the day mode.
                eod_mode = base_data.copy()
                eod_mode["dagen"] = days
                eod_mode["begin_tijd"] = mode_start
                eod_mode["eind_tijd"] = mode.get("eind_tijd", mode_end)
                eod_mode["begin_datum"] = mode.get("eind_datum")
                eod_mode["eind_datum"] = mode.get("eind_datum")
                output.append(eod_mode)

    return output


def add_a_minute(time: Time) -> Time:
    """Adding minute to time span of a parkeervakregime."""
    return (
        datetime.datetime.combine(datetime.date.today(), time) + datetime.timedelta(minutes=1)
    ).time()


def remove_a_minute(time: Time) -> Time:
    """Removing minute to time span of a parkeervakregime."""
    return (
        datetime.datetime.combine(datetime.date.today(), time) - datetime.timedelta(minutes=1)
    ).time()


def daterange(start_date: datetime.date, end_date: datetime.date) -> Iterator:
    """Calculating in-between-dates between two dates.

    This function is used for addding the dates between a
    TVM (tijdelijke verkeersmaatregel) period, that is is defined
    as a single start and end date.
    For each in-between-date a record is created.
    """
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(days=n)


def get_modes(row: shapefile.ShapeRecord) -> list:
    """Get the parkeerregimes instances (a.k.a. modes) of a parkeervak."""
    modes = []

    base = {
        "soort": row.record.SOORT or "FISCAAL",
        "bord": row.record.BORD or "",
        "e_type": row.record.E_TYPE or "",
        "kenteken": row.record.KENTEKEN,
    }
    if any(
        [
            row.record.TVM_BEGINT,
            row.record.TVM_EINDT,
            row.record.TVM_BEGIND,
            row.record.TVM_EINDD,
            row.record.TVM_OPMERK,
        ]
    ):
        # TVM (tijdelijke verkeersmaatregel)
        tvm_mode = base.copy()
        tvm_mode.update(
            {
                "soort": row.record.E_TYPE or "FISCAAL",
                "begin_datum": row.record.TVM_BEGIND or "",
                "eind_datum": row.record.TVM_EINDD or "",
                "opmerking": (row.record.TVM_OPMERK or "").replace("'", "\\'"),
                "begin_tijd": parse_time(row.record.TVM_BEGINT, datetime.time(0, 0)),
                "eind_tijd": parse_time(row.record.TVM_EINDT, datetime.time(23, 59)),
                "ind_tvm": True,
            }
        )
        modes.append(tvm_mode)

    if any([row.record.BEGINTIJD1, row.record.EINDTIJD1]):
        begin_tijd = parse_time(row.record.BEGINTIJD1, datetime.time(0, 0))
        eind_tijd = parse_time(row.record.EINDTIJD1, datetime.time(23, 59))
        if begin_tijd < eind_tijd:
            x = base.copy()
            x.update({"begin_tijd": begin_tijd, "eind_tijd": eind_tijd})
            modes.append(x)
        else:
            # Mode: 20:00, 06:00
            x = base.copy()
            x.update({"begin_tijd": datetime.time(0, 0), "eind_tijd": eind_tijd})
            y = base.copy()
            y.update({"begin_tijd": begin_tijd, "eind_tijd": datetime.time(23, 59)})
            modes.append(x)
            modes.append(y)
    if any([row.record.BEGINTIJD2, row.record.EINDTIJD2]):
        x = base.copy()
        x.update(
            {
                "begin_tijd": parse_time(row.record.BEGINTIJD2, datetime.time(0, 0)),
                "eind_tijd": parse_time(row.record.EINDTIJD2, datetime.time(23, 59)),
                "ind_separate_group": ind_multiple_different_day_period(row),
            }
        )
        modes.append(x)
    return modes


def ind_multiple_different_day_period(row: shapefile.ShapeRecord) -> boolean:
    """Returns the number of day selections with one parkeervak enitity.

    Within a parkeervak entity there could be multiple applicable days/dayperiods
    that have each their own time slots. These time slots belong to their own
    day/dayperiod. I.e. ma_vr: from 0900 till 1700, zo: from 1300 till 1600.
    This function tells if that is the case.
    """
    num_of_day_selections = [getattr(row.record, day.upper()) for day in WEEK_DAYS] + [
        row.record.MA_VR,
        row.record.MA_ZA,
    ]
    if sum(num_of_day_selections) > 1:
        return True
    return False


def days_from_row(row: shapefile.ShapeRecord) -> Iterable:
    """Parse week days from row.

    NOTE: Since a single row can hold two start and endtimes, where the first `true` value
    correspondents to the `BEGINTIJD1 and EINDTIJD1` attributes and the second `true` value
    to the `BEGINTIJD2 and EINDTIJD2`, we need to set the first true value for found days
    to false, so the next mode (BEGINTIJD2 and EINDTIJD2) can be bound to the second true value.
    """
    if row.record.MA_VR:
        # Monday incl Friday
        days = WEEK_DAYS[:5]
        # If there are more then one days registered within parkeervak,
        # then the first register applies to `BEGINTIJD1 and EINDTIJD1`
        # and these days are then set to false for the next run. So the
        # second day register are applied to `BEGINTIJD2 and EINDTIJD2`.
        if ind_multiple_different_day_period(row):
            row.record.MA_VR = False

    elif row.record.MA_ZA:
        # Monday incl Saturday
        days = WEEK_DAYS[:6]
        # If there are more then one days registered within parkeervak,
        # then the first register applies to `BEGINTIJD1 and EINDTIJD1`
        # and these days are then set to false for the next run. So the
        # second day register are applied to `BEGINTIJD2 and EINDTIJD2`.
        if ind_multiple_different_day_period(row):
            row.record.MA_ZA = False

    elif not any([getattr(row.record, day.upper()) for day in WEEK_DAYS]):
        # All days apply
        days = WEEK_DAYS

    else:
        # One day permit
        days = [day for day in WEEK_DAYS if getattr(row.record, day.upper()) is not False]
        setattr(row.record, days[0].upper(), False)

    return days


def parse_time(value: str, default: Date) -> Date:
    """Parse time or return default."""
    if value is not None:
        if value == "24:00":
            value = "23:59"
        if value.startswith("va "):
            value = value[3:]

        try:
            parsed = dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            pass
        else:
            return parsed.time()
    return default
