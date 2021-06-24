import datetime
import operator
import os
import pathlib
import re
import subprocess
from typing import Final

import dateutil.parser
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
from shapely.geometry import Polygon
from swift_hook import SwiftHook

dag_id = "parkeervakken"
postgres_conn_id = "parkeervakken_postgres"

# CONFIG = Variable.get(DAG_ID, deserialize_json=True)

TABLES: Final = dict(
    BASE=f"{dag_id}_{dag_id}",
    BASE_TEMP=f"{dag_id}_{dag_id}_temp",
    REGIMES=f"{dag_id}_{dag_id}_regimes",
    REGIMES_TEMP=f"{dag_id}_{dag_id}_regimes_temp",
)
WEEK_DAYS: Final = ["ma", "di", "wo", "do", "vr", "za", "zo"]


SQL_CREATE_TEMP_TABLES: Final = """
    DROP TABLE IF EXISTS {{ params.base_table }}_temp;
    CREATE TABLE {{ params.base_table }}_temp (
        LIKE {{ params.base_table }} INCLUDING ALL);
    ALTER TABLE {{ params.base_table }}_temp
        ALTER COLUMN geometry TYPE geometry(Polygon,28992)
        USING ST_Transform(geometry, 28992);
    DROP TABLE IF EXISTS {{ params.base_table }}_regimes_temp;
    CREATE TABLE {{ params.base_table }}_regimes_temp (
        LIKE {{ params.base_table }}_regimes INCLUDING ALL);
    DROP SEQUENCE IF EXISTS {{ params.base_table }}_regimes_temp_id_seq
       CASCADE;
    CREATE SEQUENCE {{ params.base_table }}_regimes_temp_id_seq
        OWNED BY {{ params.base_table }}_regimes_temp.id;
    ALTER TABLE {{ params.base_table }}_regimes_temp
        ALTER COLUMN id SET DEFAULT
        NEXTVAL('{{ params.base_table }}_regimes_temp_id_seq');
    ALTER TABLE {{ params.base_table }}_regimes_temp
        ADD COLUMN IF NOT EXISTS e_type_description VARCHAR;
"""

SQL_RENAME_TEMP_TABLES: Final = """
    DROP TABLE IF EXISTS {{ params.base_table }}_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}
        RENAME TO {{ params.base_table }}_old;
    ALTER TABLE {{ params.base_table }}_temp
        RENAME TO {{ params.base_table }};
    DROP TABLE IF EXISTS {{ params.base_table }}_regimes_old;
    ALTER TABLE IF EXISTS {{ params.base_table }}_regimes
        RENAME TO {{ params.base_table }}_regimes_old;
    ALTER TABLE {{ params.base_table }}_regimes_temp
        RENAME TO {{ params.base_table }}_regimes;
    ALTER INDEX IF EXISTS {{ params.base_table }}_regimes_parent_id_idx
        RENAME TO {{ params.base_table }}_regimes_old_parent_id_idx;
    CREATE INDEX {{ params.base_table }}_regimes_parent_id_idx
        ON {{ params.base_table }}_regimes(parent_id);
    ALTER INDEX IF EXISTS {{ params.base_table }}_geometry_idx
        RENAME TO {{ params.base_table }}_old_geometry_idx;
    CREATE INDEX {{ params.base_table }}_geometry_idx
        ON {{ params.base_table }} USING gist(geometry);
"""

TMP_DIR: Final = f"{SHARED_DIR}/{dag_id}"

E_TYPES: Final = dict(
    E1="Parkeerverbod",
    E2="Verbod stil te staan",
    E3="Verbod fietsen en bromfietsen te plaatsen",
    E4="Parkeergelegenheid",
    E5="Taxistandplaats",
    E6="Gehandicaptenparkeerplaats",
    E6a="Gehandicaptenparkeerplaats algemeen",
    E6b="Gehandicaptenparkeerplaats op kenteken",
    E7="Gelegenheid bestemd voor het onmiddellijk laden en lossen van goederen",
    E8="Parkeergelegenheid alleen bestemd voor de voertuigcategorie of groep voertuigen die op het bord is aangegeven",
    E9="Parkeergelegenheid alleen bestemd voor vergunninghouders",
    E10=(
        "Parkeerschijf-zone met verplicht gebruik van parkeerschijf, tevens parkeerverbod indien er langer wordt "
        "geparkeerd dan de parkeerduur die op het bord is aangegeven"
    ),
    E11="Einde parkeerschijf-zone met verplicht gebruik van parkeerschijf",
    E12="Parkeergelegenheid ten behoeve van overstappers op het openbaar vervoer",
    E13="Parkeergelegenheid ten behoeve van carpoolers",
)


def import_data(shp_file, ids):
    """
    Import Shape File into database.
    """
    base_regime_sql = (
        "("
        "'{parent_id}',"
        "'{soort}',"
        "'{e_type}',"
        "'{e_type_description}',"
        "'{bord}',"
        "'{begin_tijd}',"
        "'{eind_tijd}',"
        "E'{opmerking}',"
        "{dagen},"
        "{kenteken},"
        "{begin_datum},"
        "{eind_datum},"
        "'{aantal}'"
        ")"
    )

    parkeervakken_sql = []
    regimes_sql = []
    duplicates = []
    print(f"Processing: {shp_file}")
    with shapefile.Reader(shp_file, encodingErrors="ignore") as shape:
        for row in shape:
            if int(row.record.PARKEER_ID) in ids:
                # Exclude dupes
                duplicates.append(row.record.PARKEER_ID)
                continue
            ids.append(int(row.record.PARKEER_ID))

            regimes = create_regimes(row=row)
            soort = "FISCAAL"
            if len(regimes) == 1:
                soort = regimes[0]["soort"]

            parkeervakken_sql.append(create_parkeervaak(row=row, soort=soort))
            regimes_sql += [
                base_regime_sql.format(
                    parent_id=row.record.PARKEER_ID,
                    soort=mode["soort"],
                    e_type=mode["e_type"],
                    e_type_description=E_TYPES.get(mode["e_type"], ""),
                    bord=mode["bord"],
                    begin_tijd=mode["begin_tijd"].strftime("%H:%M"),
                    eind_tijd=mode["eind_tijd"].strftime("%H:%M"),
                    opmerking=mode["opmerking"],
                    dagen="'{" + ",".join([f'"{day}"' for day in mode["dagen"]]) + "}'",
                    kenteken=f"'{mode['kenteken']}'" if mode["kenteken"] else "NULL",
                    begin_datum=f"'{mode['begin_datum']}'" if mode["begin_datum"] else "NULL",
                    eind_datum=f"'{mode['eind_datum']}'" if mode["eind_datum"] else "NULL",
                    aantal=row.record.AANTAL,
                )
                for mode in regimes
            ]

    create_parkeervakken_sql = (
        "INSERT INTO {} ("
        "id, buurtcode, straatnaam, soort, type, aantal, geometry, e_type"
        ") VALUES {};"
    ).format(TABLES["BASE_TEMP"], ",".join(parkeervakken_sql))
    create_regimes_sql = (
        "INSERT INTO {} ("
        "parent_id, soort, e_type, e_type_description, bord, begin_tijd, eind_tijd, opmerking, dagen, "
        "kenteken, begin_datum, eind_datum, aantal"
        ") VALUES {};"
    ).format(TABLES["REGIMES_TEMP"], ",".join(regimes_sql))

    hook = PostgresHook()
    if len(parkeervakken_sql):
        try:
            hook.run(create_parkeervakken_sql)
        except Exception as e:
            raise Exception("Failed to create parkeervakken: {}".format(str(e)[0:150]))
    if len(regimes_sql):
        try:
            hook.run(create_regimes_sql)
        except Exception as e:
            raise Exception("Failed to create regimes: {}".format(str(e)[0:150]))

    print(
        "Created: {} parkeervakken and {} regimes".format(len(parkeervakken_sql), len(regimes_sql))
    )
    return duplicates


def download_latest_export_file(swift_conn_id, container, name_regex, *args, **kwargs):
    """
    Find latest export filename
    """
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
        subprocess.run(
            ["unzip", "-o", zip_path, "-d", TMP_DIR], stderr=subprocess.PIPE, check=True
        )
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Failed to extract zip: {e.stderr}")

    return latest["name"]


def run_imports(*args, **kwargs):
    """
    Run imports for all files in zip that match date.
    """
    ids = []
    for shp_file in source.glob("*.shp"):
        duplicates = import_data(str(shp_file), ids)

        if len(duplicates):
            print("Duplicates found: {}".format(", ".join(duplicates)))


args = default_args.copy()

with DAG(
    dag_id,
    default_args=args,
    description="Parkeervakken",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    source = pathlib.Path(TMP_DIR)

    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {TMP_DIR}")

    download_and_extract_zip = PythonOperator(
        task_id="download_and_extract_zip",
        python_callable=download_latest_export_file,
        op_kwargs=dict(
            swift_conn_id="objectstore_parkeervakken",
            container="tijdregimes",
            name_regex=r"^nivo_\d+\.zip",
        ),
    )

    download_and_extract_nietfiscaal_zip = PythonOperator(
        task_id="download_and_extract_nietfiscaal_zip",
        python_callable=download_latest_export_file,
        op_kwargs=dict(
            swift_conn_id="objectstore_parkeervakken",
            container="Parkeervakken",
            name_regex=r"^\d+\_nietfiscaal\.zip",
        ),
    )

    create_temp_tables = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLES,
        params=dict(base_table=f"{dag_id}_{dag_id}"),
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
                params=dict(table_name=f"{dag_id}_{dag_id}_temp"),
                result_checker=operator.ge,
            )
        ],
    )

    rename_temp_tables = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLES,
        params=dict(base_table=f"{dag_id}_{dag_id}"),
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
def create_parkeervaak(row, soort=None):
    geometry = "''"
    if row.shape.shapeTypeName == "POLYGON":
        geometry = "ST_GeometryFromText('{}', 28992)".format(str(Polygon(row.shape.points)))
    sql = (
        "("
        "'{parkeer_id}',"
        "'{buurtcode}',"
        "E'{straatnaam}',"
        "'{soort}',"
        "'{type}',"
        "'{aantal}',"
        "{geometry},"
        "'{e_type}'"
        ")"
    ).format(
        parkeer_id=row.record.PARKEER_ID,
        buurtcode=row.record.BUURTCODE,
        straatnaam=row.record.STRAATNAAM.replace("'", "\\'"),
        soort=row.record.SOORT or soort,
        type=row.record.TYPE or "",
        aantal=row.record.AANTAL,
        geometry=geometry,
        e_type=row.record.E_TYPE or "",
    )
    return sql


def create_regimes(row):

    output = []

    days = days_from_row(row)

    base_data = dict(
        parent_id=row.record.PARKEER_ID or "",
        soort="FISCAAL",
        e_type="",
        bord="",
        begin_tijd=datetime.time(0, 0),
        eind_tijd=datetime.time(23, 59),
        opmerking=row.record.OPMERKING or "",
        dagen=WEEK_DAYS,
        kenteken=None,
        begin_datum=None,
        eind_datum=None,
    )

    mode_start = datetime.time(0, 0)
    mode_end = datetime.time(23, 59)

    modes = get_modes(row)
    if len(modes) == 0:
        # No time modes, but could have full override.
        x = base_data.copy()
        x.update(
            dict(
                soort=row.record.SOORT or "FISCAAL",
                bord=row.record.BORD or "",
                e_type=row.record.E_TYPE or "",
                kenteken=row.record.KENTEKEN,
            )
        )
        return [x]

    for mode in modes:
        if mode.get("begin_tijd", datetime.time(0, 0)) > mode_start:
            # Time bound. Start of the day mode.
            sod_mode = base_data.copy()
            sod_mode["dagen"] = days
            sod_mode["begin_tijd"] = mode_start
            sod_mode["eind_tijd"] = remove_a_minute(mode["begin_tijd"])
            output.append(sod_mode)

        mode_data = base_data.copy()
        mode_data.update(mode)
        mode_data["dagen"] = days
        output.append(mode_data)
        mode_start = add_a_minute(mode["eind_tijd"])

    if mode.get("eind_tijd", datetime.time(23, 59)) < mode_end:
        # Time bound. End of the day mode.
        eod_mode = base_data.copy()
        eod_mode["dagen"] = days
        eod_mode["begin_tijd"] = add_a_minute(mode["eind_tijd"])
        output.append(eod_mode)
    return output


def add_a_minute(time):
    return (
        datetime.datetime.combine(datetime.date.today(), time) + datetime.timedelta(minutes=1)
    ).time()


def remove_a_minute(time):
    return (
        datetime.datetime.combine(datetime.date.today(), time) - datetime.timedelta(minutes=1)
    ).time()


def get_modes(row):
    modes = []

    base = dict(
        soort=row.record.SOORT or "FISCAAL",
        bord=row.record.BORD or "",
        e_type=row.record.E_TYPE or "",
        kenteken=row.record.KENTEKEN,
    )
    if any(
        [
            row.record.TVM_BEGINT,
            row.record.TVM_EINDT,
            row.record.TVM_BEGIND,
            row.record.TVM_EINDD,
            row.record.TVM_OPMERK,
        ]
    ):
        # TVM
        tvm_mode = base.copy()
        tvm_mode.update(
            dict(
                soort=row.record.E_TYPE or "FISCAAL",
                begin_datum=row.record.TVM_BEGIND or "",
                eind_datum=row.record.TVM_EINDD or "",
                opmerking=(row.record.TVM_OPMERK or "").replace("'", "\\'"),
                begin_tijd=parse_time(row.record.TVM_BEGINT, datetime.time(0, 0)),
                eind_tijd=parse_time(row.record.TVM_EINDT, datetime.time(23, 59)),
            )
        )
        modes.append(tvm_mode)

    if any([row.record.BEGINTIJD1, row.record.EINDTIJD1]):
        begin_tijd = parse_time(row.record.BEGINTIJD1, datetime.time(0, 0))
        eind_tijd = parse_time(row.record.EINDTIJD1, datetime.time(23, 59))
        if begin_tijd < eind_tijd:
            x = base.copy()
            x.update(dict(begin_tijd=begin_tijd, eind_tijd=eind_tijd))
            modes.append(x)
        else:
            # Mode: 20:00, 06:00
            x = base.copy()
            x.update(dict(begin_tijd=datetime.time(0, 0), eind_tijd=eind_tijd))
            y = base.copy()
            y.update(dict(begin_tijd=begin_tijd, eind_tijd=datetime.time(23, 59)))
            modes.append(x)
            modes.append(y)
    if any([row.record.BEGINTIJD2, row.record.EINDTIJD2]):
        x = base.copy()
        x.update(
            dict(
                begin_tijd=parse_time(row.record.BEGINTIJD2, datetime.time(0, 0)),
                eind_tijd=parse_time(row.record.EINDTIJD2, datetime.time(23, 59)),
            )
        )
        modes.append(x)
    return modes


def days_from_row(row):
    """
    Parse week days from row.
    """

    if row.record.MA_VR:
        # Monday to Friday
        days = WEEK_DAYS[:4]
    elif row.record.MA_ZA:
        # Monday to Saturday
        days = WEEK_DAYS[:5]

    elif not any([getattr(row.record, day.upper()) for day in WEEK_DAYS]):
        # All days apply
        days = WEEK_DAYS
    else:
        # One day permit
        days = [day for day in WEEK_DAYS if getattr(row.record, day.upper()) is not None]

    return days


def parse_time(value, default=None):
    """
    Parse time or return default
    """
    if value is not None:
        if value == "24:00":
            value = "23:59"
        if value.startswith("va "):
            value = value[3:]

        try:
            parsed = dateutil.parser.parse(value)
        except dateutil.parser._parser.ParserError:
            pass
        else:
            return parsed.time()
    return default
