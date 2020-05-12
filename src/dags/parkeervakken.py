import datetime
import pathlib
import dateutil
import shapefile
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from shapely.geometry import Polygon
from swift_operator import SwiftOperator
from common import default_args

dag_id = "parkeervakken"
postgres_conn_id = "parkeervakken_postgres"

# CONFIG = Variable.get(DAG_ID, deserialize_json=True)

TABLES = dict(
    BASE=f"{dag_id}_{dag_id}",
    BASE_TEMP=f"{dag_id}_{dag_id}_temp",
    REGIMES=f"{dag_id}_{dag_id}_regimes",
    REGIMES_TEMP=f"{dag_id}_{dag_id}_regimes_temp",
)


SQL_CREATE_TEMP_TABLES = """
    DROP TABLE IF EXISTS {{ params.base_table }}_temp;
    CREATE TABLE {{ params.base_table }}_temp (
        LIKE {{ params.base_table }} INCLUDING ALL);
    ALTER TABLE {{ params.base_table }}_temp
        ALTER COLUMN geom TYPE geometry(Polygon,28992)
        USING ST_Transform(geom, 28992);
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
"""

SQL_RENAME_TEMP_TABLES = """
    ALTER TABLE IF EXISTS {{ params.base_table }}
        RENAME TO {{ params.base_table }}_old;
    ALTER TABLE {{ params.base_table }}_temp
        RENAME TO {{ params.base_table }};
    ALTER TABLE IF EXISTS {{ params.base_table }}_regimes
        RENAME TO {{ params.base_table }}_regimes_old;
    ALTER TABLE {{ params.base_table }}_regimes_temp
        RENAME TO {{ params.base_table }}_regimes;
"""

TMP_DIR = f"/tmp/{dag_id}"


def import_data(shp_file, ids):
    """
    Import Shape File into database.
    """
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
            parkeervakken_sql.append(create_parkeervaak(row=row))

            if any(
                [
                    row.record.KENTEKEN,
                    row.record.BORD,
                    row.record.E_TYPE,
                    row.record.BEGINTIJD1,
                    row.record.EINDTIJD1,
                    row.record.BEGINTIJD2,
                    row.record.EINDTIJD2,
                    row.record.TVM_BEGINT,
                    row.record.TVM_EINDT,
                    row.record.TVM_BEGIND,
                    row.record.TVM_EINDD,
                    row.record.TVM_OPMERK,
                ]
            ):
                regimes_sql += create_regimes(row=row)

    create_parkeervakken_sql = (
        "INSERT INTO {} ("
        "id, buurtcode, straatnaam, soort, type, aantal, geom, e_type"
        ") VALUES {};"
    ).format(TABLES["BASE_TEMP"], ",".join(parkeervakken_sql))
    create_regimes_sql = (
        "INSERT INTO {} ("
        "parent_id, soort, e_type, begin_tijd, eind_tijd, opmerking, dagen"
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
        "Created: {} parkeervakken and {} regimes".format(
            len(parkeervakken_sql), len(regimes_sql)
        )
    )
    return duplicates


def find_export_date():
    """
    Find closest export date.
    """
    today = datetime.date.today()
    date = today
    if today.weekday() == 3:
        date = today
    elif today.weekday() > 3:
        # This week
        date = today - datetime.timedelta(days=today.weekday() - 3)
    else:
        # Last week
        date = today - datetime.timedelta(days=today.weekday() + 4)
    return date.strftime("%Y%m%d")


def run_imports(last_date):
    """
    Run imports for all files in zip that match date.
    """
    ids = []
    for shp_file in source.glob("*{}.shp".format(last_date)):
        duplicates = import_data(str(shp_file), ids)

        if len(duplicates):
            print("Duplicates found: {}".format(", ".join(duplicates)))


with DAG(dag_id, default_args=default_args, description="Parkeervakken") as dag:
    last_date = find_export_date()
    zip_file = "nivo_{}.zip".format(last_date)
    source = pathlib.Path(TMP_DIR)

    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {TMP_DIR}")

    fetch_zip = SwiftOperator(
        task_id="fetch_zip",
        container="tijdregimes",
        object_id=zip_file,
        output_path=f"{TMP_DIR}/{zip_file}",
        conn_id="parkeervakken_objectstore",
    )

    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f'unzip -o "{TMP_DIR}/{zip_file}" -d {TMP_DIR}',
    )

    create_temp_tables = PostgresOperator(
        task_id="create_temp_tables",
        sql=SQL_CREATE_TEMP_TABLES,
        params=dict(base_table=f"{dag_id}_{dag_id}"),
    )

    run_import_task = PythonOperator(
        task_id="run_import_task",
        op_kwargs={"last_date": last_date},
        python_callable=run_imports,
        dag=dag,
    )

    rename_temp_tables = PostgresOperator(
        task_id="rename_temp_tables",
        sql=SQL_RENAME_TEMP_TABLES,
        params=dict(base_table=f"{dag_id}_{dag_id}"),
    )

(
    mk_tmp_dir
    >> fetch_zip
    >> extract_zip
    >> create_temp_tables
    >> run_import_task
    >> rename_temp_tables
)


# Internals
def create_parkeervaak(row):
    geom = "''"
    if row.shape.shapeTypeName == "POLYGON":
        geom = "ST_GeometryFromText('{}', 28992)".format(str(Polygon(row.shape.points)))
    sql = (
        "("
        "'{parkeer_id}',"
        "'{buurtcode}',"
        "E'{straatnaam}',"
        "'{soort}',"
        "'{type}',"
        "'{aantal}',"
        "{geom},"
        "'{e_type}'"
        ")"
    ).format(
        parkeer_id=row.record.PARKEER_ID,
        buurtcode=row.record.BUURTCODE,
        straatnaam=row.record.STRAATNAAM.replace("'", "\\'"),
        soort=row.record.SOORT or "",
        type=row.record.TYPE or "",
        aantal=row.record.AANTAL,
        geom=geom,
        e_type=row.record.E_TYPE or "",
    )
    return sql


def create_regimes(row):
    sql = (
        "("
        "'{parent_id}',"
        "'{soort}',"
        "'{e_type}',"
        "'{begin_tijd}',"
        "'{eind_tijd}',"
        "'{opmerking}',"
        "{dagen}"
        ")"
    )

    output = []

    days = days_from_row(row)

    base_data = dict(
        soort=row.record.SOORT or "",
        e_type=row.record.E_TYPE or "",
        bord=row.record.BORD or "",
        begin_tijd="00:00",
        eind_tijd="23:59",
        opmerking=row.record.OPMERKING or "",
        dagen=days,
        parent_id=row.record.PARKEER_ID or "",
    )

    if row.record.KENTEKEN:
        kenteken_regime = base_data.copy()
        kenteken_regime.update(
            dict(
                kenteken=row.record.KENTEKEN,
                begin_tijd=format_time(row.record.BEGINTIJD1, "00:00"),
                eind_tijd=format_time(row.record.EINDTIJD1, "23:59"),
            )
        )
        output.append(sql.format(**kenteken_regime))
    elif any([row.record.BEGINTIJD1, row.record.EINDTIJD1]):
        output.append(sql.format(**base_data))

        second_mode = base_data.copy()
        second_mode["begin_tijd"] = format_time(row.record.BEGINTIJD1, "00:00")
        second_mode["eind_tijd"] = format_time(row.record.EINDTIJD1, "23:59")

        output.append(sql.format(**second_mode))
    elif any([row.record.BEGINTIJD2, row.record.EINDTIJD2]):
        output.append(sql.format(**base_data))

        second_mode = base_data.copy()
        second_mode["begin_tijd"] = format_time(row.record.BEGINTIJD2, "00:00")
        second_mode["eind_tijd"] = format_time(row.record.EINDTIJD2, "23:59")

        output.append(sql.format(**second_mode))
    elif any(
        [
            row.record.TVM_BEGINT,
            row.record.TVM_EINDT,
            row.record.TVM_BEGIND,
            row.record.TVM_EINDD,
            row.record.TVM_OPMERK,
        ]
    ):
        output.append(sql.format(**base_data))
        # TVM
        tvm_mode = base_data.copy()
        tvm_mode.update(
            dict(
                begin_datum=row.record.TVM_BEGIND or "",
                eind_datum=row.record.TVM_EINDD or "",
                opmerking=row.record.TVM_OPMERK or "",
                begin_tijd=format_time(row.record.TVM_BEGINT, "00:00"),
                eind_tijd=format_time(row.record.TVM_EINDT, "23:59"),
            )
        )
        output.append(sql.format(**tvm_mode))
    return output


def days_from_row(row):
    """
    Parse week days from row.
    """
    week_days = ["ma", "di", "wo", "do", "vr", "za", "zo"]

    if row.record.MA_VR:
        # Monday to Friday
        days = week_days[:4]
    elif row.record.MA_ZA:
        # Monday to Saturday
        days = week_days[:5]
    elif all([getattr(row.record, day.upper()) for day in week_days]):
        # All days apply
        days = week_days

    elif not any([getattr(row.record, day.upper()) for day in week_days]):
        # All days apply
        days = week_days
    else:
        # One day permit
        days = [
            day for day in week_days if getattr(row.record, day.upper()) is not None
        ]

    return "'{" + ",".join([f'"{day}"' for day in days]) + "}'"


def format_time(value, default=None):
    """
    Format time or return None
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
            return parsed.strftime("%H:%M")
    return default
