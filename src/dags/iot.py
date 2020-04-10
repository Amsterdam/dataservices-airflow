import json
from functools import partial
import pathlib
from string_utils import slugify
from airflow import DAG
from airflow.models import Variable

# from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from http_fetch_operator import HttpFetchOperator

from common import default_args, slack_webhook_token, DATAPUNT_ENVIRONMENT
from importscripts.import_iot import import_iot

dag_id = "iot"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
dag_config = Variable.get(dag_id, deserialize_json=True)

SQL_TABLE_RENAMES = """
    ALTER TABLE IF EXISTS iot_things RENAME TO iot_things_old;
    ALTER TABLE IF EXISTS iot_locations RENAME TO iot_locations_old;
    ALTER TABLE IF EXISTS iot_owners RENAME TO iot_owners_old;
    ALTER VIEW IF EXISTS iot_markers RENAME TO iot_markers_old;

    ALTER TABLE iot_things_new RENAME TO iot_things;
    ALTER TABLE iot_locations_new RENAME TO iot_locations;
    ALTER TABLE iot_owners_new RENAME TO iot_owners;
    ALTER VIEW iot_markers_new RENAME TO iot_markers;

    DROP TABLE IF EXISTS iot_owners_old CASCADE;
    DROP TABLE IF EXISTS iot_locations_old CASCADE;
    DROP TABLE IF EXISTS iot_things_old CASCADE;
    DROP VIEW IF EXISTS iot_markers_old;
"""


def cfg_factory(dag_id):
    return partial(Variable.get, dag_id, deserialize_json=True)


cfg = cfg_factory(dag_id)


def merge_sensor_files(tmp_dir, obj_names):
    root = []
    for obj_name in obj_names:
        with (pathlib.Path(tmp_dir) / f"{obj_name}.json").open() as f:
            rows = json.load(f)
            for row in rows:
                row["SELECTIE"] = obj_name
            root.append(rows)

    with (pathlib.Path(tmp_dir) / "sensors.json").open("w") as f:
        json.dump(root, f)


with DAG(dag_id, default_args=default_args, template_searchpath=["/"]) as dag:

    fetch_jsons = []
    tmp_dir = f"/tmp/{dag_id}"

    slack_at_start = SlackWebhookOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    obj_names = ("3D sensor", "WiFi sensor", "Telcamera")

    for name in obj_names:
        data = {
            "TABEL": "CROWDSENSOREN",
            "SELECT": name,
            "SELECTIEKOLOM": "Soort",
            "THEMA": "cmsa",
            "TAAL": "en",
            "BEHEER": 0,
            "NIEUW": "niet",
        }
        fetch_jsons.append(
            HttpFetchOperator(
                task_id=f"fetch_{slugify(name, separator='_')}",
                endpoint="_php/haal_objecten.php",
                data=data,
                http_conn_id="ams_maps_conn_id",
                tmp_file=f"{tmp_dir}/{name}.json",
            )
        )

    merge_sensors = PythonOperator(
        task_id="merge_sensors",
        python_callable=merge_sensor_files,
        op_args=[tmp_dir, obj_names],
    )

    import_iot = PythonOperator(
        task_id="import_iot",
        python_callable=import_iot,
        op_args=[
            f"{data_path}/{dag_id}/cameras.xlsx",
            f"{data_path}/{dag_id}/beacons.csv",
            f"{tmp_dir}/sensors.json",
            f"{tmp_dir}",
        ],
    )

    create_tables = PostgresOperator(
        task_id="create_tables", sql=f"{sql_path}/iot_data_create.sql",
    )

    import_data = PostgresOperator(
        task_id="import_data",
        sql=[f"{tmp_dir}/iot_things_new.sql", f"{tmp_dir}/iot_locations_new.sql",],
    )

    rename_tables = PostgresOperator(task_id="rename_tables", sql=SQL_TABLE_RENAMES)


(
    slack_at_start
    >> fetch_jsons
    >> merge_sensors
    >> import_iot
    >> create_tables
    >> import_data
    >> rename_tables
)

"""
echo "[" > "${TMPDIR}/sensors.json"
for S in "3D%20sensor" "WiFi%20sensor" "Telcamera"
do
    curl "https://maps.amsterdam.nl/_php/haal_objecten.php?TABEL=CROWDSENSOREN&SELECT=${S}&SELECTIEKOLOM=Soort&THEMA=cmsa&TAAL=en&BEHEER=0&NIEUW=niet" >> "${TMPDIR}/sensors.json"
    echo "," >> "${TMPDIR}/sensors.json"
done
echo "[]]" >> "${TMPDIR}/sensors.json"

echo "Process import data"
python ${SCRIPT_DIR}/import.py "${DATA_DIR}/cameras.xlsx" "${DATA_DIR}/beacons.csv" "${TMPDIR}/sensors.json" ${TMPDIR}

rm -f "${TMPDIR}/sensors.json"

echo "Create tables"
psql -X --set ON_ERROR_STOP=on << SQL
\\i ${SCRIPT_DIR}/iot_data_create.sql
SQL

echo "Import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\\i ${TMPDIR}/iot_things_new.sql
\\i ${TMPDIR}/iot_locations_new.sql
SQL

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS iot_things RENAME TO iot_things_old;
ALTER TABLE IF EXISTS iot_locations RENAME TO iot_locations_old;
ALTER TABLE IF EXISTS iot_owners RENAME TO iot_owners_old;
ALTER VIEW IF EXISTS iot_markers RENAME TO iot_markers_old;

ALTER TABLE iot_things_new RENAME TO iot_things;
ALTER TABLE iot_locations_new RENAME TO iot_locations;
ALTER TABLE iot_owners_new RENAME TO iot_owners;
ALTER VIEW iot_markers_new RENAME TO iot_markers;

DROP TABLE IF EXISTS iot_owners_old CASCADE;
DROP TABLE IF EXISTS iot_locations_old CASCADE;
DROP TABLE IF EXISTS iot_things_old CASCADE;
DROP VIEW IF EXISTS iot_markers_old;
COMMIT;
S
"""
