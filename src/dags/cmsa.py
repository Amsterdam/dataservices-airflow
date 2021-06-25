from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_cmsa import import_cmsa
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

DAG_ID: Final = "cmsa"
VARIABLES: Final = Variable.get(DAG_ID, deserialize_json=True)
FILES_TO_DOWNLOAD: Final = VARIABLES["files_to_download"]
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
SQL_PATH: Final = Path(__file__).resolve().parents[0] / "sql"

SQL_TABLE_RENAMES: Final = """
    ALTER TABLE IF EXISTS cmsa_sensor RENAME TO cmsa_sensor_old;
    ALTER TABLE IF EXISTS cmsa_locatie RENAME TO cmsa_locatie_old;
    ALTER TABLE IF EXISTS cmsa_markering RENAME TO cmsa_markering_old;

    ALTER TABLE cmsa_sensor_new RENAME TO cmsa_sensor;
    ALTER TABLE cmsa_locatie_new RENAME TO cmsa_locatie;
    ALTER TABLE cmsa_markering_new RENAME TO cmsa_markering;

    DROP TABLE IF EXISTS cmsa_sensor_old CASCADE;
    DROP TABLE IF EXISTS cmsa_locatie_old CASCADE;
    DROP TABLE IF EXISTS cmsa_markering_old;
"""


with DAG(
    DAG_ID,
    description="""Crowd Monitoring Systeem Amsterdam:
                3D sensoren, wifi sensoren, (tel)camera's en beacons""",
    default_args=default_args,
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:
    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(TMP_DIR)

    # 3. Download sensor data (geojson) from maps.amsterdam.nl
    download_geojson = HttpFetchOperator(
        task_id="download_geojson",
        endpoint="open_geodata/geojson.php?KAARTLAAG=CROWDSENSOREN&THEMA=cmsa",
        http_conn_id="ams_maps_conn_id",
        tmp_file=TMP_DIR / "sensors.geojson",
    )

    # 4. Download additional data (beacons.csv, cameras.xlsx)
    fetch_files = [
        SwiftOperator(
            task_id=f"download_{file}",
            # if conn is ommitted, it defaults to Objecstore Various Small Datasets
            # swift_conn_id="SWIFT_DEFAULT",
            container="cmsa",
            object_id=file,
            output_path=TMP_DIR / file,
        )
        for file in FILES_TO_DOWNLOAD
    ]

    # 5. Create SQL insert statements out of downloaded data
    proces_cmsa = PythonOperator(
        task_id="proces_sensor_data",
        python_callable=import_cmsa,
        op_args=[
            TMP_DIR / "cameras.xlsx",
            TMP_DIR / "beacons.csv",
            TMP_DIR / "sensors.geojson",
            TMP_DIR,
        ],
    )

    # 6. Create target tables: Sensor en Locatie
    create_tables = PostgresFilesOperator(
        task_id="create_target_tables",
        sql_files=[f"{SQL_PATH}/cmsa_data_create.sql"],
    )

    # 7. Insert data into DB
    import_data = PostgresFilesOperator(
        task_id="import_data_into_DB",
        sql_files=[
            TMP_DIR / "cmsa_sensor_new.sql",
            TMP_DIR / "cmsa_locatie_new.sql",
        ],
    )

    # 8. Create target tables: Markering (join between Sensor en Locatie)
    fill_markering = PostgresFilesOperator(
        task_id="insert_into_table_markering",
        sql_files=[f"{SQL_PATH}/cmsa_data_insert_markering.sql"],
    )

    # 9. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 10. Rename temp named tables to final names
    rename_tables = PostgresOperator(task_id="rename_tables", sql=SQL_TABLE_RENAMES)

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    (
        slack_at_start
        >> mkdir
        >> download_geojson
        >> fetch_files
        >> proces_cmsa
        >> create_tables
        >> import_data
        >> fill_markering
        >> provenance_translation
        >> rename_tables
        >> grant_db_permissions
    )

dag.doc_md = """
    #### DAG summary
    This DAG contains crowd monitoring sensor data,
    the source is the CMSA (Crowd Monitoring Systeem Amsterdam)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/cmsa.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/cmsa.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=cmsa/locatie&x=106434&y=488995&radius=10
"""
