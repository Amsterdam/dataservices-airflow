import pathlib

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from http_fetch_operator import HttpFetchOperator

from postgres_files_operator import PostgresFilesOperator

from swift_operator import SwiftOperator

from provenance_rename_operator import ProvenanceRenameOperator
from postgres_permissions_operator import PostgresPermissionsOperator

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)
from importscripts.import_cmsa import import_cmsa

dag_id = "cmsa"
variables = Variable.get(dag_id, deserialize_json=True)
files_to_download = variables["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
fetch_jsons = []


SQL_TABLE_RENAMES = """
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
    dag_id,
    description="""Crowd Monitoring Systeem Amsterdam:
                3D sensoren, wifi sensoren, (tel)camera's en beacons""",
    default_args=default_args,
    template_searchpath=["/"],
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download sensor data (geojson) from maps.amsterdam.nl
    download_geojson = HttpFetchOperator(
        task_id="download_geojson",
        endpoint="open_geodata/geojson.php?KAARTLAAG=CROWDSENSOREN&THEMA=cmsa",
        http_conn_id="ams_maps_conn_id",
        tmp_file=f"{tmp_dir}/sensors.geojson",
    )

    # 4. Download additional data (beacons.csv, cameras.xlsx)
    fetch_files = [
        SwiftOperator(
            task_id=f"download_{file}",
            # if conn is ommitted, it defaults to Objecstore Various Small Datasets
            # swift_conn_id="SWIFT_DEFAULT",
            container="cmsa",
            object_id=str(file),
            output_path=f"{tmp_dir}/{file}",
        )
        for file in files_to_download
    ]

    # 5. Create SQL insert statements out of downloaded data
    proces_cmsa = PythonOperator(
        task_id="proces_sensor_data",
        python_callable=import_cmsa,
        op_args=[
            f"{tmp_dir}/cameras.xlsx",
            f"{tmp_dir}/beacons.csv",
            f"{tmp_dir}/sensors.geojson",
            tmp_dir,
        ],
    )

    # 6. Create target tables: Sensor en Locatie
    create_tables = PostgresFilesOperator(
        task_id="create_target_tables",
        sql_files=[f"{sql_path}/cmsa_data_create.sql"],
    )

    # 7. Insert data into DB
    import_data = PostgresFilesOperator(
        task_id="import_data_into_DB",
        sql_files=[
            f"{tmp_dir}/cmsa_sensor_new.sql",
            f"{tmp_dir}/cmsa_locatie_new.sql",
        ],
    )

    # 8. Create target tables: Markering (join between Sensor en Locatie)
    fill_markering = PostgresFilesOperator(
        task_id="insert_into_table_markering",
        sql_files=[f"{sql_path}/cmsa_data_insert_markering.sql"],
    )

    # 9. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 10. Rename temp named tables to final names
    rename_tables = PostgresOperator(task_id="rename_tables", sql=SQL_TABLE_RENAMES)

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


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
