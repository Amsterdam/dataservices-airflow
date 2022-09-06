from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from common import SHARED_DIR, MessageOperator, default_args
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_cmsa import import_cmsa
from postgres_files_operator import PostgresFilesOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

DAG_ID: Final = "cmsa"
VARIABLES: Final = Variable.get(DAG_ID, deserialize_json=True)
FILES_TO_DOWNLOAD: Final = VARIABLES["files_to_download"]
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
SQL_PATH: Final = Path(__file__).resolve().parents[0] / "sql"

TMP_TABLE_PREFIX: Final = "tmp_"
TABLES_WITH_IDENTITY: Final = ("cmsa_locatie", "cmsa_markering")
TABLES: Final = ("cmsa_sensor", *TABLES_WITH_IDENTITY)

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
    )

    mkdir = mk_dir(TMP_DIR, clean_if_exists=True)

    download_geojson = HttpFetchOperator(
        task_id="download_geojson",
        endpoint="open_geodata/geojson_lnglat.php?KAARTLAAG=CROWDSENSOREN&THEMA=cmsa",
        http_conn_id="ams_maps_conn_id",
        tmp_file=TMP_DIR / "sensors.geojson",
    )

    fetch_files = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="SWIFT_DEFAULT",
            container="cmsa",
            object_id=file,
            output_path=TMP_DIR / file,
        )
        for file in FILES_TO_DOWNLOAD
    ]

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

    rm_tmp_tables = PostgresOnAzureOperator(
        task_id="rm_tmp_tables",
        sql="DROP TABLE IF EXISTS {tables} CASCADE".format(
            tables=", ".join(map(lambda t: f"{TMP_TABLE_PREFIX}{t}", TABLES))  # noqa: C417
        ),
    )

    # Drop the identity on the `id` column, otherwise SqlAlchemyCreateObjectOperator
    # gets seriously confused; as in: its finds something it didn't create and errors out.
    drop_identity_from_tables = [
        PostgresOnAzureOperator(
            task_id=f"drop_identity_from_table_{table}",
            sql="""
                ALTER TABLE IF EXISTS {{ params.table }}
                    ALTER COLUMN id DROP IDENTITY IF EXISTS;
            """,
            params={"table": table},
        )
        for table in TABLES_WITH_IDENTITY
    ]

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DAG_ID,
        ind_extra_index=False,
    )

    add_identity_to_tables = [
        PostgresOnAzureOperator(
            task_id=f"add_identity_to_table_{table}",
            sql="""
                ALTER TABLE {{ params.table }}
                    ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
            """,
            params={"table": table},
        )
        for table in TABLES_WITH_IDENTITY
    ]

    join_parallel_tasks = DummyOperator(task_id="join_parallel_tasks")

    postgres_create_tables_like = [
        PostgresTableCopyOperator(
            task_id=f"postgres_create_tables_like_{table}",
            dataset_name_lookup=DAG_ID,
            source_table_name=table,
            target_table_name=f"{TMP_TABLE_PREFIX}{table}",
            # Only copy table definitions. Don't do anything else.
            truncate_target=False,
            copy_data=False,
            drop_source=False,
        )
        for table in TABLES
    ]

    import_data = PostgresFilesOperator(
        task_id="import_data_into_db",
        dataset_name=DAG_ID,
        sql_files=[
            TMP_DIR / f"{TMP_TABLE_PREFIX}cmsa_sensor.sql",
            TMP_DIR / f"{TMP_TABLE_PREFIX}cmsa_locatie.sql",
        ],
    )

    fill_markering = PostgresOnAzureOperator(
        task_id="fill_markering",
        sql="""
            INSERT INTO {{ params.tmp_table_prefix }}cmsa_markering (sensor_id,
                                                                     locatie_id,
                                                                     sensornaam,
                                                                     sensortype,
                                                                     geometry)
                (SELECT sensor.id,
                        locatie.id,
                        sensor.naam,
                        sensor.type,
                        locatie.geometry
                 FROM {{ params.tmp_table_prefix }}cmsa_sensor AS sensor
                          INNER JOIN {{ params.tmp_table_prefix }}cmsa_locatie AS locatie
                                     ON locatie.sensor_id = sensor.id
                );
        """,
        params={"tmp_table_prefix": TMP_TABLE_PREFIX},
    )

    # Though identity columns are much more convenient than the old style serial columns, the
    # sequences associated with them are not renamed automatically when the table with the
    # identity columns is renamed. Hence, when renaming a table `tmp_fubar`, with identity column
    # `id`, to `fubar`, the associated sequence will still be named `tmp_fubar_id_seq`.
    #
    # This is not a problem, unless we suffer from certain OCD tendencies. Hence, we leave the
    # slight naming inconsistency as-is. Should you want to address it, don't rename the
    # sequence. Simply drop cascade it instead. After all, the identity column is only required
    # for the duration of the import; we don't need it afterwards. And when you do want to drop
    # cascade it, reuse the existing `drop_identity_to_tables` task by wrapping it in a
    # function and call it just before renaming the tables.
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_tables_for_{table}",
            new_table_name=table,
            old_table_name=f"{TMP_TABLE_PREFIX}{table}",
            cascade=True,
        )
        for table in TABLES
    ]

    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    (
        slack_at_start
        >> mkdir
        >> download_geojson
        >> fetch_files
        >> proces_cmsa
        >> rm_tmp_tables
        >> drop_identity_from_tables
        >> sqlalchemy_create_objects_from_schema
        >> add_identity_to_tables
        >> join_parallel_tasks
        >> postgres_create_tables_like
        >> import_data
        >> fill_markering
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
