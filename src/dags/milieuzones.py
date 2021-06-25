import operator
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_milieuzones import import_milieuzones
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.milieuzones import DEL_ROWS, DROP_COLS
from swift_operator import SwiftOperator

dag_id: str = "milieuzones"
variables_milieuzones: dict = Variable.get("milieuzones", deserialize_json=True)
files_to_download: dict = variables_milieuzones["files_to_download"]
tables_to_create: dict = variables_milieuzones["tables_to_create"]
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}
db_conn: object = DatabaseEngine()


with DAG(
    dag_id,
    description="touringcars, taxis, brom- en snorfietsen, vrachtwagens en bestelbussen",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
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
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            # Default swift = Various Small Datasets objectstore
            # swift_conn_id="SWIFT_DEFAULT",
            container="milieuzones",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for file in files_to_download
    ]

    # 4. Convert data to geojson
    convert_to_geojson = [
        PythonOperator(
            task_id=f"convert_{file}_to_geojson",
            python_callable=import_milieuzones,
            op_args=[
                f"{tmp_dir}/{file}",
                f"{tmp_dir}/geojson_{file}",
            ],
        )
        for file in files_to_download
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to
    # another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Create SQL
    import_data = [
        Ogr2OgrOperator(
            task_id=f"import_data_{key}",
            target_table_name=f"{dag_id}_{key}_new",
            input_file=f"{tmp_dir}/geojson_milieuzones.json",
            s_srs=None,
            auto_detect_type="YES",
            mode="PostgreSQL",
            fid="fid",
            db_conn=db_conn,
        )
        for key, code in tables_to_create.items()
    ]

    # 6. Drop unnecessary cols
    drop_cols = [
        PostgresOperator(
            task_id=f"drop_cols_{key}",
            sql=DROP_COLS,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in tables_to_create
    ]

    # 7. Drop unnecessary cols
    del_rows = [
        PostgresOperator(
            task_id=f"del_rows_{key}",
            sql=DEL_ROWS,
            params=dict(tablename=f"{dag_id}_{key}_new"),
        )
        for key in tables_to_create
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 9. Revalidate invalid geometry records
    # the source has some invalid records
    # to do: inform the source maintainer
    revalidate_geometry_records = [
        PostgresOperator(
            task_id=f"revalidate_geometry_{key}",
            sql=[
                f"""UPDATE {dag_id}_{key}_new
                SET geometry = ST_CollectionExtract((st_makevalid(geometry)),3)
                WHERE 1=1 AND ST_IsValid(geometry) is false;
                COMMIT;""",
            ],
        )
        for key in tables_to_create.keys()
    ]

    # Prepare the checks and added them per source to a dictionary
    for key in tables_to_create.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=1,
                params=dict(table_name=f"{dag_id}_{key}_new"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params=dict(
                    table_name=f"{dag_id}_{key}_new",
                    geotype=["MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 10. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in tables_to_create.keys()
    ]

    # 11. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=f"{dag_id}_{key}_new",
            new_table_name=f"{dag_id}_{key}",
        )
        for key in tables_to_create.keys()
    ]

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> download_data

for data, convert in zip(download_data, convert_to_geojson):

    data >> convert >> Interface

Interface >> import_data

for (
    import_data_file,
    drop_cols,
    del_rows,
    revalidate_geometry_record,
    multi_check,
    rename_table,
) in zip(
    import_data,
    drop_cols,
    del_rows,
    revalidate_geometry_records,
    multi_checks,
    rename_tables,
):

    (
        [import_data_file >> drop_cols >> del_rows]
        >> provenance_translation
        >> revalidate_geometry_record
    )

    [revalidate_geometry_record >> multi_check >> rename_table]

rename_tables >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about environmental omission zones (milieuzones) i.e.
    touringcars, taxi's, brom- en snorfietsen, vrachtwagens en bestelbussen.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/milieuzones.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/milieuzones.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=milieuzones/taxi&x=106434&y=488995&radius=10
"""
