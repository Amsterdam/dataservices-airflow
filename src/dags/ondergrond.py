import operator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from contact_point.callbacks import get_contact_point_on_failure_callback
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator
from ogr2ogr_operator import Ogr2OgrOperator
from provenance_rename_operator import ProvenanceRenameOperator
from airflow.operators.postgres_operator import PostgresOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from airflow.operators.dummy_operator import DummyOperator
from postgres_permissions_operator import PostgresPermissionsOperator


from common.db import DatabaseEngine

from common import (
    quote_string,
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)


dag_id = "ondergrond"
variables = Variable.get(dag_id, deserialize_json=True)
files_to_download = variables["files_to_download"]
db_conn = DatabaseEngine()
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


SQL_DROP_UNNECESSARY_COLUMNS_TMP_TABLE = """
    ALTER TABLE {{ params.tablename }}
    DROP COLUMN IF EXISTS dateringtot,
    DROP COLUMN IF EXISTS dateringvan,
    DROP COLUMN IF EXISTS bestandsnaam,
    DROP COLUMN IF EXISTS openbaarna,
    DROP COLUMN IF EXISTS "datum toegevoegd",
    DROP COLUMN IF EXISTS "datum rapport",
    DROP COLUMN IF EXISTS nummer,
    DROP COLUMN IF EXISTS "opmerkingen",
    DROP COLUMN IF EXISTS fid;
"""

SQL_DROP_TMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

with DAG(
    dag_id,
    description="uitgevoerde onderzoeken in of op de ondergrond, bijv. Archeologische verwachtingen (A), Bodemkwaliteit (B), Conventionele explosieven (C) kademuren Dateren (D) en Ondergrondse Obstakels (OO).",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id)
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

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{data_file}",
            swift_conn_id="objectstore_dataruimte",
            container="ondergrond",
            object_id=f"historische_onderzoeken/{data_file}",
            output_path=f"{tmp_dir}/{data_file}",
        )
        for _, data_file in files_to_download.items()
    ]

    # 4. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_{table_name}_based_upon_schema",
            data_schema_name=dag_id,
            data_table_name=f"{dag_id}_{table_name}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for table_name, data_file in files_to_download.items()
    ]

    # 5.create the SQL for creating the table using ORG2OGR PGDump
    GEOJSON_to_DB = [
        Ogr2OgrOperator(
            task_id=f"import_data_{table_name}",
            target_table_name=f"{dag_id}_{table_name}_new",
            input_file=f"{tmp_dir}/{data_file}",
            s_srs="EPSG:3857",
            t_srs="EPSG:28992",
            geometry_name="geometrie",
            mode="PostgreSQL",
            db_conn=db_conn,
        )
        for table_name, data_file in files_to_download.items()
    ]

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # prepare the checks and added them per source to a dictionary
    for table_name, _ in files_to_download.items():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{table_name}",
                pass_value=10,
                params=dict(table_name=f"{dag_id}_{table_name}_new"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{table_name}",
                params=dict(
                    table_name=f"{dag_id}_{table_name}_new",
                    geotype=[
                        "MULTIPOLYGON",
                    ],
                    geo_column="geometrie",
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name["{table_name}"] = total_checks

    # 7. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{table_name}",
            checks=check_name["{table_name}"],
        )
        for table_name, _ in files_to_download.items()
    ]

    # 8. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different
    # number of lanes (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 9. Drop cols - that do not show up in the API
    drop_unnecessary_cols = [
        PostgresOperator(
            task_id=f"drop_unnecessary_cols_{dag_id}_{table_name}_new",
            sql=SQL_DROP_UNNECESSARY_COLUMNS_TMP_TABLE,
            params=dict(tablename=f"{dag_id}_{table_name}_new"),
        )
        for table_name, _ in files_to_download.items()
        if table_name == "historischeonderzoeken"
    ]

    # 10. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different
    # number of lanes (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 11. Check for changes to merge in target table
    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_{table_name}",
            source_table=f"{dag_id}_{table_name}_new",
            target_table=f"{dag_id}_{table_name}",
        )
        for table_name, _ in files_to_download.items()
    ]

    # 12. Clean up
    clean_up = [
        PostgresOperator(
            task_id="clean_up",
            sql=SQL_DROP_TMP_TABLE,
            params=dict(tablename=f"{dag_id}_{table_name}_new"),
        )
        for table_name, _ in files_to_download.items()
    ]

    # 13. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> download_data

# FLOW
for (download_file, create_table, import_data) in zip(download_data, create_tables, GEOJSON_to_DB):

    [download_file >> create_table >> import_data] >> provenance_translation >> multi_checks

for check_data in zip(multi_checks):

    check_data >> Interface >> drop_unnecessary_cols

for drop_cols in zip(drop_unnecessary_cols):

    drop_cols >> Interface2 >> change_data_capture

for (check_changes, clean_tmp) in zip(change_data_capture, clean_up):

    [check_changes >> clean_tmp]

clean_up >> grant_db_permissions


# Mark down
dag.doc_md = """
    #### DAG summary
    This DAG contains info about conducted research on a specific location.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/ondergrond.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/ondergrond.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=ondergrond/ondergrond&x=106434&y=488995&radius=10
"""
