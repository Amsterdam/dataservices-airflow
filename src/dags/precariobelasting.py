import operator
import re
from functools import partial
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import define_temp_db_schema, pg_params
from common.sql import SQL_DROP_TABLE
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.precariobelasting_add import ADD_GEBIED_COLUMN, ADD_TITLE, RENAME_DATAVALUE_GEBIED
from swift_operator import SwiftOperator

DAG_ID: Final = "precariobelasting"
variables: dict = Variable.get(DAG_ID, deserialize_json=True)
tmp_dir: str = f"{SHARED_DIR}/{DAG_ID}"
tmp_database_schema: str = define_temp_db_schema(dataset_name=DAG_ID)
data_endpoints: dict[str, str] = variables["temp_data"]
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}

# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=DAG_ID)


def clean_data(file_name: str) -> None:
    """Cleans the data content of the given file.

    Translates non-ASCII characters: \xc2\xad to None.
    Will remove space hyphen characters.

    Params:
        file_name: Name of file to clean data.
    """  # noqa: D301
    data = open(file_name).read()
    result = re.sub(r"[\xc2\xad]", "", data)
    with open(file_name, "w") as output:
        output.write(result)


with DAG(
    DAG_ID,
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. create download temp directory to store the data
    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file_name}",
            swift_conn_id="objectstore_dataservices",
            container="Dataservices",
            object_id=url,
            output_path=f"{tmp_dir}/{url}",
        )
        for file_name, url in data_endpoints.items()
    ]

    # 4. Cleanse the downloaded data (remove the space hyphen characters)
    clean_up_data = [
        PythonOperator(
            task_id=f"clean_data_{file_name}",
            python_callable=clean_data,
            op_args=[f"{tmp_dir}/{url}"],
        )
        for file_name, url in data_endpoints.items()
    ]

    # 5. drop TEMP table on the database
    # PostgresOperator will execute SQL in safe mode.
    drop_if_exists_tmp_tables = [
        PostgresOnAzureOperator(
            task_id=f"drop_if_exists_tmp_table_{key}",
            sql=SQL_DROP_TABLE,
            params={"schema": tmp_database_schema, "tablename": f"{DAG_ID}_{key}_new"},
        )
        for key in data_endpoints.keys()
    ]

    # 6. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 7. Import data
    # NOTE: ogr2ogr demands the PK is of type integer.
    import_data = [
        Ogr2OgrOperator(
            task_id=f"import_data_{file_name}",
            target_table_name=f"{DAG_ID}_{file_name}_new",
            db_schema=tmp_database_schema,
            input_file=f"{tmp_dir}/{url}",
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            fid="id",
            auto_detect_type="YES",
            mode="PostgreSQL",
            geometry_name="geometry",
            promote_to_multi=True,
        )
        for file_name, url in data_endpoints.items()
    ]

    # 8. Prepare the checks and added them per source to a dictionary
    for file_name in data_endpoints.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{file_name}",
                pass_value=2,
                params={
                    "table_name": "{tmp_schema}.{dataset}_{table}_new".format(
                        tmp_schema=tmp_database_schema, dataset=DAG_ID, table=file_name
                    )
                },
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{file_name}",
                params={
                    "table_name": "{tmp_schema}.{dataset}_{table}_new".format(
                        tmp_schema=tmp_database_schema, dataset=DAG_ID, table=file_name
                    ),
                    "geotype": ["POLYGON", "MULTIPOLYGON"],
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[file_name] = total_checks

    # 9. Execute bundled checks (step 7) on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{file_name}",
            checks=check_name[file_name],
            dataset_name=DAG_ID,
        )
        for file_name in data_endpoints.keys()
    ]

    # 10. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema=tmp_database_schema,
    )

    # 11. Add column TITLE as its was set in the display property in the metadataschema
    add_title_columns = [
        PostgresOnAzureOperator(
            task_id=f"add_title_column_{file_name}",
            sql=ADD_TITLE,
            params={
                "schema": tmp_database_schema,
                "tablenames": [f"{DAG_ID}_{file_name}_new"],
            },
        )
        for file_name in data_endpoints.keys()
    ]

    # 12. Dummy operator is used act as an interface
    # between one set of parallel tasks to another parallel taks set
    # (without this intermediar Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 13. Add derived columns (only woonschepen en bedrijfsvaartuigen are missing gebied as column)
    add_gebied_columns = [
        PostgresOnAzureOperator(
            task_id=f"add_gebied_{file_name}",
            sql=ADD_GEBIED_COLUMN,
            params={
                "schema": tmp_database_schema,
                "tablenames": [f"{DAG_ID}_{file_name}_new"],
            },
        )
        for file_name in ["woonschepen", "bedrijfsvaartuigen"]
    ]

    # 14. rename values in column gebied for terrassen en passagiersvaartuigen
    # (woonschepen en bedrijfsvaartuigen are added in the previous step)
    rename_value_gebieden = [
        PostgresOnAzureOperator(
            task_id=f"rename_value_{file_name}",
            sql=RENAME_DATAVALUE_GEBIED,
            params={
                "schema": tmp_database_schema,
                "tablenames": [f"{DAG_ID}_{file_name}_new"],
            },
        )
        for file_name in ["terrassen", "passagiersvaartuigen"]
    ]

    # 15. Insert data from temp to target table
    copy_data_to_target = [
        PostgresTableCopyOperator(
            task_id=f"copy_data_to_target_{key}",
            dataset_name_lookup=DAG_ID,
            dataset_name=DAG_ID,
            source_table_name=f"{DAG_ID}_{key}_new",
            source_schema_name=tmp_database_schema,
            target_table_name=f"{DAG_ID}_{key}",
            drop_target_if_unequal=False,
        )
        for key in data_endpoints.keys()
    ]

    # FLOW. define flow with parallel executing of serial tasks for each file
    slack_at_start >> mk_tmp_dir >> download_data

    for (data, clean_up, drop_table, ingest_data, add_title_col) in zip(
        download_data, clean_up_data, drop_if_exists_tmp_tables, import_data, add_title_columns
    ):

        [data >> clean_up >> drop_table >> ingest_data] >> provenance_translation

        add_title_col >> Interface

    provenance_translation >> add_title_columns
    Interface >> add_gebied_columns

    for add_gebied_column, rename_value_gebied in zip(add_gebied_columns, rename_value_gebieden):
        add_gebied_column >> rename_value_gebied

    rename_value_gebieden >> Interface2 >> multi_checks

    for data_checks, copy_data in zip(multi_checks, copy_data_to_target):

        [data_checks >> copy_data]

    dag.doc_md = """
    #### DAG summary
    This DAG contains precariobelasting data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/precariobelasting/precariobelasting/
"""
