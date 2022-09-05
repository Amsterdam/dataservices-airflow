import operator
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import define_temp_db_schema
from common.path import mk_dir
from common.sql import SQL_DROP_TABLE
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

# Note: to snake case is needed in target table because of the provenance check, because
# number are seen as a start for underscore seperator. Covid19 is therefore translated as covid_19
# TODO: change logic for snake_case when dealing with numbers
DAG_ID: Final = "covid19"
TABLE_DATASET_NAME: Final = "covid_19"
variables_covid19: dict = Variable.get("covid19", deserialize_json=True)
files_to_download: dict = variables_covid19["files_to_download"]

# Note: Gebiedsverbod is absolete since "nieuwe tijdelijke wetgeving Corona maatregelen 01-12-2020"
# TODO: remove Gebiedsverbod and Straatartiestverbod from var.yml, if DSO-API endpoint and
# Amsterdam Schema definition can be removed.
tables_to_create = variables_covid19["tables_to_create"]
tables_to_check = {
    k: v for k, v in tables_to_create.items() if k not in ("gebiedsverbod", "straatartiestverbod")
}

TMP_DIR: Final = f"{SHARED_DIR}/{DAG_ID}"
tmp_database_schema: str = define_temp_db_schema(dataset_name=DAG_ID)
data_file: Final = f"{TMP_DIR}/OOV_COVID19_totaal.shp"
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


with DAG(
    DAG_ID,
    description="type of restriction area's.",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(TMP_DIR))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="SWIFT_DEFAULT",
            container="covid19",
            object_id=file,
            output_path=f"{TMP_DIR}/{file}",
        )
        for file in files_to_download
    ]

    # 4. drop TEMP table on the database
    # PostgresOperator will execute SQL in safe mode.
    drop_if_exists_tmp_tables = [
        PostgresOnAzureOperator(
            task_id=f"drop_if_exists_tmp_table_{key}",
            sql=SQL_DROP_TABLE,
            dataset_name=TABLE_DATASET_NAME,
            params={"schema": tmp_database_schema, "tablename": f"{TABLE_DATASET_NAME}_{key}_new"},
        )
        for key in tables_to_create.keys()
    ]

    # 5. Dummy operator acts as an interface between parallel tasks to
    # another parallel tasks with different number of lanes
    # (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 6. Import data
    # NOTE: ogr2ogr demands the PK is of type integer.
    import_data = [
        Ogr2OgrOperator(
            task_id=f"import_data_{key}",
            target_table_name=f"{TABLE_DATASET_NAME}_{key}_new",
            db_schema=tmp_database_schema,
            input_file=data_file,
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            fid="id",
            auto_detect_type="YES",
            mode="PostgreSQL",
            geometry_name="geometry",
            promote_to_multi=True,
            sql_statement=f"""
                    SELECT *
                    FROM OOV_COVID19_totaal
                    WHERE TYPE = '{code}'
                """,  # noqa: S608
        )
        for key, code in tables_to_create.items()
    ]

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{TABLE_DATASET_NAME}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema=tmp_database_schema,
    )

    # Prepare the checks and added them per source to a dictionary
    for key in tables_to_check.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=1,
                params={
                    "table_name": "{tmp_schema}.{dataset}_{table}_new".format(
                        tmp_schema=tmp_database_schema, dataset=TABLE_DATASET_NAME, table=key
                    )
                },
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params={
                    "table_name": "{tmp_schema}.{dataset}_{table}_new".format(
                        tmp_schema=tmp_database_schema, dataset=TABLE_DATASET_NAME, table=key
                    ),
                    "geotype": ["MULTIPOLYGON"],
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{key}", checks=check_name[key], dataset_name=TABLE_DATASET_NAME
        )
        for key in tables_to_check.keys()
    ]

    # 9. Dummy operator acts as an interface between parallel tasks to another parallel
    #  tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 10. Insert data from temp to target table
    copy_data_to_target = [
        PostgresTableCopyOperator(
            task_id=f"copy_data_to_target_{key}",
            dataset_name_lookup=DAG_ID,
            dataset_name=TABLE_DATASET_NAME,
            source_table_name=f"{TABLE_DATASET_NAME}_{key}_new",
            source_schema_name=tmp_database_schema,
            target_table_name=f"{TABLE_DATASET_NAME}_{key}",
            drop_target_if_unequal=False,
        )
        for key in tables_to_create.keys()
    ]


# FLOW
slack_at_start >> mkdir >> download_data

for data, drop in zip(download_data, drop_if_exists_tmp_tables):

    [data >> drop] >> Interface

Interface >> import_data

for data_into_temp_table in zip(import_data):

    data_into_temp_table >> provenance_translation

provenance_translation >> multi_checks >> Interface2

Interface2 >> copy_data_to_target

dag.doc_md = """
    #### DAG summary
    This DAG contains COVID19 Openbare Orde en Veiligheid related restricted areas
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/covid_19.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/covid_19.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=covid_19/covid_19&x=106434&y=488995&radius=10
"""
