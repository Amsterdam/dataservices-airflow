import logging
import operator
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    SLACK_ICON_START,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.path import mk_dir
from common.sql import SQL_DROP_TABLE, SQL_GEOMETRY_VALID
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from schematools.utils import to_snake_case

logger = logging.getLogger(__name__)
db_conn = DatabaseEngine()

DAG_ID: Final = "wegenbestand"
# Wegenbestand holds a sub categorie ZZV `zone zwaar verkeer` for
# some tables to be listed under.
DATASET_ID_ZZV: Final = "wegenbestandZoneZwaarVerkeer"
DATASET_ID_ZZV_DATABASE: Final = to_snake_case(DATASET_ID_ZZV)
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
variables: dict[str, dict[str, str]] = Variable.get(DAG_ID, deserialize_json=True)
files_to_download: dict[str, dict[str, str]] = variables["files_to_download"]  # type: ignore
data_values: dict[str, dict[str, str]] = variables["data_values"]  # type: ignore
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


with DAG(
    DAG_ID,
    description="""Wegenbestand Amsterdam bevat gegevens over stedelijke zones voor zwaarverkeer
                en tunnels/routes voor vervoer gevaarlijkestoffen.""",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"{SLACK_ICON_START} starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(TMP_DIR))

    # 3. download .geopackage data from data.amsterdam.nl
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{values['file_type']}_{resource_name}",
            endpoint=values["url"],
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{TMP_DIR}/{resource_name}.{values['file_type']}",
        )
        for resource_name, values in files_to_download.items()
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel
    # tasks with different number of lanes (without this intermediar, Airflow will raise an error)
    Interface = DummyOperator(task_id="interface")

    # 5a. path 1: import geopackage to database
    load_geopackage = [
        Ogr2OgrOperator(
            task_id=f"import_gpkg_layer_{values['table']}",
            sql_statement="SELECT *"  # noqa: S608
            f" FROM {quote_string(values['geopackage_layer'])}"
            f" WHERE zone_zzv = {quote_string(values['data_filter'])}",
            input_file=f"{TMP_DIR}/{resource_name}.{extension['file_type']}",
            t_srs="EPSG:28992",
            mode="PostgreSQL",
            nln_options=[f"{DATASET_ID_ZZV_DATABASE}_{values['table']}_new"],
            db_conn=db_conn,
        )
        for values in data_values.values()
        if values["source"] == "zone_zwaar_verkeer"
        for resource_name, extension in files_to_download.items()
        if resource_name == values["source"]
    ]

    # 5b. path 2: import geojson to database
    load_geojson = [
        Ogr2OgrOperator(
            task_id=f"import_geojson_{values['table']}",
            target_table_name=f"{DAG_ID}_{values['table']}_new",
            input_file=f"{TMP_DIR}/{values['table']}.{extension['file_type']}",
            t_srs="EPSG:28992",
            mode="PostgreSQL",
            db_conn=db_conn,
        )
        for values in data_values.values()
        if values["source"] == "wegenbestand"
        for table_name, extension in files_to_download.items()
        if table_name == values["table"]
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel
    # tasks with different number of lanes (without this intermediar, Airflow will raise an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 6. RENAME columns based on PROVENANCE
    # for sublevel zonezwaarverkeer
    provenance_trans = [
        ProvenanceRenameOperator(
            task_id=f"provenance_rename_{values}",
            dataset_name=DATASET_ID_ZZV if values == "zone_zwaar_verkeer" else DAG_ID,
            prefix_table_name=f"{DATASET_ID_ZZV_DATABASE}_"
            if values == "zone_zwaar_verkeer"
            else f"{DAG_ID}_",
            postfix_table_name="_new",
            rename_indexes=False,
            pg_schema="public",
        )
        for values in {values["source"] for values in data_values.values()}
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel
    # tasks with different number of lanes (without this intermediar, Airflow will raise an error)
    Interface3 = DummyOperator(task_id="interface3")

    # 7. Make geometry valid
    make_geo_valid = [
        PostgresOperator(
            task_id=f"make_geo_valid_{values['table']}",
            sql=SQL_GEOMETRY_VALID,
            params={
                "tablename": f"{DATASET_ID_ZZV_DATABASE}_{values['table']}_new"
                if values["source"] == "zone_zwaar_verkeer"
                else f"{DAG_ID}_{values['table']}_new",
                "geo_column": "geometry",
                "geom_type_number": "2",
            },
        )
        for values in data_values.values()
    ]

    # Prepare the checks and added them per source to a dictionary
    for values in data_values.values():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{values['table']}",
                pass_value=2,
                params={
                    "table_name": f"{DATASET_ID_ZZV_DATABASE}_{values['table']}_new"
                    if values["source"] == "zone_zwaar_verkeer"
                    else f"{DAG_ID}_{values['table']}_new"
                },
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{values['table']}",
                params={
                    "table_name": f"{DATASET_ID_ZZV_DATABASE}_{values['table']}_new"
                    if values["source"] == "zone_zwaar_verkeer"
                    else f"{DAG_ID}_{values['table']}_new",
                    "geotype": [
                        "POINT",
                        "MULTILINESTRING",
                    ],
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[values["table"]] = [*total_checks]

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{values['table']}",
            checks=check_name[values["table"]],
        )
        for values in data_values.values()
    ]

    # 9. Check for changes to merge in target table
    change_data_capture = [
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{values['table']}",
            dataset_name=DATASET_ID_ZZV,
            source_table_name=f"{DATASET_ID_ZZV_DATABASE}_{values['table']}_new"
            if values["source"] == "zone_zwaar_verkeer"
            else f"{DAG_ID}_{values['table']}_new",
            target_table_name=f"{DATASET_ID_ZZV_DATABASE}_{values['table']}"
            if values["source"] == "zone_zwaar_verkeer"
            else f"{DAG_ID}_{values['table']}",
            drop_target_if_unequal=True,
        )
        for values in data_values.values()
    ]

    # 10. Clean up (remove temp table _new)
    clean_ups = [
        PostgresOperator(
            task_id=f"clean_up_{values['table']}",
            sql=SQL_DROP_TABLE,
            params={
                "tablename": f"{DATASET_ID_ZZV_DATABASE}_{values['table']}_new"
                if values["source"] == "zone_zwaar_verkeer"
                else f"{DAG_ID}_{values['table']}_new"
            },
        )
        for values in data_values.values()
    ]

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)


# FLOW
slack_at_start >> mkdir >> download_data

for download in zip(download_data):
    [download]

# Path 1: geopackage import
download_data >> Interface >> load_geopackage

for import_geopackage in zip(load_geopackage):
    [import_geopackage]

# Path 2: geojson import
Interface >> load_geojson

for import_geojson in zip(load_geojson):
    [import_geojson]

# Merge path 1&2: and continue main path
load_geopackage >> Interface2
load_geojson >> Interface2 >> provenance_trans

for renames in zip(provenance_trans):
    [renames]

provenance_trans >> Interface3 >> make_geo_valid

for (geo_valid, multi_check, capture_data, clean_up) in zip(
    make_geo_valid, multi_checks, change_data_capture, clean_ups
):
    [geo_valid >> multi_check >> capture_data >> clean_up]

clean_ups >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about city accessibility routes/zones for heavy classified traffic.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner (Bas Bussink) at bereikbaarheidsthermometer@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/wegenbestand.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/wegenbestand.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=wegenbestand_zone_zwaar_verkeer/binnen&x=120320&y=488448&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=wegenbestand_zone_zwaar_verkeer/buiten&x=120320&y=488448&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=wegenbestand_zone_zwaar_verkeer/breed_opgezette_wegen&x=120320&y=488448&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=wegenbestand_zone_zwaar_verkeer/routes_gevaarlijke_stoffen&x=118601&y=490715&radius=1
    https://api.data.amsterdam.nl/geosearch?datasets=wegenbestand_zone_zwaar_verkeer/tunnels_gevaarlijke_stoffen&x=124630&y=480803&radius=1
"""
