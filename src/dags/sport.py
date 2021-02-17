import operator
import re
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from ogr2ogr_operator import Ogr2OgrOperator
from http_fetch_operator import HttpFetchOperator
from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator
from typeahead_location_operator import TypeAHeadLocationOperator

from common import (
    default_args,
    pg_params,
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

from sql.sport import ADD_GEOMETRY_COL, DEL_ROWS

dag_id = "sport"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
files_to_download = variables["files_to_download"]
data_endpoints = variables["data_endpoints"]
files_to_SQL = {**files_to_download, **data_endpoints}
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


# remove carriage returns / newlines and double spaces in data
def clean_data(file_name):
    data = open(file_name, "r").read()
    remove_returns = re.sub(r"[\n\r]", "", data)
    remove_double_spaces = re.sub(r"[ ]{2,}", " ", remove_returns)
    # TODO: contact data maintainer to ask for encoding schema for
    # sportvelden specificly, other datasets can be correctly encoded.
    # the unicode replacement karakter is translated to the EM DASH sign
    # the source seems to have the replacement karakter in it without no
    # possibity to transform it anymore
    remove_unkown_karakter = re.sub("\uFFFD", "\u2014", remove_double_spaces)
    with open(file_name, "w") as output:
        output.write(remove_unkown_karakter)


with DAG(
    dag_id,
    description="sportfaciliteiten, -objecten en -aanbieders",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. create download temp directory to store the data
    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. download source 1: .csv or .geojson data from Objectstore
    download_data_obs = [
        HttpFetchOperator(
            task_id=f"download_obs_{file_name}_{source_type}",
            endpoint=f"{url}",
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{tmp_dir}/{file_name}.{re.sub('_.*', '', source_type)}",
            output_type="text",
            encoding_schema="UTF-8",
        )
        for source_type, data in files_to_download.items()
        for file_name, url in data.items()
    ]

    # 4. Download source 2: .geojson from Maps.amsterdam.nl
    download_data_maps = [
        HttpFetchOperator(
            task_id=f"download_maps_{file_name}_{source_type}",
            endpoint=f"{url}",
            http_conn_id="ams_maps_conn_id",
            tmp_file=f"{tmp_dir}/{file_name}.{re.sub('_.*', '', source_type)}",
            output_type="text",
        )
        for source_type, data in data_endpoints.items()
        for file_name, url in data.items()
    ]

    # 5. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 6.create the SQL for creating the table using ORG2OGR PGDump
    to_sql = [
        Ogr2OgrOperator(
            task_id=f"{source_type}_to_SQL_{file_name}",
            target_table_name=f"{dag_id}_{file_name}_new",
            sql_output_file=f"{tmp_dir}/{file_name}.sql",
            input_file=f"{tmp_dir}/{file_name}.{re.sub('_.*', '', source_type)}",
        )
        for source_type, data in files_to_SQL.items()
        for file_name in data.keys()
    ]

    # 7. Clean data from returns and spaces
    cleanse_data = [
        PythonOperator(
            task_id=f"cleanse_{file_name}",
            python_callable=clean_data,
            op_args=[
                f"{tmp_dir}/{file_name}.sql",
            ],
        )
        for _, data in files_to_SQL.items()
        for file_name in data.keys()
    ]

    # 8. Load data into the table
    load_tables = [
        BashOperator(
            task_id=f"load_table_{file_name}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{file_name}.sql",
        )
        for data in files_to_SQL.values()
        for file_name in data.keys()
    ]

    # 9. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different
    # number of lanes (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 10. Add geometry column for tables
    #    where source file (like the .csv files) has no geometry field, only x and y fields
    add_geom_col = [
        PostgresOperator(
            task_id=f"add_geom_column_{file_name}",
            sql=ADD_GEOMETRY_COL,
            params=dict(tablename=f"{dag_id}_{file_name}"),
        )
        for file_name in files_to_download["csv"].keys()
    ]

    # 11. RENAME columns based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 12. Look up geometry based on non-geometry data (i.e. address)
    lookup_geometry_typeahead = [
        TypeAHeadLocationOperator(
            task_id=f"lookup_geometry_if_not_exists_{file_name}",
            source_table=f"{dag_id}_{file_name}_new",
            source_location_column="adres",
            source_key_column="id",
            geometry_column="geometry",
        )
        for file_name in files_to_download["csv"].keys()
    ]

    # 13. delete duplicate rows in zwembad en sporthal, en gymzaal en sportzaal
    delete_duplicate_rows = PostgresOperator(
        task_id="del_duplicate_rows",
        sql=DEL_ROWS,
    )

    # Prepare the checks and added them per source to a dictionary
    for data in files_to_SQL.values():
        for file_name in data.keys():

            total_checks.clear()
            count_checks.clear()
            geo_checks.clear()

            count_checks.append(
                COUNT_CHECK.make_check(
                    check_id=f"count_check_{file_name}",
                    pass_value=2,
                    params=dict(table_name=f"{dag_id}_{file_name}_new"),
                    result_checker=operator.ge,
                )
            )

            geo_checks.append(
                GEO_CHECK.make_check(
                    check_id=f"geo_check_{file_name}",
                    params=dict(
                        table_name=f"{dag_id}_{file_name}_new",
                        geotype=[
                            "POINT",
                            "POLYGON",
                            "MULTIPOLYGON",
                            "MULTILINESTRING",
                            "LINESTRING",
                        ],
                    ),
                    pass_value=1,
                )
            )

            total_checks = count_checks + geo_checks
            check_name[f"{file_name}"] = total_checks

    # 14. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{file_name}", checks=check_name[f"{file_name}"]
        )
        for data in files_to_SQL.values()
        for file_name in data.keys()
    ]

    # 15. RENAME TABLES
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{file_name}",
            old_table_name=f"{dag_id}_{file_name}_new",
            new_table_name=f"{dag_id}_{file_name}",
        )
        for data in files_to_SQL.values()
        for file_name in data.keys()
    ]

    # FLOW. define flow with parallel executing of serial tasks for each file
    for data in download_data_obs:

        data >> Interface >> to_sql

    for data_maps in download_data_maps:

        data_maps >> Interface >> to_sql

    for (to_sql, cleanse_data, load_tables) in zip(to_sql, cleanse_data, load_tables):

        to_sql >> cleanse_data >> load_tables >> Interface2 >> add_geom_col

    for add_geometry, lookup_geometry in zip(add_geom_col, lookup_geometry_typeahead):

        add_geometry >> provenance_translation >> lookup_geometry >> delete_duplicate_rows >> multi_checks

    for multi_checks, rename_tables in zip(multi_checks, rename_tables):

        multi_checks >> rename_tables

    slack_at_start >> mk_tmp_dir >> download_data_obs
    slack_at_start >> mk_tmp_dir >> download_data_maps


dag.doc_md = """
    #### DAG summery
    This DAG containts data about sport related facilities, objects and providers
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/sport.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/sport.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=sport/zwembad&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=sport/hardlooproute&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=sport/sportaanbieder&x=106434&y=488995&radius=10
"""
