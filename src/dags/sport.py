import operator
import re
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from collections import defaultdict
from common.db import DatabaseEngine
from ogr2ogr_operator import Ogr2OgrOperator
from http_fetch_operator import HttpFetchOperator
from provenance_rename_operator import ProvenanceRenameOperator
from typeahead_location_operator import TypeAHeadLocationOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

from sql.sport import ADD_GEOMETRY_COL, DEL_ROWS, SQL_DROP_TMP_TABLE
from importscripts.import_sport import add_unique_id_to_csv, add_unique_id_to_geojson  # noqa


# business / source keys
COMPOSITE_KEYS = {
    "csv": "add_unique_id_to_csv",
    "resources_csv": {
        "zwembad": ("Naam", "Type"),
        "sportaanbieder": (
            "Sport",
            "Naam",
            "Naam accommodatie",
            "Adres accommodatie",
            "Stadsdeel",
            "Website",
        ),
        "gymsportzaal": ("Naam", "Type"),
        "sporthal": ("Naam", "Type"),
    },
    "geojson": "add_unique_id_to_geojson",
    "resources_geojson": {
        "hardlooproute": ("name",),
        "sportveld": ("GUID",),
        "sportpark": ("CODE",),
        "openbaresportplek": ("id",),
    },
}

dag_id = "sport"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"/tmp/{dag_id}"
files_to_download = variables["files_to_download"]
data_endpoints = variables["data_endpoints"]
files_to_import = defaultdict(list)
db_conn = DatabaseEngine()
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


# populate all resources to proces for unique id generation
for resource_type, resources in files_to_download.items():
    for resource in resources:
        files_to_import[resource_type].append(resource)

for resource_type, resources in data_endpoints.items():
    for resource in resources:
        files_to_import[resource_type].append(resource)


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


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
            task_id=f"download_obs_{resource}_{resource_type}",
            endpoint=f"{url}",
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{tmp_dir}/{resource}.{re.sub('_.*', '', resource_type)}",
            output_type="text",
            encoding_schema="UTF-8",
        )
        for resource_type, resources in files_to_download.items()
        for resource, url in resources.items()
    ]

    # 4. Download source 2: .geojson from Maps.amsterdam.nl
    download_data_maps = [
        HttpFetchOperator(
            task_id=f"download_maps_{resource}_{resource_type}",
            endpoint=f"{url}",
            http_conn_id="ams_maps_conn_id",
            tmp_file=f"{tmp_dir}/{resource}.{re.sub('_.*', '', resource_type)}",
            output_type="text",
        )
        for resource_type, resources in data_endpoints.items()
        for resource, url in resources.items()
    ]

    # 5. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 6. Create unique ID for all sets based on a resource value(s)
    unique_id = [
        PythonOperator(
            task_id=f"add_id_{resource}",
            python_callable=globals()[COMPOSITE_KEYS[resource_format]],
            op_kwargs={
                "file": f"{tmp_dir}/{resource}.{resource_format}",
                "composite_key": COMPOSITE_KEYS[f"resources_{resource_format}"][resource],
            },
        )
        for resource_format, resources in files_to_import.items()
        for resource in resources
    ]

    # 7. convert geojson to csv
    load_data = [
        Ogr2OgrOperator(
            task_id=f"import_{resource}",
            target_table_name=f"{dag_id}_{resource}_new",
            input_file=f"{tmp_dir}/{resource}.{resource_format}",
            s_srs="EPSG:4326",
            t_srs="EPSG:28992",
            input_file_sep="SEMICOLON",
            auto_dect_type="YES",
            geometry_name="geometry",
            mode="PostgreSQL",
            fid="id",
            db_conn=db_conn,
        )
        for resource_format, resources in files_to_import.items()
        for resource in resources
    ]

    # 8. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 9. Add geometry column for tables
    # where source file (like the .csv files) has no geometry field, only x and y fields
    add_geom_col = [
        PostgresOperator(
            task_id=f"add_geom_column_{resource}",
            sql=ADD_GEOMETRY_COL,
            params=dict(tablename=f"{dag_id}_{resource}"),
        )
        for resource in files_to_download["csv"].keys()
    ]

    # 10. RENAME columns based on PROVENANCE
    provenance_trans = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 11. Look up geometry based on non-geometry data (i.e. address)
    lookup_geometry_typeahead = [
        TypeAHeadLocationOperator(
            task_id=f"lookup_geometry_if_not_exists_{resource}",
            source_table=f"{dag_id}_{resource}_new",
            source_location_column="adres",
            source_key_column="id",
            geometry_column="geometry",
        )
        for resource in files_to_download["csv"].keys()
    ]

    # 12. delete duplicate rows in zwembad en sporthal, en gymzaal en sportzaal
    del_dupl_rows = PostgresOperator(
        task_id="del_duplicate_rows",
        sql=DEL_ROWS,
    )

    # Prepare the checks and added them per source to a dictionary
    for resources in files_to_import.values():
        for resource in resources:

            total_checks.clear()
            count_checks.clear()
            geo_checks.clear()

            count_checks.append(
                COUNT_CHECK.make_check(
                    check_id=f"count_check_{resource}",
                    pass_value=2,
                    params=dict(table_name=f"{dag_id}_{resource}_new"),
                    result_checker=operator.ge,
                )
            )

            geo_checks.append(
                GEO_CHECK.make_check(
                    check_id=f"geo_check_{resource}",
                    params=dict(
                        table_name=f"{dag_id}_{resource}_new",
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
            check_name[f"{resource}"] = total_checks

    # 13. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{resource}", checks=check_name[f"{resource}"]
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 14. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_{resource}_based_upon_schema",
            data_schema_name=f"{dag_id}",
            data_table_name=f"{dag_id}_{resource}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 15. Check for changes to merge in target table
    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_{resource}",
            source_table=f"{dag_id}_{resource}_new",
            target_table=f"{dag_id}_{resource}",
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 16. Clean up (remove temp table _new)
    clean_up = [
        PostgresOperator(
            task_id=f"clean_up_{resource}",
            sql=SQL_DROP_TMP_TABLE,
            params=dict(tablename=f"{dag_id}_{resource}_new"),
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # FLOW. define flow with parallel executing of serial tasks for each file
    for data in download_data_obs:

        data >> Interface >> unique_id

    for data_maps in download_data_maps:

        data_maps >> Interface >> unique_id

    for (create_id, import_data) in zip(unique_id, load_data):

        [create_id >> import_data] >> Interface2 >> add_geom_col

    for (add_geom, lookup_geom) in zip(add_geom_col, lookup_geometry_typeahead):

        add_geom >> provenance_trans >> lookup_geom >> del_dupl_rows >> multi_checks

    for (multi_check, create_table, check_changes, clean_up) in zip(
        multi_checks, create_tables, change_data_capture, clean_up
    ):

        [multi_check >> create_table >> check_changes >> clean_up]

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
