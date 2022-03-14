import operator
import re
from collections import defaultdict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    OTAP_ENVIRONMENT,
    SHARED_DIR,
    SLACK_ICON_START,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.sql import SQL_DROP_TABLE, SQL_GEOMETRY_VALID
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator

# The imported functions are taken from globals(), hence the noqa.
from importscripts.import_sport import add_unique_id_to_csv, add_unique_id_to_geojson  # noqa: F401
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.sport import ADD_GEOMETRY_COL, DEL_ROWS
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from typeahead_location_operator import TypeAHeadLocationOperator

# business / source keys
COMPOSITE_KEYS: dict = {
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
tmp_dir = f"{SHARED_DIR}/{dag_id}"
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


def clean_data(file_name: str) -> None:
    """The unicode replacement karakter is translated to the EM DASH sign.

    Args:
        file_name: Name of file to clean up

    Executes:
        Writes the cleaned data back into the file

    TODO: contact data maintainer to ask for encoding schema for
    sportvelden specificly, other datasets can be correctly encoded.
    """
    data = open(file_name).read()
    remove_returns = re.sub(r"[\n\r]", "", data)
    remove_double_spaces = re.sub(r"[ ]{2,}", " ", remove_returns)
    remove_unkown_karakter = re.sub("\uFFFD", "\u2014", remove_double_spaces)
    with open(file_name, "w") as output:
        output.write(remove_unkown_karakter)


with DAG(
    dag_id,
    description="sportfaciliteiten, -objecten en -aanbieders",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"{SLACK_ICON_START} Starting {dag_id} ({OTAP_ENVIRONMENT})",
        username="admin",
    )

    # 2. create download temp directory to store the data
    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. download source 1: .csv or .geojson data from Objectstore
    download_data_obs = [
        HttpFetchOperator(
            task_id=f"download_obs_{resource}_{resource_type}",
            endpoint=url,
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
            endpoint=url,
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

    # 7. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 8. Clean data
    cleanse_data = [
        PythonOperator(
            task_id=f"cleanse_{resource}",
            python_callable=clean_data,
            op_args=[
                f"{tmp_dir}/{resource}.geojson",
            ],
        )
        for resource in files_to_download["geojson"].keys()
    ]

    # 9. convert geojson to csv
    load_data = [
        Ogr2OgrOperator(
            task_id=f"import_{resource}",
            target_table_name=f"{dag_id}_{resource}_new",
            input_file=f"{tmp_dir}/{resource}.{resource_format}",
            # the lat long in source openbaresportplek are twisted.
            # So we need to correct it by switching it in reverse
            s_srs="+proj=latlong +datum=WGS84 +axis=neu +wktext"
            if resource == "openbaresportplek"
            else "EPSG:4326",
            t_srs="EPSG:28992",
            input_file_sep="SEMICOLON",
            auto_detect_type="YES",
            geometry_name="geometry",
            mode="PostgreSQL",
            fid="id",
            db_conn=db_conn,
        )
        for resource_format, resources in files_to_import.items()
        for resource in resources
    ]

    # 10. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface3 = DummyOperator(task_id="interface3")

    # 11. Add geometry column for tables
    # where source file (like the .csv files) has no geometry field, only x and y fields
    add_geom_col = [
        PostgresOperator(
            task_id=f"add_geom_column_{resource}",
            sql=ADD_GEOMETRY_COL,
            params={"tablename": f"{dag_id}_{resource}"},
        )
        for resource in files_to_download["csv"].keys()
    ]

    # 12. RENAME columns based on PROVENANCE
    provenance_trans = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 13. Look up geometry based on non-geometry data (i.e. address)
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

    # 14. delete duplicate rows in zwembad en sporthal, en gymzaal en sportzaal
    del_dupl_rows = PostgresOperator(
        task_id="del_duplicate_rows",
        sql=DEL_ROWS,
    )

    # 15. Make geometry valid
    geom_valid = [
        PostgresOperator(
            task_id=f"geom_valid_{resource}",
            sql=SQL_GEOMETRY_VALID,
            params={"tablename": f"{dag_id}_{resource}_new"},
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

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
                    params={"table_name": f"{dag_id}_{resource}_new"},
                    result_checker=operator.ge,
                )
            )

            geo_checks.append(
                GEO_CHECK.make_check(
                    check_id=f"geo_check_{resource}",
                    params={
                        "table_name": f"{dag_id}_{resource}_new",
                        "geotype": [
                            "POINT",
                            "POLYGON",
                            "MULTIPOLYGON",
                            "MULTILINESTRING",
                            "LINESTRING",
                        ],
                    },
                    pass_value=1,
                )
            )

            total_checks = count_checks + geo_checks
            check_name[resource] = [*total_checks]

    # 16. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{resource}", checks=check_name[resource])
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 17. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_{resource}_based_upon_schema",
            data_schema_name=dag_id,
            data_table_name=f"{dag_id}_{resource}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 18. Check for changes to merge in target table
    change_data_capture = [
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{resource}",
            dataset_name=dag_id,
            source_table_name=f"{dag_id}_{resource}_new",
            target_table_name=f"{dag_id}_{resource}",
            drop_target_if_unequal=True,
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 19. Clean up (remove temp table _new)
    clean_ups = [
        PostgresOperator(
            task_id=f"clean_up_{resource}",
            sql=SQL_DROP_TABLE,
            params={"tablename": f"{dag_id}_{resource}_new"},
        )
        for resources in files_to_import.values()
        for resource in resources
    ]

    # 20. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


# FLOW. define flow with parallel executing of serial tasks for each file
slack_at_start >> mk_tmp_dir >> (download_data_obs + download_data_maps)

for data in download_data_obs:
    data >> Interface

for data_maps in download_data_maps:
    data_maps >> Interface

Interface >> cleanse_data >> Interface2 >> unique_id

for (create_id, import_data) in zip(unique_id, load_data):
    [create_id >> import_data] >> Interface3

Interface3 >> add_geom_col

add_geom_col >> provenance_trans >> lookup_geometry_typeahead >> del_dupl_rows >> geom_valid

for (geom_validate, multi_check, create_table, check_changes, clean_up) in zip(
    geom_valid, multi_checks, create_tables, change_data_capture, clean_ups
):

    [geom_validate >> multi_check >> create_table >> check_changes >> clean_up]

clean_ups >> grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains data about sport related facilities, objects and providers
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
