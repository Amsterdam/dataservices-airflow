import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params,
    quote_string,
    slack_webhook_token,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

dag_id = "overlastgebieden"

variables_overlastgebieden = Variable.get("overlastgebieden", deserialize_json=True)
files_to_download = variables_overlastgebieden["files_to_download"]
tables_to_create = variables_overlastgebieden["tables_to_create"]
# Note: Vuurwerkvrijezones (VVZ) data is temporaly! not processed due to covid19 national measures
tables_to_check = {k: v for k, v in tables_to_create.items() if k != "vuurwerkvrij"}
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


with DAG(
    dag_id,
    description="alcohol-, straatartiest-, aanleg- en parkenverbodsgebieden, mondmaskerverplichtinggebieden, e.d.",
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
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            # Default swift = Various Small Datasets objectstore
            # swift_conn_id="SWIFT_DEFAULT",
            container="overlastgebieden",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for file in files_to_download
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Create SQL
    SHP_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{key}",
            bash_command="ogr2ogr -f 'PGDump' "
            "-s_srs EPSG:28992 -t_srs EPSG:28992 "
            f"-nln {dag_id}_{key}_new "
            f"{tmp_dir}/{dag_id}_{key}_new.sql {tmp_dir}/OOV_gebieden_totaal.shp "
            "-lco GEOMETRY_NAME=geometry "
            "-nlt PROMOTE_TO_MULTI "
            "-lco precision=NO "
            "-lco FID=id "
            f"-sql \"SELECT * FROM OOV_gebieden_totaal WHERE 1=1 AND TYPE = '{code}'\" ",
        )
        for key, code in tables_to_create.items()
    ]

    # 6. Create TABLE
    create_tables = [
        BashOperator(
            task_id=f"create_table_{key}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}_{key}_new.sql",
        )
        for key in tables_to_create.keys()
    ]

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 8. Revalidate invalid geometry records
    # the source has some invalid records
    # to do: inform the source maintainer
    remove_null_geometry_records = [
        PostgresOperator(
            task_id=f"remove_null_geom_records_{key}",
            sql=[
                f"UPDATE {dag_id}_{key}_new SET geometry = ST_CollectionExtract((st_makevalid(geometry)),3) WHERE 1=1 AND ST_IsValid(geometry) is false; COMMIT;",
            ],
        )
        for key in tables_to_create.keys()
    ]

    # Prepare the checks and added them per source to a dictionary
    for key in tables_to_check.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=2,
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

    # 9. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in tables_to_check.keys()
    ]

    # 10. Dummy operator acts as an interface between parallel tasks to another parallel
    #     tasks with different number of lanes (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 11. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=f"{dag_id}_{key}_new",
            new_table_name=f"{dag_id}_{key}",
        )
        for key in tables_to_create.keys()
    ]

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> download_data

for data in zip(download_data):

    data >> Interface

Interface >> SHP_to_SQL

for (create_SQL, create_table, remove_null_geometry_record,) in zip(
    SHP_to_SQL,
    create_tables,
    remove_null_geometry_records,
):

    [create_SQL >> create_table >> remove_null_geometry_record] >> provenance_translation

provenance_translation >> multi_checks >> Interface2 >> rename_tables

rename_tables >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about nuisance areas (overlastgebieden) i.e. vuurwerkvrijezones, dealeroverlastgebieden, barbecueverbodgebiedeb, etc.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/overlastgebieden.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/overlastgebieden.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=overlastgebieden/vuurwerkvrij&x=106434&y=488995&radius=10
"""
