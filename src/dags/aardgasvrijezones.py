import operator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from environs import Env

from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator
from swift_operator import SwiftOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)


dag_id = "aardgasvrijezones"
variables_aardgasvrijezones = Variable.get("aardgasvrijezones", deserialize_json=True)
files_to_download = variables_aardgasvrijezones["files_to_download"]
tmp_dir = f"/tmp/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="(deels) gerealiseerde of geplande aardgasvrije buurten, en buurtinitiatieven",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
    template_searchpath=["/"],
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
            #  when swift_conn_id is ommitted then the default connection will be the VSD objectstore
            #  swift_conn_id="SWIFT_DEFAULT",
            container="aardgasvrij",
            object_id=f"{file}",
            output_path=f"{tmp_dir}/{file}",
        )
        for files in files_to_download.values()
        for file in files
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Create SQL
    SHP_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{key}",
            bash_command=f"ogr2ogr -f 'PGDump' "
            f"-t_srs EPSG:28992 -s_srs EPSG:28992 "
            f"-nln {dag_id}_{key}_new "
            f"{tmp_dir}/{key}.sql {tmp_dir}/{file} "
            f"-lco GEOMETRY_NAME=geometry "
            f"-nlt PROMOTE_TO_MULTI "
            f"-lco FID=id",
        )
        for key, files in files_to_download.items()
        for file in files
        if "shp" in file
    ]

    # 6. Create TABLE
    create_tables = [
        BashOperator(
            task_id=f"create_table_{key}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{key}.sql",
        )
        for key in files_to_download.keys()
    ]

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    for key in files_to_download.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=2,
                params=dict(table_name=f"{dag_id}_{key}_new "),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params=dict(
                    table_name=f"{dag_id}_{key}_new ",
                    geotype=["MULTIPOINT", "MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[f"{key}"] = total_checks

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{key}", checks=check_name[f"{key}"]
        )
        for key in files_to_download.keys()
    ]

    # 9. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=f"{dag_id}_{key}_new",
            new_table_name=f"{dag_id}_{key}",
        )
        for key in files_to_download.keys()
    ]


for data in zip(download_data):

    data >> Interface >> SHP_to_SQL

for (create_SQL, create_table, multi_check, rename_table,) in zip(
    SHP_to_SQL, create_tables, multi_checks, rename_tables,
):

    [create_SQL >> create_table] >> provenance_translation >> multi_checks

    [multi_check >> rename_table]

slack_at_start >> mkdir >> download_data

dag.doc_md = """
    #### DAG summery
    This DAG containts data of natural gas free districts (aardgasvrije buurten) and local initiatives 
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/aardgasvrijezones.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/aardgasvrijezones.html
    Example geosearch: 
    https://api.data.amsterdam.nl/geosearch?datasets=aardgasvrijezones/buurt&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=aardgasvrijezones/buurtinitiatief&x=106434&y=488995&radius=10
"""
