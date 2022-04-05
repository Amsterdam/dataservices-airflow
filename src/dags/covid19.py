import operator
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from common import SHARED_DIR, MessageOperator, default_args, pg_params, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

# Note: to snake case is needed in target table because of the provenance check, because
# number are seen as a start for underscore seperator. Covid19 is therefore translated as covid_19
# TODO: change logic for snake_case when dealing with numbers
dag_id = "covid19"
table_name = "covid_19"
variables_covid19 = Variable.get("covid19", deserialize_json=True)
files_to_download = variables_covid19["files_to_download"]

# Note: Gebiedsverbod is absolete since "nieuwe tijdelijke wetgeving Corona maatregelen 01-12-2020"
# TODO: remove Gebiedsverbod and Straatartiestverbod from var.yml, if DSO-API endpoint and
# Amsterdam Schema definition can be removed.
tables_to_create = variables_covid19["tables_to_create"]
tables_to_check = {
    k: v for k, v in tables_to_create.items() if k not in ("gebiedsverbod", "straatartiestverbod")
}

tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

with DAG(
    dag_id,
    description="type of restriction area's.",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="SWIFT_DEFAULT",
            container="covid19",
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
            f"-nln {key} "
            f"{tmp_dir}/{key}.sql {tmp_dir}/OOV_COVID19_totaal.shp "
            "-lco GEOMETRY_NAME=geometry "
            "-nlt PROMOTE_TO_MULTI "
            "-lco precision=NO "
            "-lco FID=id "
            f"-sql \"SELECT * FROM OOV_COVID19_totaal WHERE TYPE = '{code}'\"",
        )
        for key, code in tables_to_create.items()
    ]

    # 6. Create TABLE
    create_tables = [
        BashOperator(
            task_id=f"create_table_{key}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{key}.sql",
        )
        for key in tables_to_create.keys()
    ]

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        rename_indexes=False,
        pg_schema="public",
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
                params=dict(table_name=key),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params=dict(
                    table_name=key,
                    geotype=["MULTIPOLYGON"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in tables_to_check.keys()
    ]

    # 9. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 10. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=key,
            new_table_name=f"{table_name}_{key}",
        )
        for key in tables_to_create.keys()
    ]

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> download_data

for data in zip(download_data):

    data >> Interface

Interface >> SHP_to_SQL

for (create_SQL, create_table, rename_table,) in zip(
    SHP_to_SQL,
    create_tables,
    rename_tables,
):

    [create_SQL >> create_table] >> provenance_translation

provenance_translation >> multi_checks >> Interface2

Interface2 >> rename_tables

rename_tables >> grant_db_permissions

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
