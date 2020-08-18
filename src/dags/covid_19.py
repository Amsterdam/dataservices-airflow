import operator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

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

# to snake case is needed in target table because of the provenance check, because
# number are seen as a start for underscore seperator.Covid19 is therefore translated as covid_19
# to do: change logic for snake_case when dealing with numbers
dag_id = "covid_19"

variables_covid19 = Variable.get("covid19", deserialize_json=True)
files_to_download = variables_covid19["files_to_download"]
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
    description="alcohol-, straatartiest-, aanleg- en parkenverbodsgebieden, mondmaskerverplichtinggebieden, e.d.",
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
            # Default swift = Various Small Datasets objectstore
            #swift_conn_id="SWIFT_DEFAULT",
            container="covid19",
            object_id=f"{file}",
            output_path=f"{tmp_dir}/{file}",
        )
        for file in files_to_download       
    ]

    # 5. Create SQL
    SHP_to_SQL = BashOperator(
            task_id=f"create_SQL",
            bash_command=f"ogr2ogr -f 'PGDump' "
            f"-s_srs EPSG:28992 -t_srs EPSG:28992 "            
            f"-nln {dag_id} "
            f"{tmp_dir}/{dag_id}.sql {tmp_dir}/OOV_COVID19_totaal.shp "
            f"-lco GEOMETRY_NAME=geometry "
            f"-nlt PROMOTE_TO_MULTI "
            f"-lco precision=NO "
            f"-lco FID=id",            
        )
      

    # 6. Create TABLE
    create_table = BashOperator(
            task_id=f"create_table",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.sql",
        )    
    
    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dag_id}",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    total_checks.clear()
    count_checks.clear()
    geo_checks.clear()

    count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check",
                pass_value=2,
                params=dict(table_name=f"{dag_id}"),
                result_checker=operator.ge,
            )
        )

    geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check",
                params=dict(table_name=f"{dag_id}", geotype=["MULTIPOLYGON"],),
                pass_value=1,
            )
        )

    total_checks = count_checks + geo_checks
    check_name[f"{dag_id}"] = total_checks

    # 8. Execute bundled checks on database
    multi_check = PostgresMultiCheckOperator(
            task_id=f"multi_check", checks=check_name[f"{dag_id}"]
        )       
    
    # 9. drop exsisting table
    drop_table = PostgresOperator(
        task_id=f"drop_table", sql=[f"DROP TABLE IF EXISTS {dag_id}_{dag_id} CASCADE",],
    )

    # 10. Rename TABLE
    rename_table = PostgresTableRenameOperator(
            task_id=f"rename_table",
            old_table_name=f"{dag_id}",
            new_table_name=f"{dag_id}_{dag_id}",
        )
       

for data in zip(download_data):

    data >> SHP_to_SQL >> create_table >> provenance_translation >> multi_check >> drop_table >> rename_table 

slack_at_start >> mkdir >> download_data

dag.doc_md = """
    #### DAG summery
    This DAG containts COVID19 Openbare Orde en Veiligheid related restricted areas
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
