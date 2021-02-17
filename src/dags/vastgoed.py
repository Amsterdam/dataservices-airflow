import operator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from provenance_rename_operator import ProvenanceRenameOperator
from postgres_rename_operator import PostgresTableRenameOperator
from swift_operator import SwiftOperator

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
)


dag_id = "vastgoed"
variables_vastgoed = Variable.get("vastgoed", deserialize_json=True)
files_to_download = variables_vastgoed["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
check_name = {}

with DAG(
    dag_id,
    description="verhuurbare eenheden van gemeentelijke vastgoed objecten",
    default_args=default_args,
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
    download_data = SwiftOperator(
            task_id=f"download_{files_to_download[0]}",
            # when swift_conn_id is ommitted then the default connection will be the VSD objectstore
            # swift_conn_id="SWIFT_DEFAULT",
            container="vastgoed",
            object_id=f"{files_to_download[0]}",
            output_path=f"{tmp_dir}/{files_to_download[0]}",
        )
    
    # 4. Convert data to UTF8 character set
    convert_to_UTF8 = BashOperator(
                      task_id="convert_to_UTF8",
                      bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{files_to_download[0]} > "
                                   f"{tmp_dir}/{dag_id}_utf8.csv",
    )

    # 5. Create TABLE from CSV
    # The source has no spatial data, but OGR2OGR is used to create the SQL insert statements.
    CSV_to_SQL = BashOperator(
            task_id=f"CSV_to_SQL",
            bash_command=f"ogr2ogr -f 'PGDump' "
            f"-s_srs EPSG:28992 -t_srs EPSG:28992 "
            f"-nln {dag_id}_{dag_id}_new "
            f"{tmp_dir}/{dag_id}.sql {tmp_dir}/{dag_id}_utf8.csv "          
            f"-lco SEPARATOR=SEMICOLON "
            f"-oo AUTODETECT_TYPE=YES "
            f"-lco FID=id "
            # remove empty records
            f"-sql \'SELECT * FROM {dag_id}_utf8 WHERE 1=1 AND \"bag pand id\" is not NULL \'",
        )

    # 6. Create TABLE
    insert_data = BashOperator(
            task_id=f"SQL_insert_data_{dag_id}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.sql",
        ) 
    
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
    total_checks.clear()
    count_checks.clear()   

    count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check",
                pass_value=20,
                params=dict(table_name=f"{dag_id}_{dag_id}_new"),
                result_checker=operator.ge,
            )
        )

    check_name[f"{dag_id}"] = count_checks

    # 8. Execute bundled checks on database (in this case just a count check)
    multi_checks = PostgresMultiCheckOperator(
            task_id=f"count_check_{dag_id}", checks=check_name[f"{dag_id}"]
        )    

    # 9. Rename TABLE
    rename_tables = PostgresTableRenameOperator(
            task_id=f"rename_table_{dag_id}",
            old_table_name=f"{dag_id}_{dag_id}_new",
            new_table_name=f"{dag_id}_{dag_id}",
        )    

slack_at_start >> mkdir >> download_data >> convert_to_UTF8 >> CSV_to_SQL >> insert_data >> provenance_translation >> multi_checks >> rename_tables

dag.doc_md = """
    #### DAG summery
    This DAG containts data of real estate objects of the city of Amsterdam
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/vastgoed.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/vastgoed.html
    Example geosearch: 
    not applicable
"""
