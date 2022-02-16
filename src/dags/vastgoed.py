import operator
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from common import (
    OTAP_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    pg_params,
    slack_webhook_token,
)
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from swift_operator import SwiftOperator

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
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f":runner: Starting {dag_id} ({OTAP_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = SwiftOperator(
        task_id=f"download_{files_to_download[0]}",
        swift_conn_id="SWIFT_DEFAULT",
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
        task_id="CSV_to_SQL",
        bash_command="ogr2ogr -f 'PGDump' "
        "-s_srs EPSG:28992 -t_srs EPSG:28992 "
        f"-nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {tmp_dir}/{dag_id}_utf8.csv "
        "-lco SEPARATOR=SEMICOLON "
        "-oo AUTODETECT_TYPE=YES "
        "-lco FID=id "
        # remove empty records
        f"-sql 'SELECT * FROM {dag_id}_utf8 WHERE \"bag pand id\" is not NULL '",
    )

    # 6. Create TABLE
    insert_data = BashOperator(
        task_id=f"SQL_insert_data_{dag_id}",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.sql",
    )

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
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
            check_id="count_check",
            pass_value=20,
            params=dict(table_name=f"{dag_id}_{dag_id}_new"),
            result_checker=operator.ge,
        )
    )

    check_name[dag_id] = count_checks

    # 8. Execute bundled checks on database (in this case just a count check)
    multi_checks = PostgresMultiCheckOperator(
        task_id=f"count_check_{dag_id}", checks=check_name[dag_id]
    )

    # 9. Rename TABLE
    rename_tables = PostgresTableRenameOperator(
        task_id=f"rename_table_{dag_id}",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 10. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

[
    slack_at_start
    >> mkdir
    >> download_data
    >> convert_to_UTF8
    >> CSV_to_SQL
    >> insert_data
    >> provenance_translation
    >> multi_checks
    >> rename_tables
    >> grant_db_permissions
]


dag.doc_md = """
    #### DAG summary
    This DAG contains data of real estate objects of the city of Amsterdam
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
