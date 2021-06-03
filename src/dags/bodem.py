import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.bodem_set_geo_datatype import SET_GEOM
from swift_operator import SwiftOperator

dag_id = "bodem"
variables_bodem = Variable.get("bodem", deserialize_json=True)
files_to_download = variables_bodem["files_to_download"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="bodem analyse onderzoeken",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id)
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
            task_id=f"download_file_{key}",
            swift_conn_id="OBJECTSTORE_MILIEUTHEMAS",
            container="Milieuthemas",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for key, file in files_to_download.items()
    ]

    # 4. Transform seperator from pipeline to semicolon and set code schema to UTF-8
    change_seperator = [
        BashOperator(
            task_id=f"change_seperator_{key}",
            bash_command=f"cat {tmp_dir}/{file} | sed 's/|/;/g' > {tmp_dir}/seperator_{file} ;"
            f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/seperator_{file} > "
            f"{tmp_dir}/utf-8_{file}",
        )
        for key, file in files_to_download.items()
    ]
    # 5. Create SQL
    csv_to_SQL = [
        BashOperator(
            task_id=f"create_SQL_{key}",
            bash_command="ogr2ogr -f 'PGDump' "
            "-t_srs EPSG:28992 "
            f"-nln {key} "
            f"{tmp_dir}/{dag_id}_{key}.sql {tmp_dir}/utf-8_{file} "
            "-lco SEPARATOR=SEMICOLON "
            "-oo AUTODETECT_TYPE=YES "
            "-lco FID=ID",
        )
        for key, file in files_to_download.items()
    ]

    # 6. Create TABLE
    create_tables = [
        BashOperator(
            task_id=f"create_table_{key}",
            bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}_{key}.sql",
        )
        for key, _ in files_to_download.items()
    ]

    # 7. RE-define GEOM type (because ogr2ogr cannot set geom with .csv import)
    redefine_geom = [
        PostgresOperator(
            task_id=f"re-define_geom_{key}",
            sql=SET_GEOM,
            params=dict(tablename=key),
        )
        for key, _ in files_to_download.items()
    ]

    # 8. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        rename_indexes=False,
        pg_schema="public",
    )

    # 9. Drop Exisiting TABLE
    drop_tables = [
        PostgresOperator(
            task_id=f"drop_existing_table_{key}",
            sql=[
                f"DROP TABLE IF EXISTS {dag_id}_{key} CASCADE",
            ],
        )
        for key in files_to_download.keys()
    ]

    # 10. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=key,
            new_table_name=f"{dag_id}_{key}",
        )
        for key in files_to_download.keys()
    ]

    # Prepare the checks and added them per source to a dictionary
    for key in files_to_download.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=2,
                params=dict(table_name=key),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params=dict(
                    table_name=key,
                    geotype=["POINT"],
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 11. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in files_to_download.keys()
    ]

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

for (
    data,
    change_seperator,
    create_SQL,
    create_table,
    redefine_geom,
    multi_check,
    drop_table,
    rename_table,
) in zip(
    download_data,
    change_seperator,
    csv_to_SQL,
    create_tables,
    redefine_geom,
    multi_checks,
    drop_tables,
    rename_tables,
):

    [
        data >> change_seperator >> create_SQL >> create_table >> redefine_geom
    ] >> provenance_translation

    [multi_check >> drop_table >> rename_table]


provenance_translation >> multi_checks

rename_tables >> grant_db_permissions

(slack_at_start >> mkdir >> download_data)

dag.doc_md = """
    #### DAG summary
    This DAG contains soil (bodem) analyses data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/bodem/grond/
    https://api.data.amsterdam.nl/v1/bodem/grondwater/
    https://api.data.amsterdam.nl/v1/bodem/asbest/
"""
