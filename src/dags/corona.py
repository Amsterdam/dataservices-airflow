from pathlib import Path
from typing import Final, List, Set

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import DatabaseEngine
from common.path import mk_dir
from common.sql import SQL_CHECK_COUNT
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from http_fetch_operator import HttpFetchOperator
from importscripts.import_corona_gevallen import data_import_gevallen_opnames
from importscripts.import_corona_handhaving import data_import_handhaving
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator

env = Env()

DAG_ID: Final = "corona"
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
variables: dict[str, dict[str, str]] = Variable.get(DAG_ID, deserialize_json=True)
RIVM_data: dict[str, dict[str, str]] = variables["data_endpoints"]
OOV_data: dict[str, dict[str, str]] = variables["files_to_download"]
database_table_names: set = {value["dataset"] for value in OOV_data.values()} | {
    value["dataset"] for value in RIVM_data.values()
}
total_checks: list = []
count_checks: list = []
check_name: dict = {}
db_conn: object = DatabaseEngine()

with DAG(
    DAG_ID,
    description="corona gevallen, ziekenhuisopnames en handhavingsacties.",
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(TMP_DIR, clean_if_exists=False)

    # 3a. download handhaving data (OOV source)
    download_handhaving = [
        SFTPOperator(
            task_id=f"download_{name}",
            ssh_conn_id="OOV_BRIEVENBUS_CORONA",
            local_filepath=TMP_DIR / file["data"],
            remote_filepath=file["data"],
            operation="get",
            create_intermediate_dirs=True,
        )
        for name, file in OOV_data.items()
    ]

    # 3b. Import handhaving data (OOV source)
    import_OOV_data = PythonOperator(
        task_id=f"import_OOV_data",
        python_callable=data_import_handhaving,
        op_kwargs={
            "csv_file": TMP_DIR / OOV_data["handhaving"]["data"],
            "db_table_name": f"{DAG_ID}_handhaving_new",
        },
    )

    # 4a. Download gevallen en ziekenhuisopnames data (RIVM source)
    download_gevallen_ziekhuisopnames = [
        HttpFetchOperator(
            task_id=f"download_{name}",
            endpoint=file["data"],
            http_conn_id="RIVM_BASE_URL",
            tmp_file=TMP_DIR / file["data"].split("/")[2],
        )
        for name, file in RIVM_data.items()
    ]

    # 4b. Import gevallen en ziekenhuisopnames data (RIVM source)
    import_RIVM_data = PythonOperator(
        task_id=f"import_RIVM_data",
        python_callable=data_import_gevallen_opnames,
        op_kwargs={
            "source_data_gevallen": TMP_DIR / RIVM_data["gevallen"]["data"].split("/")[2],
            "source_data_ziekenhuis": TMP_DIR
            / RIVM_data["ziekenhuisopnames"]["data"].split("/")[2],
            "db_table_name": f"{DAG_ID}_gevallen_new",
        },
    )

    # 5. Dummy operator acts as an interface between parallel tasks to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 6. Rename COLUMNS based on provenance (if specified)
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 7. Rename TABLES
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{table_name}",
            old_table_name=f"{DAG_ID}_{table_name}_new",
            new_table_name=f"{DAG_ID}_{table_name}",
        )
        for table_name in database_table_names
    ]

    # 8. Check minimum number of records
    check_count = [
        PostgresCheckOperator(
            task_id=f"check_count_{table_name}",
            sql=SQL_CHECK_COUNT,
            params={"tablename": f"{DAG_ID}_{table_name}", "mincount": 10},
        )
        for table_name in database_table_names
    ]

    # 9. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    # FLOW
    # Path 1: OOV
    slack_at_start >> mkdir >> download_handhaving

    for handhaving in download_handhaving:
        handhaving >> import_OOV_data

    # Path 2: RIVM
    mkdir >> download_gevallen_ziekhuisopnames

    for gevallen in download_gevallen_ziekhuisopnames:
        gevallen >> import_RIVM_data

    # Merging path 1&2 back to main
    import_RIVM_data >> Interface
    import_OOV_data >> Interface

    Interface >> provenance_translation >> rename_tables

    for table, check in zip(rename_tables, check_count):
        [table >> check] >> grant_db_permissions

    grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about Corona (covid-19) related enforcement actions, infected cases and hosptial admissions.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/corona.html
"""
