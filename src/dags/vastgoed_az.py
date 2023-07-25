import operator
import os
from functools import partial
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from common import SHARED_DIR, MessageOperator, default_args
from common.db import pg_params
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.vastgoed import ADD_LEADING_ZEROS, CHANGE_DATA_TYPE
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "vastgoed_az"
DATASET_ID: Final = "vastgoed"
variables_vastgoed: Final = Variable.get("vastgoed", deserialize_json=True)
files_to_download: str = variables_vastgoed["files_to_download"]
tmp_dir: Final = Path(SHARED_DIR) / DATASET_ID
total_checks: list = []
count_checks: list = []
check_name: dict = {}

# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=DATASET_ID)

with DAG(
    DAG_ID,
    description="verhuurbare eenheden van gemeentelijke vastgoed objecten",
    default_args=default_args,
    schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
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

    # Deprecated since team SOEB creates the .csv in UTF8 themselfs.
    # 4. Convert data to UTF8 character set
    # convert_to_UTF8 = BashOperator(
    #     task_id="convert_to_UTF8",
    #     bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{files_to_download[0]} > "
    #     f"{tmp_dir}/{dag_id}_utf8.csv",
    # )

    # 5. Import data
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        input_file=f"{tmp_dir}/export_{DATASET_ID}.csv",
        s_srs="EPSG:28992",
        t_srs="EPSG:28992",
        input_file_sep="SEMICOLON",
        auto_detect_type="YES",
        geometry_name="geometry",
        fid="id",
        mode="PostgreSQL",
        # remove empty records
        sql_statement=f"""SELECT * FROM export_{DATASET_ID}
                WHERE \"bag_pand_id\" is not NULL""",  # noqa: S608
    )

    # 7. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
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
            params={"table_name": f"{DATASET_ID}_{DATASET_ID}_new"},
            result_checker=operator.ge,
        )
    )

    check_name[DATASET_ID] = count_checks

    # 8. Execute bundled checks on database (in this case just a count check)
    multi_checks = PostgresMultiCheckOperator(
        task_id=f"count_check_{DATASET_ID}", checks=check_name[DATASET_ID]
    )

    # 9. change datatype of columns
    change_data_type = PostgresOnAzureOperator(
        task_id="change_data_type",
        sql=CHANGE_DATA_TYPE,
        params={
            "tablename": f"{DATASET_ID}_{DATASET_ID}_new",
            "colname": ["pand_id", "verblijfsobject_id"],
            "coltype": "varchar",
        },
    )

    # 10. add leading zeros to data
    add_leading_zeros = PostgresOnAzureOperator(
        task_id="add_leading_zeros",
        sql=ADD_LEADING_ZEROS,
        params={
            "tablename": f"{DATASET_ID}_{DATASET_ID}_new",
            "colname": ["pand_id", "verblijfsobject_id"],
            "num_of_zero": "16",
        },
    )

    # 11. Rename TABLE
    rename_tables = PostgresTableRenameOperator(
        task_id=f"rename_table_{DATASET_ID}",
        old_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        new_table_name=f"{DATASET_ID}_{DATASET_ID}",
    )

    # 12. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

# FLOW
(
    slack_at_start
    >> mkdir
    >> download_data
    >> import_data
    >> provenance_translation
    >> multi_checks
    >> change_data_type
    >> add_leading_zeros
    >> rename_tables
    >> grant_db_permissions
)


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
