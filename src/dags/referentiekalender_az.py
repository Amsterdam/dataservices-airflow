import os
from typing import Final

from airflow import DAG
from airflow.operators.python import PythonOperator
from common import MessageOperator, default_args
from common.sql import SQL_CHECK_COUNT
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_referentiekalender import load_from_dwh
from postgres_check_operator import PostgresCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.referentiekalender import SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_BOR"]

DAG_ID: Final = "referentiekalender_az"
DATASET_ID: Final = "referentiekalender"

with DAG(
    DAG_ID,
    default_args={**default_args},
    schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
    description="""Generieke kalenderreferentie hierarchische gegevens met datum
        als laagste granulariteit.""",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2.
    # Load referentiekalender data into DB
    load_dwh = PythonOperator(
        task_id="load_from_dwh_stadsdelen",
        python_callable=load_from_dwh,
        provide_context=True,
        op_args=[f"{DATASET_ID}_datum_new", DATASET_ID],
    )

    # 3. Check minimum number of records
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params={"tablename": f"{DATASET_ID}_datum_new", "mincount": 1000},
    )

    # 4.
    # Rename COLUMNS based on provenance (if specified)
    provenance_dwh_data = ProvenanceRenameOperator(
        task_id="provenance_dwh",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 5.
    # Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = SqlAlchemyCreateObjectOperator(
        task_id="create_table",
        data_schema_name=DATASET_ID,
        data_table_name=f"{DATASET_ID}_datum",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 6.
    # Check for changes to merge in target table by using CDC
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name_lookup=DATASET_ID,
        source_table_name=f"{DATASET_ID}_datum_new",
        target_table_name=f"{DATASET_ID}_datum",
        drop_target_if_unequal=True,
    )

    # 7.
    # Clean up; delete temp table
    clean_up = PostgresOnAzureOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{DATASET_ID}_datum_new"},
    )

    # 8. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)


# FLOW
[
    slack_at_start
    >> load_dwh
    >> check_count
    >> provenance_dwh_data
    >> create_tables
    >> change_data_capture
    >> clean_up
    >> grant_db_permissions
]

dag.doc_md = """
    #### DAG summary
    This DAG processes a generic date hierarchy data.
    Orginates from DWH stadsdelen by means of a database connection.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/referentiekalender.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/referentiekalender.html
    Example geosearch:
    N.A.
"""
