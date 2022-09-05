from airflow import DAG
from airflow.operators.python import PythonOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import MessageOperator, default_args
from common.sql import SQL_CHECK_COUNT
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_financien_grootboek import load_from_dwh
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.financien_grootboek import SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

data_schema_id: str = "financien"
dag_id: str = "financien_grootboek"


with DAG(
    dag_id,
    default_args=default_args,
    description="""Financiele gegevens uit de centrale administratie van het
                    Amsterdams Financieel Systeem (AFS) beschikbaar gesteld via
                    een datamart van DWH AMI (Amsterdamse Management Informatie).""",
    schedule_interval="0 8 * * *",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Load finance data into DB
    load_dwh = PythonOperator(
        task_id="load_from_ami_dwh",
        python_callable=load_from_dwh,
        provide_context=True,
        op_args=[dag_id, f"{dag_id}_new"],
    )

    # 3. Check minimum number of records
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params={"tablename": f"{dag_id}_new", "mincount": 5000},
    )

    # 4. Rename COLUMNS based on provenance (if specified)
    provenance_dwh_data = ProvenanceRenameOperator(
        task_id="provenance_dwh",
        dataset_name=data_schema_id,
        prefix_table_name=f"{data_schema_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 5.
    # Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = SqlAlchemyCreateObjectOperator(
        task_id="create_table",
        data_schema_name=data_schema_id,
        data_table_name=dag_id,
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 6. Check for changes to merge in target table by using CDC
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name_lookup=dag_id,
        source_table_name=f"{dag_id}_new",
        target_table_name=dag_id,
        drop_target_if_unequal=True,
    )

    # 7. Clean up; delete temp table
    clean_up = PostgresOnAzureOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{dag_id}_new"},
    )

    # 8. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=data_schema_id)


# FLOW
(
    slack_at_start
    >> load_dwh
    >> check_count
    >> provenance_dwh_data
    >> create_tables
    >> change_data_capture
    >> clean_up
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG processes finance related data.
    Orginates from DWH AMI (Amsterdamse Management Informatie) by means of a database connection.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at ami@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/financien.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/financien.html
    Example geosearch:
    N.A.
"""
