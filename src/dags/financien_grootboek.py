from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from importscripts.import_financien_grootboek import load_from_dwh
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from sql.financien_grootboek import SQL_DROP_TMP_TABLE

from common import (
    default_args,
    DATAPUNT_ENVIRONMENT,
    slack_webhook_token,
    MessageOperator,
)
from common.sql import SQL_CHECK_COUNT

DATASTORE_TYPE: str = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)

data_schema_id: str = "financien"
dag_id: str = "financien_grootboek"


with DAG(
    dag_id,
    default_args={**default_args},
    description="""Financiele gegevens uit de centrale administratie van het
                    Amsterdams Financieel Systeem (AFS) beschikbaar gesteld via
                    een datamart van DWH AMI (Amsterdamse Management Informatie).""",
    schedule_interval="0 8 * * *",
) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Load finance data into DB
    load_dwh = PythonOperator(
        task_id="load_from_ami_dwh",
        python_callable=load_from_dwh,
        op_args=[f"{dag_id}_new"],
    )

    # 3. Check minimum number of records
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=5000),
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
    change_data_capture = PgComparatorCDCOperator(
        task_id="change_data_capture",
        source_table=f"{dag_id}_new",
        target_table=dag_id,
        use_pg_copy=True,
        key_column="id",
        use_key=True,
    )

    # 7. Clean up; delete temp table
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params=dict(tablename=f"{dag_id}_new"),
    )

    # 8. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=data_schema_id)


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