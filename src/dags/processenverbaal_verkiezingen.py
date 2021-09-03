import operator
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.objectstore import fetch_objectstore_credentials
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_processenverbaalverkiezingen import save_data
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.processenverbaalverkiezingen import SQL_DROP_TMP_TABLE, SQL_REDEFINE_PK
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

dag_id: str = "processenverbaalverkiezingen"
schema_name: str = "verkiezingen"
table_name: str = "processenverbaal"
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
data_file: str = f"{SHARED_DIR}/{dag_id}/{dag_id}.csv"
conn: dict = fetch_objectstore_credentials("OBJECTSTORE_PROCESSENVERBAALVERKIEZINGEN")
conn_id: str = "OBJECTSTORE_PROCESSENVERBAALVERKIEZINGEN"
tenant: str = conn["TENANT_ID"]
base_url = URL(f"https://{tenant}.objectstore.eu")
db_conn: object = DatabaseEngine()
count_checks: list = []
check_name: dict = {}


with DAG(
    dag_id,
    default_args=default_args,
    description="Processenverbaal publicaties verkiezingen (pdf's in objectstore).",
    # every ten minutes the data is refreshed
    schedule_interval="*/10 * * * *",
    catchup=False,
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id="verkiezingen"),
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
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Get listing uploaded Processenverbaal
    get_file_listing = PythonOperator(
        task_id="get_file_listing",
        python_callable=save_data,
        op_kwargs={
            "start_folder": table_name,
            "output_file": data_file,
            "conn_id": conn_id,
            "base_url": base_url,
        },
    )

    # 5. Load data
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{schema_name}_{table_name}_new",
        input_file=data_file,
        s_srs=None,
        auto_detect_type="YES",
        mode="PostgreSQL",
        fid="fid",
        db_conn=db_conn,
    )

    # 6. Redefine PK
    set_pk = PostgresOperator(
        task_id="set_pk",
        sql=SQL_REDEFINE_PK,
        params={"tablename": f"{schema_name}_{table_name}_new"},
    )

    # 7. RENAME columns based on PROVENANCE
    provenance_trans = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=schema_name,
        prefix_table_name=f"{schema_name}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks
    count_checks.clear()

    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=1,
            params={"table_name": f"{schema_name}_{table_name}_new"},
            result_checker=operator.ge,
        )
    )

    check_name[dag_id] = count_checks

    # 8. Execute bundled checks on database (in this case just a count check)
    count_check = PostgresMultiCheckOperator(task_id="count_check", checks=check_name[dag_id])

    # 9. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_target_table = SqlAlchemyCreateObjectOperator(
        task_id="create_target_table_based_upon_schema",
        data_schema_name=schema_name,
        data_table_name=f"{schema_name}_{table_name}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 10. Check for changes to merge in target table
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name=schema_name,
        source_table_name=f"{schema_name}_{table_name}_new",
        target_table_name=f"{schema_name}_{table_name}",
        drop_target_if_unequal=True,
    )

    # 11. Clean up (remove temp table _new)
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{schema_name}_{table_name}_new"},
    )

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW
(
    slack_at_start
    >> mkdir
    >> get_file_listing
    >> import_data
    >> set_pk
    >> provenance_trans
    >> count_check
    >> create_target_table
    >> change_data_capture
    >> clean_up
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains URI's (and metadata) of 'processenverbaal verkiezingen'
    document publications. This DAG supposed to run for a short period after
    the elections. When all documents (processenverbaal) have been uploaded to
    the objectstore, this dag can be set inactive. When active, it wil run
    every 10 minutes.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/verkiezingen/processenverbaal.html
    Example geosearch:
    N.A.
"""
