import operator
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from more_ds.network.url import URL
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from provenance_rename_operator import ProvenanceRenameOperator
from ogr2ogr_operator import Ogr2OgrOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

from common import (
    default_args,
    SHARED_DIR,
    MessageOperator,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
)

from common.db import DatabaseEngine
from common.objectstore import fetch_objectstore_credentials

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
)

from importscripts.import_processenverbaalverkiezingen import save_data
from sql.processenverbaalverkiezingen import SQL_REDEFINE_PK, SQL_DROP_TMP_TABLE


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
        params=dict(tablename=f"{schema_name}_{table_name}_new"),
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
            params=dict(table_name=f"{schema_name}_{table_name}_new"),
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
        data_schema_name=f"{schema_name}",
        data_table_name=f"{schema_name}_{table_name}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 10. Check for changes to merge in target table
    change_data_capture = PgComparatorCDCOperator(
        task_id="change_data_capture",
        source_table=f"{schema_name}_{table_name}_new",
        target_table=f"{schema_name}_{table_name}",
    )

    # 11. Clean up (remove temp table _new)
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params=dict(tablename=f"{schema_name}_{table_name}_new"),
    )

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
)

dag.doc_md = """
    #### DAG summery
    This DAG containts URI's (and metadata) of 'processenverbaal verkiezingen'
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
