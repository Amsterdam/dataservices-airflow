import logging
from typing import Final, Iterator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, env, slack_webhook_token
from contact_point.callbacks import get_contact_point_on_failure_callback
from dynamic_dagrun_operator import TriggerDynamicDagRunOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator

# These seemingly unused imports are actually used, albeit indirectly. See the code with
# `globals()[select_statement]`. Hence the `noqa: F401` comments.
from sql.basiskaart import CREATE_MVIEWS, CREATE_TABLES
from sql.basiskaart import SELECT_GEBOUWVLAK_SQL as SELECT_GEBOUWVLAK_SQL  # noqa: F401
from sql.basiskaart import (  # noqa: F401
    SELECT_INRICHTINGSELEMENTLIJN_SQL as SELECT_INRICHTINGSELEMENTLIJN_SQL,
)
from sql.basiskaart import (  # noqa: F401
    SELECT_INRICHTINGSELEMENTPUNT_SQL as SELECT_INRICHTINGSELEMENTPUNT_SQL,
)
from sql.basiskaart import (  # noqa: F401
    SELECT_INRICHTINGSELEMENTVLAK_SQL as SELECT_INRICHTINGSELEMENTVLAK_SQL,
)
from sql.basiskaart import SELECT_LABELS_SQL as SELECT_LABELS_SQL  # noqa: F401
from sql.basiskaart import SELECT_SPOORLIJN_SQL as SELECT_SPOORLIJN_SQL  # noqa: F401
from sql.basiskaart import SELECT_TERREINDEELVLAK_SQL as SELECT_TERREINDEELVLAK_SQL  # noqa: F401
from sql.basiskaart import SELECT_WATERDEELLIJN_SQL as SELECT_WATERDEELLIJN_SQL  # noqa: F401
from sql.basiskaart import SELECT_WATERDEELVLAK_SQL as SELECT_WATERDEELVLAK_SQL  # noqa: F401
from sql.basiskaart import SELECT_WEGDEELLIJN_SQL as SELECT_WEGDEELLIJN_SQL  # noqa: F401
from sql.basiskaart import SELECT_WEGDEELVLAK_SQL as SELECT_WEGDEELVLAK_SQL  # noqa: F401
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

DAG_ID: Final = "basiskaart"
OWNER: Final = "dataservices"
VARIABLES: Final = Variable.get(DAG_ID, deserialize_json=True)
TABLES_TO_CREATE: Final = VARIABLES["tables_to_create"]
SOURCE_CONNECTION: Final = "AIRFLOW_CONN_POSTGRES_BASISKAART"
IMPORT_STEP: Final = 10_000
logger = logging.getLogger(__name__)


def create_tables_from_basiskaartdb_to_masterdb(
    source_connection: str, source_select_statement: str, target_base_table: str
) -> None:
    """Copy data from basiskaart DB to master DB.

    Source_connection contains the environment variable name (as defined in the
    docker-compose.yml) of the source connection i.e. AIRFLOW_CONN_POSTGRES_BASISKAART
    Source_select_statement contains the SQL select query that will be executed on the source
    DB. Target_base_table contains the table in master DB where the data (the result of
    source_select_statement execution) is inserted into.
    """
    try:
        # setup the DB source connection
        source_engine = create_engine(env(source_connection).split("?")[0])
    except SQLAlchemyError as e:
        raise Exception(str(e)) from e

    # fetch data from source DB
    with source_engine.connect() as src_conn:
        count = 0
        cursor = src_conn.execute(source_select_statement)
        while True:
            fetch_iterator = cursor.fetchmany(size=IMPORT_STEP)
            batch_count = copy_data_in_batch(target_base_table, fetch_iterator)
            count += batch_count
            if batch_count < IMPORT_STEP:
                break
    logger.info("Total records imported: %d", count)


def copy_data_in_batch(target_base_table: str, fetch_iterator: Iterator) -> int:
    """Insert data from iterator into target table."""
    masterdb_hook = PostgresHook()
    items = []

    # setup SQL insert values the be executed
    for row in fetch_iterator:
        items.append([column_value for column_value in row])

    result = len(items)

    # execute SQL insert statement
    # the Airflow PostgresHook.insert_rows instance method is used to "executemany" SQL query
    # which also serializes the data to a save SQL format
    if result:
        try:
            masterdb_hook.insert_rows(
                target_base_table, items, target_fields=None, commit_every=1000, replace=False
            )
        except Exception as e:
            raise Exception(f"Failed to insert batch data: {str(e)[0:150]}") from e

    return result


def create_basiskaart_dag(is_first: bool, table_name: str, select_statement: str) -> DAG:
    """Generates a DAG for each table.

    The table_name is the target table in de masterDB where the data will be inserted. The
    select_statement is one of the imported SQL query selects (see above) that will be executed
    on the source DB.
    """
    # start time first DAG
    # Note: the basiskaartimport task in Jenkins runs at an arbitrary but invariant time between
    # 3 and 5 a.m. Because of this, the first DAG starts running at 7 a.m.
    schedule_start_hour = 7

    dag = DAG(
        f"{DAG_ID}_{table_name}",
        default_args={"owner": OWNER, **default_args},
        # the first DAG will have the is_first boolean set to True
        # the other DAG's will be triggered to start when the previous DAG is finished
        # (estafette run / relay run)
        schedule_interval=f"0 {schedule_start_hour} * * *" if is_first else None,
        description="""
        basisregistratie grootschalige topologie (BGT) en kleinschalige basiskaart (KBK10 en 50).
        The basiskaart data is collected from basiskaart DB.""",
        tags=["basiskaart"],
        on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
    )

    with dag:

        # 1. Post info message on slack
        slack_at_start = MessageOperator(
            task_id="slack_at_start",
            http_conn_id="slack",
            webhook_token=slack_webhook_token,
            message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
            username="admin",
        )

        # 2. Create temp and target table
        create_tables = PostgresOperator(
            task_id="create_tables",
            sql=CREATE_TABLES,
            params={"base_table": table_name, "dag_id": DAG_ID},
        )

        # 3. Copy data into temp table
        copy_data = PythonOperator(
            task_id="insert_data",
            python_callable=create_tables_from_basiskaartdb_to_masterdb,
            op_kwargs={
                "source_connection": SOURCE_CONNECTION,
                "source_select_statement": globals()[select_statement],
                "target_base_table": f"{DAG_ID}_{table_name}_temp",
            },
            dag=dag,
        )

        # 4. Check for changes in temp table to merge in target table
        change_data_capture = PgComparatorCDCOperator(
            task_id="change_data_capture",
            source_table=f"{DAG_ID}_{table_name}_temp",
            target_table=f"{DAG_ID}_{table_name}",
        )

        # 5. Create mviews for T-REX tile server
        create_mviews = PostgresOperator(
            task_id="create_mviews",
            sql=CREATE_MVIEWS,
            params={"base_table": table_name, "dag_id": DAG_ID},
        )

        # 6. Rename COLUMNS based on Provenance
        provenance_translation = ProvenanceRenameOperator(
            task_id="rename_columns",
            dataset_name=DAG_ID,
            prefix_table_name=f"{DAG_ID}_",
            rename_indexes=False,
            pg_schema="public",
        )

        # 7. Drop temp table
        clean_up = PostgresOperator(
            task_id="drop_temp_table",
            sql=[
                f"DROP TABLE IF EXISTS {DAG_ID}_{table_name}_temp CASCADE",
            ],
        )

        # 8. Trigger next DAG to run (estafette)
        trigger_next_dag = TriggerDynamicDagRunOperator(
            task_id="trigger_next_dag",
            dag_id_prefix=f"{DAG_ID}_",
            trigger_rule="all_done",
        )

        # 9. Grant database permissions
        grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    # Flow
    (
        slack_at_start
        >> create_tables
        >> copy_data
        >> change_data_capture
        >> create_mviews
        >> provenance_translation
        >> clean_up
        >> trigger_next_dag
        >> grant_db_permissions
    )

    dag.doc_md = """
    #### DAG summary
    This DAG contains BGT (basisregistratie grootschalige topografie) i
    and KBK10 (kleinschalige basiskaart 10)
    and KBK50 (kleinschalige basiskaart 50) data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues
    and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    NA
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/basiskaart.html
    Note: The basiskaart data is collected from the GOB objectstore
    and processed in the basiskaart DB
    => which is the source for this DAG.
    """

    return dag


for i, (table, select_statement) in enumerate(TABLES_TO_CREATE.items()):
    globals()[f"{DAG_ID}_{table}"] = create_basiskaart_dag(i == 0, table, select_statement)
