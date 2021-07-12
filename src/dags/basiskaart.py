import logging
import operator
from dataclasses import dataclass
from typing import Final, Iterable, Iterator, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, env, slack_webhook_token
from contact_point.callbacks import get_contact_point_on_failure_callback
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator

# These seemingly unused imports are actually used, albeit indirectly. See the code with
# `globals()[select_statement]`. Hence the `noqa: F401` comments.
from sql.basiskaart import CREATE_MVIEWS
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
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

DAG_ID: Final = "basiskaart"
VARIABLES: Final = Variable.get(DAG_ID, deserialize_json=True)
TABLE_TO_SELECT: Final = VARIABLES["tables_to_create"]
SOURCE_CONNECTION: Final = "AIRFLOW_CONN_POSTGRES_BASISKAART"
IMPORT_STEP: Final = 10_000
TMP_TABLE_POSTFIX: Final = "_temp"

logger = logging.getLogger(__name__)


@dataclass
class Table:
    """Simple dataclass to represent various table ids and a select statement."""

    id: str  # noqa: A003
    base_id: str
    tmp_id: str
    select: str


TABLES: Final[List[Table]] = [
    Table(
        id=f"{DAG_ID}_{base_id}",
        base_id=base_id,
        tmp_id=f"{DAG_ID}_{base_id}{TMP_TABLE_POSTFIX}",
        select=select,
    )
    for base_id, select in TABLE_TO_SELECT.items()
]


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
        logger.debug("Executing SQL query: %r", source_select_statement)
        cursor: ResultProxy = src_conn.execute(source_select_statement)
        while True:
            fetch_iterator = cursor.fetchmany(size=IMPORT_STEP)
            batch_count = copy_data_in_batch(target_base_table, fetch_iterator, cursor.keys())
            count += batch_count
            if batch_count < IMPORT_STEP:
                break
    logger.info("Total records imported: %d", count)


def copy_data_in_batch(
    target_base_table: str, fetch_iterator: Iterator, col_names: Iterable[str]
) -> int:
    """Insert data from iterator into target table."""
    # TODO:
    # This function is enormously inefficient;
    # - it reads everything into memory by converting the iterator into a list
    # - it inserts each row one by one into the database (<- by far slowest part)
    #
    # It should have used:
    # - PostgresHook's `bulk_load` (though that function is rather limited in functionality)
    # - Or use a prepare statement in conjunction with psycopg2.extras.execute_batch as
    #   `PostgresInsertCsvOperator` has done
    masterdb_hook = PostgresHook()

    rows = list(fetch_iterator)
    if row_count := len(rows):
        try:
            masterdb_hook.insert_rows(
                target_base_table, rows, target_fields=col_names, commit_every=1000, replace=False
            )
        except Exception as e:
            raise Exception(f"Failed to insert batch data: {str(e)[0:150]}") from e

    return row_count


def join(number: int) -> DummyOperator:
    """Return numbered DummyOperator to join parallel tasks."""
    return DummyOperator(task_id=f"join_parallel_tasks_{number}")


def rm_tmp_tables(task_id_postfix: str, tables: Iterable[Table]) -> PostgresOperator:
    """Remove tmp tables."""
    return PostgresOperator(
        task_id=f"rm_tmp_tables{task_id_postfix}",
        sql="DROP TABLE IF EXISTS {tables}".format(
            tables=", ".join(map(operator.attrgetter("tmp_id"), tables))
        ),
    )


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    # Note: the basiskaartimport task in Jenkins runs at an arbitrary but invariant time
    # between 3 and 5 a.m. Because of this, the first DAG starts running at 7 a.m.
    schedule_interval="0 7 * * *",
    description="""
        Basisregistratie grootschalige topologie (BGT) en kleinschalige basiskaart (KBK10 en 50).
        The basiskaart data is collected from basiskaart DB.""",
    tags=["basiskaart"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    rm_tmp_tables_pre = rm_tmp_tables("_pre", TABLES)

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DAG_ID,
        ind_extra_index=True,
    )

    create_temp_tables = [
        PostgresTableCopyOperator(
            task_id=f"create_temp_table_{table.tmp_id}",
            source_table_name=table.id,
            target_table_name=table.tmp_id,
            # Only copy table definitions. Don't do anything else.
            truncate_target=False,
            copy_data=False,
            drop_source=False,
        )
        for table in TABLES
    ]

    join_1 = join(1)

    copy_data = [
        PythonOperator(
            task_id=f"insert_data_into_{table.id}",
            python_callable=create_tables_from_basiskaartdb_to_masterdb,
            op_kwargs={
                "source_connection": SOURCE_CONNECTION,
                "source_select_statement": globals()[table.select],
                "target_base_table": table.tmp_id,
            },
            dag=dag,
        )
        for table in TABLES
    ]

    join_2 = join(2)

    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_for_{table.id}",
            source_table=table.tmp_id,
            target_table=table.id,
        )
        for table in TABLES
    ]

    join_3 = join(3)

    # Create mviews for T-REX tile server
    create_mviews = [
        PostgresOperator(
            task_id=f"create_mviews_for_{table.id}",
            sql=CREATE_MVIEWS,
            params={"base_table": table.base_id, "dag_id": DAG_ID},
        )
        for table in TABLES
    ]

    rm_tmp_tables_post = rm_tmp_tables("_post", TABLES)

    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    # Flow
    (
        slack_at_start
        >> rm_tmp_tables_pre
        >> sqlalchemy_create_objects_from_schema
        >> create_temp_tables
        >> join_1
        >> copy_data
        >> join_2
        >> change_data_capture
        >> join_3
        >> create_mviews
        >> rm_tmp_tables_post
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
