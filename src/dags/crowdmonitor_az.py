import os
import logging
from contextlib import closing
from typing import Final, Iterable, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from common import default_args, env
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_on_azure_hook import PostgresOnAzureHook
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.engine import Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_MOBI"]

DAG_ID: Final = "crowdmonitor_az"
DATASET_ID: Final = "crowdmonitor"
TABLE_ID = f"{DATASET_ID}_passanten"
IMPORT_STEP = 10_000
TMP_TABLE_POSTFIX: Final = "_temp"

SQL_ADD_AGGREGATES: Final = """
    SET TIME ZONE 'Europe/Amsterdam';
    DELETE
    FROM {{ params.table }}
    WHERE periode = '{{ params.periode }}';
    INSERT INTO {{ params.table }}(sensor, naam_locatie, periode, datum_uur, aantal_passanten)
    SELECT sensor,
       naam_locatie,
       '{{ params.periode }}',
       DATE_TRUNC('{{ params.periode_en }}', datum_uur) AS datum_uur,
       SUM(aantal_passanten) AS aantal_passanten
    FROM {{ params.table }}
    WHERE periode = 'uur'
    GROUP BY sensor, naam_locatie, DATE_TRUNC('{{ params.periode_en }}', datum_uur);
"""

logger = logging.getLogger(__name__)


def copy_data_from_dbwaarnemingen_to_masterdb(**context: Context) -> None:
    """Copy data from external DB to our reference DB."""
    try:
        waarnemingen_engine = create_engine(
            env("AIRFLOW_CONN_POSTGRES_DBWAARNEMINGEN").split("?")[0]
        )
    except SQLAlchemyError as e:
        raise Exception(str(e)) from e

    with waarnemingen_engine.connect() as waarnemingen_connection:
        count = 0
        # Ensure that the column names in the final SELECT clause match the columns names of the
        # target table in our reference database. The INSERT statement generation code depends
        # on it
        cursor = waarnemingen_connection.execute(
            """
            SET TIME ZONE 'Europe/Amsterdam';
            WITH cmsa AS (
                SELECT sensor,
                       DATE_TRUNC('hour', timestamp_rounded) AS datum_uur,
                       SUM(total_count) AS aantal_passanten
                    FROM continuousaggregate_cmsa15min
                    WHERE timestamp_rounded > TO_DATE('2019-01-01', 'YYYY-MM-DD')
                    GROUP BY sensor, datum_uur)
            SELECT v.sensor,
                   'uur' AS periode,
                   s.location_name AS naam_locatie,
                   v.datum_uur,
                   CASE
                       WHEN v.aantal_passanten < 15 THEN 0
                       ELSE v.aantal_passanten
                   END AS aantal_passanten,
                   s.gebied,
                   ST_Transform(s.geom, 28992) AS geometrie  -- Ref DB only uses SRID 28992
                FROM cmsa v
                         JOIN peoplemeasurement_sensors s ON s.objectnummer = v.sensor
                WHERE s.is_public IS TRUE;
            """
        )
        while True:
            rows: list[Row] = cursor.fetchmany(size=IMPORT_STEP)
            batch_count = insert_many(rows, IMPORT_STEP, context)
            count += batch_count
            if batch_count < IMPORT_STEP:
                break
        logger.info("Number of records imported: %d", count)


def insert_many(rows: list[Row], batch_size: Optional[int], context: Context) -> int:
    """Insert rows in batches."""
    if batch_size is None:
        batch_size = len(rows)

    if rows_count := len(rows):
        col_names = rows[0].keys()
        insert_stmt = sql.SQL("INSERT INTO {table_id} ({col_names}) VALUES %s").format(
            table_id=sql.Identifier(f"{TABLE_ID}{TMP_TABLE_POSTFIX}"),
            col_names=sql.SQL(", ").join(
                map(
                    sql.Identifier,
                    col_names,
                )
            ),
        )

        masterdb_hook = PostgresOnAzureHook(dataset_name=DATASET_ID, context=context)
        try:
            with closing(masterdb_hook.get_conn()) as conn, closing(conn.cursor()) as cur:
                logger.info("Executing SQL statement %s", insert_stmt.as_string(conn))
                execute_values(cur, insert_stmt, rows, page_size=batch_size)
                conn.commit()

        except Exception as e:
            raise Exception(f"Failed to insert batch data: {str(e)[0:150]}") from e
        else:
            logger.info("Number of records created: %d", rows_count)
    return rows_count


def rm_tmp_tables(
    task_id_postfix: str, tables: Iterable[str] = (f"{TABLE_ID}{TMP_TABLE_POSTFIX}",)
) -> PostgresOnAzureOperator:
    """Remove tmp tables."""
    return PostgresOnAzureOperator(
        task_id=f"rm_tmp_tables{task_id_postfix}",
        # Airflow is bizarre in some ways. Its `PostgresOperator` specifies the `sql` argument to
        # `__init__` to be of solely type `str`. Internally it uses `PostgresHook` to execute
        # the query. As it turns out `PostgresHook` accepts more types for `sql` than just a
        # `str`. It also accepts a `psycopg2.sql.Composable` (by virtue of building on top of
        # `psycopg2`), or a list of `str`s and/or `psycopg2.sql.Composable`s. So why does
        # PostgresOperator limit `sql` to type `str`? It beats me? And though I am a big
        # fan of type annotations, I am going to ignore them here, because `PostgresOperator` is
        # wrong!
        #
        # But `PostgresHook` is not without issues either. If the type of `sql` is not a string,
        # it assumes it to be a list of SQL statements. Well `psycopg2.sql.Composable` is not a
        # `str`, nor is it a list of SQL statements. By wrapping `psycopg2.sql.Composable` in a
        # list we work around the wrongful assumption made by `PostgresHook`. *ugh*
        sql=[
            sql.SQL("DROP TABLE IF EXISTS {tables}").format(
                tables=sql.SQL(", ").join(map(sql.Identifier, tables))
            )
        ],
    )


args = default_args.copy()

with DAG(
    DAG_ID,
    default_args=args,
    description="Crowd Monitor",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    rm_tmp_table_pre = rm_tmp_tables("_pre")

    # Drop the identity on the `id` column, otherwise SqlAlchemyCreateObjectOperator
    # gets seriously confused; as in: its finds something it didn't create and errors out.
    drop_identity_from_table = PostgresOnAzureOperator(
        task_id="drop_identity_from_table",
        sql="""
                ALTER TABLE IF EXISTS {{ params.table_id }}
                    ALTER COLUMN id DROP IDENTITY IF EXISTS;
            """,
        params={"table_id": TABLE_ID},
    )

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DATASET_ID,
        ind_extra_index=True,
    )

    add_identity_to_table = PostgresOnAzureOperator(
        task_id="add_identity_to_table",
        sql="""
            ALTER TABLE {{ params.table_id }}
                ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
        """,
        params={"table_id": TABLE_ID},
    )

    create_temp_table = PostgresTableCopyOperator(
        task_id="create_temp_table",
        dataset_name_lookup=DATASET_ID,
        source_table_name=TABLE_ID,
        target_table_name=f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
        # Only copy table definitions. Don't do anything else.
        truncate_target=False,
        copy_data=False,
        drop_source=False,
    )

    copy_data = PythonOperator(
        task_id="copy_data",
        python_callable=copy_data_from_dbwaarnemingen_to_masterdb,
        dag=dag,
        provide_context=True,
    )

    add_aggregates_day = PostgresOnAzureOperator(
        task_id="add_aggregates_day",
        sql=SQL_ADD_AGGREGATES,
        params={"table": f"{TABLE_ID}{TMP_TABLE_POSTFIX}", "periode": "dag", "periode_en": "day"},
    )

    add_aggregates_week = PostgresOnAzureOperator(
        task_id="add_aggregates_week",
        sql=SQL_ADD_AGGREGATES,
        params={
            "table": f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
            "periode": "week",
            "periode_en": "week",
        },
    )

    # Though identity columns are much more convenient than the old style serial columns, the
    # sequences associated with them are not renamed automatically when the table with the
    # identity columns is renamed. Hence, when renaming a table `tmp_fubar`, with identity column
    # `id`, to `fubar`, the associated sequence will still be named `tmp_fubar_id_seq`.
    #
    # This is not a problem, unless we suffer from certain OCD tendencies. Hence, we leave the
    # slight naming inconsistency as-is. Should you want to address it, don't rename the
    # sequence. Simply drop cascade it instead. After all, the identity column is only required
    # for the duration of the import; we don't need it afterwards. And when you do want to drop
    # cascade it, reuse the existing `drop_identity_to_tables` task by wrapping it in a
    # function and call it just before renaming the tables.
    rename_temp_table = PostgresTableRenameOperator(
        task_id=f"rename_tables_for_{TABLE_ID}",
        new_table_name=TABLE_ID,
        old_table_name=f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
        cascade=True,
    )

    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

    (
        rm_tmp_table_pre
        >> drop_identity_from_table
        >> sqlalchemy_create_objects_from_schema
        >> add_identity_to_table
        >> create_temp_table
        >> copy_data
        >> add_aggregates_day
        >> add_aggregates_week
        >> rename_temp_table
        >> grant_db_permissions
    )
