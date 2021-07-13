import logging
from typing import Final, Iterable, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import default_args, env
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from sqlalchemy import create_engine
from sqlalchemy.engine import RowProxy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

DAG_ID = "crowdmonitor"
TABLE_ID = f"{DAG_ID}_passanten"
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


def copy_data_from_dbwaarnemingen_to_masterdb() -> None:
    """Copy data from external DB to our reference DB."""
    try:
        waarnemingen_engine = create_engine(
            env("AIRFLOW_CONN_POSTGRES_DBWAARNEMINGEN").split("?")[0]
        )
    except SQLAlchemyError as e:
        raise Exception(str(e)) from e

    with waarnemingen_engine.connect() as waarnemingen_connection:
        count = 0
        cursor = waarnemingen_connection.execute(
            """
            SET TIME ZONE 'Europe/Amsterdam';
            WITH cmsa AS (
                SELECT sensor,
                       DATE_TRUNC('hour', timestamp_rounded) AS datum_uur,
                       SUM(total_count) AS aantal_passanten
                    FROM cmsa_15min_view_v8_materialized
                    WHERE timestamp_rounded > TO_DATE('2019-01-01', 'YYYY-MM-DD')
                    GROUP BY sensor, datum_uur)
            SELECT v.sensor,
                   s.location_name,
                   v.datum_uur,
                   v.aantal_passanten,
                   s.gebied,
                   s.geom AS geometrie
                FROM cmsa v
                         JOIN peoplemeasurement_sensors s ON s.objectnummer = v.sensor;
            """
        )
        while True:
            rows: List[RowProxy] = cursor.fetchmany(size=IMPORT_STEP)
            batch_count = copy_data_in_batch(rows)
            count += batch_count
            if batch_count < IMPORT_STEP:
                break
        logger.info("Number of records imported: %d", count)


def copy_data_in_batch(rows: List[RowProxy], periode: str = "uur") -> int:
    """Copy data in batches."""
    row_values: List[str] = []
    for row in rows:
        row_values.append(
            "('{sensor}', "
            "'{location_name}',"
            "'{periode}',"
            "TIMESTAMP '{datum_uur}', "
            "{aantal_passanten},"
            "{gebied},"
            "{geometrie})".format(
                sensor=row[0],
                location_name=row[1],
                periode=periode,
                datum_uur=row[2].strftime("%Y-%m-%d %H:%M:%S"),
                aantal_passanten=int(row[3]),
                gebied=f"'{row[4]}'" if row[4] else "NULL",
                # Apparently we get the results in SRID 4326, whereas we store everything
                # exclusively in SRID 28992. Hence we need to apply a transform..
                geometrie=f"ST_Transform('{row[5]}', 28992)" if row[5] else "NULL",
            )
        )
    row_count = len(row_values)
    if row_count:
        items_sql = ",".join(row_values)
        insert_sql = f"""
            INSERT INTO {TABLE_ID}{TMP_TABLE_POSTFIX}
            (sensor, naam_locatie, periode, datum_uur, aantal_passanten, gebied, geometrie)
                VALUES {items_sql};
        """

        masterdb_hook = PostgresHook()
        try:
            masterdb_hook.run(insert_sql)
        except Exception as e:
            raise Exception(f"Failed to insert batch data: {str(e)[0:150]}")
        else:
            logger.info("Number of records created: %d", row_count)
    return row_count


def rm_tmp_tables(
    task_id_postfix: str, tables: Iterable[str] = (f"{TABLE_ID}{TMP_TABLE_POSTFIX}",)
) -> PostgresOperator:
    """Remove tmp tables."""
    return PostgresOperator(
        task_id=f"rm_tmp_tables{task_id_postfix}",
        sql="DROP TABLE IF EXISTS {tables}".format(tables=", ".join(tables)),
    )


args = default_args.copy()

with DAG(
    DAG_ID,
    default_args=args,
    description="Crowd Monitor",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    rm_tmp_table_pre = rm_tmp_tables("_pre")

    # Drop the identity on the `id` column, otherwise SqlAlchemyCreateObjectOperator
    # gets seriously confused; as in: its finds something it didn't create and errors out.
    drop_identity_from_table = PostgresOperator(
        task_id="drop_identity_from_table",
        sql="""
                ALTER TABLE IF EXISTS {{ params.table_id }}
                    ALTER COLUMN id DROP IDENTITY IF EXISTS;
            """,
        params={"table_id": TABLE_ID},
    )

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DAG_ID,
        ind_extra_index=True,
    )

    add_identity_to_table = PostgresOperator(
        task_id="add_identity_to_table",
        sql="""
            ALTER TABLE {{ params.table_id }}
                ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
        """,
        params={"table_id": TABLE_ID},
    )

    create_temp_table = PostgresTableCopyOperator(
        task_id="create_temp_table",
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
    )

    add_aggregates_day = PostgresOperator(
        task_id="add_aggregates_day",
        sql=SQL_ADD_AGGREGATES,
        params={"table": f"{TABLE_ID}{TMP_TABLE_POSTFIX}", "periode": "dag", "periode_en": "day"},
    )

    add_aggregates_week = PostgresOperator(
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

    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

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
