import logging
import os
from contextlib import closing
from typing import Final, Optional

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.context import Context
from common import default_args
from common.db import DatabaseEngine, get_ora_engine, wkt_loads_wrapped
from common.sql import SQL_CHECK_COUNT, SQL_CHECK_GEO
from contact_point.callbacks import get_contact_point_on_failure_callback
from geoalchemy2 import Geometry
from importscripts.grex import _pick_branch
from postgres_check_operator import PostgresCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from psycopg2 import sql
from sqlalchemy.types import Date, Float, Integer, Text

logger = logging.getLogger(__name__)

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "grex_az"
DATASET_ID: Final = "grex"

table_name = f"{DATASET_ID}_projecten"
table_name_new = f"{table_name}_new"
SQL_TABLE_RENAME: Final = f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    ALTER TABLE {table_name_new} RENAME TO {table_name};
    ALTER TABLE {table_name} RENAME CONSTRAINT {table_name_new}_pkey TO {table_name}_pkey;
    ALTER INDEX ix_{table_name_new}_id RENAME TO ix_{table_name}_id;
    ALTER INDEX idx_{table_name_new}_geometry RENAME TO idx_{table_name}_geometry;
"""


def load_grex_from_dwh(
    table_name: str, source_srid: int, dataset_name: Optional[str] = None, **context: Context
) -> Optional[set]:
    """Imports data from source into target database.

    Args:
        table_name: Name of target table to import data.
        source_srid: SRID of source.
        dataset_name: Name of the dataset as known in the Amsterdam schema.
            Since the DAG name can be different from the dataset name, the latter
            can be explicity given. Only applicable for Azure referentie db connection.
            Defaults to None. If None, it will use the execution context to get the
            DAG id as surrogate. Assuming that the DAG id equals the dataset name
            as defined in Amsterdam schema.

    Executes:
        SQL statements
    """
    context = context["dag"].dag_id
    postgreshook_instance = DatabaseEngine(
        dataset_name=dataset_name, context=context
    ).get_postgreshook_instance()
    db_engine = DatabaseEngine(dataset_name=dataset_name, context=context).get_engine()
    dwh_ora_engine = get_ora_engine("oracle_dwh_stadsdelen")
    with dwh_ora_engine.get_conn() as connection:
        df = pd.read_sql(
            """
            SELECT PLANNR as "id"
                 , PLANNR
                 , PLANNAAM
                 , STARTDATUM
                 , PLANSTATUS
                 , OPPERVLAKTE
                 , GEOMETRIE_WKT as "geometry"
            FROM DMDATA.GREX_GV_PLANNEN_V2
        """,
            connection,
            index_col="id",
            coerce_float=True,
            params=None,
            parse_dates=["startdatum"],
            columns=None,
            chunksize=None,
        )
        # it seems that get_conn() makes the columns case sensitive
        # lowercase all columns so the database will handle them as case insensitive
        df.columns = map(str.lower, df.columns)

        # remove duplicate ID's (if exists)
        duplicate_ind = zip(df["plannr"], df["plannr"].duplicated(keep=False))
        duplicate_ids = {plannr for plannr, ind_dup in duplicate_ind if ind_dup}
        for plannr in duplicate_ids:
            df = df[df["plannr"] != plannr]

        # remove column, not needed anymore.
        df = df.drop(["plannr"], axis=1)

        # remove duplicates out of processing.
        df = df.drop_duplicates(keep=False)

        df["geometry"] = df["geometry"].apply(
            func=wkt_loads_wrapped, source_srid=source_srid, geom_type_family="polygon"
        )
        grex_rapportage_dtype = {
            "id": Integer(),
            "plannaam": Text(),
            "startdatum": Date(),
            "planstatus": Text(),
            "oppervlakte": Float(),
            "geometry": Geometry(geometry_type="geometry", srid=source_srid),
        }
        df.to_sql(table_name, db_engine, if_exists="replace", dtype=grex_rapportage_dtype)

        with closing(postgreshook_instance.get_conn().cursor()) as cur:
            cur.execute(
                sql.SQL("ALTER TABLE {table_name} ADD PRIMARY KEY (ID); COMMIT;").format(
                    table_name=sql.Identifier(table_name)
                )
            )
            cur.execute(
                sql.SQL(
                    """UPDATE {table_name}
                 SET geometry = ST_CollectionExtract(ST_Makevalid(geometry), 3)
                 WHERE ST_IsValid(geometry) = False
                 OR ST_GeometryType(geometry) != 'ST_MultiPolygon';
                 COMMIT;"""
                ).format(table_name=sql.Identifier(table_name))
            )
            if source_srid != 28992:
                cur.execute(
                    sql.SQL(
                        """ ALTER TABLE {table_name}
                    ALTER COLUMN geometry TYPE geometry(MultiPolygon,28992)
                    USING ST_Transform(geometry,28992); COMMIT;"""
                    ).format(table_name=sql.Identifier(table_name))
                )
            cur.execute(
                sql.SQL("DELETE FROM {table_name} WHERE geometry is NULL; COMMIT;").format(
                    table_name=sql.Identifier(table_name)
                )
            )

        logger.info("duplicate_ids = %d", duplicate_ids)
        return duplicate_ids


with DAG(
    DAG_ID,
    default_args=default_args,
    description="GrondExploitatie",
    schedule_interval="0 7 * * *",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. load data
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_grex_from_dwh,
        provide_context=True,
        op_args=[table_name_new, 4326, DATASET_ID],
    )

    # 2. Pick next step based on existence of duplicate rows.
    # if duplicates exists, then run task `notify_duplicate_rows` as
    # the next step. Else run the step `check_count` and continue.
    # To retrieve duplicates it uses the return value of the previous step (xcom)
    pick_branch = BranchPythonOperator(
        task_id="pick_branch",
        python_callable=_pick_branch,
        provide_context=True,
    )

    # 3. Dummy operator to indicate NO duplicates found in source data.
    no_duplicates_found = DummyOperator(task_id="no_duplicates_found")

    # 4. Dummy operator to indicate duplicates found in source data.
    # This is merly an indictor to investigate the source data.
    duplicates_found = DummyOperator(task_id="duplicates_found")

    # 5. check num of records threshold
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params={"tablename": table_name_new, "mincount": 400},
        trigger_rule="one_success",
    )

    # 6. check geo types
    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params={
            "tablename": table_name_new,
            "geotype": "ST_MultiPolygon",
            "geo_column": "geometry",
        },
    )

    # 7. rename temp to final table
    rename_table = PostgresOnAzureOperator(task_id="rename_table", sql=SQL_TABLE_RENAME)

    # 8. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(
        task_id="grants", dag_name=DATASET_ID, create_roles=False
    )


# WORKFLOW
(
    load_data
    >> pick_branch
    >> [no_duplicates_found, duplicates_found]
    >> check_count
    >> check_geo
    >> rename_table
    >> grant_db_permissions
)
