from typing import Final

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import default_args
from common.db import get_engine, get_ora_engine_using_service_name
from common.sql import SQL_CHECK_COUNT, SQL_CHECK_GEO
from contact_point.callbacks import get_contact_point_on_failure_callback
from geoalchemy2 import Geometry, WKTElement
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from shapely import wkt
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from sqlalchemy.types import Date, Float, Integer, Text

dag_id = "grex"

table_name = f"{dag_id}_projecten"
table_name_new = f"{table_name}_new"
SQL_TABLE_RENAME: Final = f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    ALTER TABLE {table_name_new} RENAME TO {table_name};
    ALTER TABLE {table_name} RENAME CONSTRAINT {table_name_new}_pkey TO {table_name}_pkey;
    ALTER INDEX ix_{table_name_new}_id RENAME TO ix_{table_name}_id;
    ALTER INDEX idx_{table_name_new}_geometry RENAME TO idx_{table_name}_geometry;
"""


def wkt_loads_wrapped(data: str) -> WKTElement:
    """Loading WKT (wellkown text) geometry definition.

    And translate single geometry to multi.

    Args:
        data: Geometry data from source.
    """
    p = wkt.loads(data)
    if isinstance(p, Polygon):
        p = MultiPolygon([p])
    elif isinstance(p, MultiPolygon):
        pass
    else:
        p = p
    p = WKTElement(p.wkt, srid=4326)
    return p


def load_grex_from_dwh(table_name: str) -> None:
    """Imports data from source into target database.

    Args:
        table_name: Name of target table to import data.
    Executes:
        SQL statements
    """
    db_engine = get_engine()
    dwh_ora_engine = get_ora_engine_using_service_name("oracle_dwh_stadsdelen")
    # import cx_Oracle
    with dwh_ora_engine.get_conn() as connection:
        # with dwh_ora_engine.connect() as connection:
        #   SELECT PLANNR as ID
        #              , PLANNAAM
        #              , STARTDATUM
        #              , PLANSTATUS
        #              , OPPERVLAKTE
        #              , GEOMETRIE_WKT AS GEOMETRY
        #         FROM DMDATA.GREX_GV_PLANNEN_V2
        # with cx_Oracle.connect(**dwh_ora_engine) as connection:
        df = pd.read_sql(
            """
            SELECT owner, table_name FROM all_tables

        """,
            connection,
            # index_col="id",
            coerce_float=True,
            params=None,
            parse_dates=["startdatum"],
            columns=None,
            chunksize=None,
        )
        # df["geometry"] = df["geometry"].apply(wkt_loads_wrapped)
        grex_rapportage_dtype = {
            "id": Integer(),
            "plannaam": Text(),
            "startdatum": Date(),
            "planstatus": Text(),
            "oppervlakte": Float(),
            "geometry": Geometry(geometry_type="GEOMETRY", srid=4326),
        }
        df.to_sql(table_name, db_engine, if_exists="replace", dtype=grex_rapportage_dtype)
        # with db_engine.connect() as connection:
        #     connection.execute(f"ALTER TABLE-> WKTElement) != 'ST_MultiPolygon';
        #          COMMIT;
        #      """
        #     )
        #     connection.execute(
        #         f"""
        #          ALTER TABLE {table_name}
        #          ALTER COLUMN geometry TYPE geometry(MultiPolygon,28992)
        #          USING ST_Transform(geometry,28992);
        #      """
        #     )
        #     connection.execute(f"DELETE FROM {table_name} WHERE geometry is NULL")


with DAG(
    "grex",
    default_args=default_args,
    description="GrondExploitatie",
    schedule_interval="0 6 * * *",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_grex_from_dwh,
        op_args=[table_name_new],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params={"tablename": table_name_new, "mincount": 400},
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params={
            "tablename": table_name_new,
            "geotype": "ST_MultiPolygon",
            "geo_column": "geometry",
        },
    )

    rename_table = PostgresOperator(task_id="rename_table", sql=SQL_TABLE_RENAME)

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


load_data >> check_count >> check_geo >> rename_table >> grant_db_permissions
