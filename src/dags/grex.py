import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from geoalchemy2 import Geometry, WKTElement
from shapely import wkt
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from sqlalchemy.types import Date, Float, Integer, Text

from common import default_args
from common.sql import SQL_CHECK_COUNT, SQL_CHECK_GEO
from common.db import get_engine, get_ora_engine
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator

dag_id = "grex"

table_name = f"{dag_id}_projecten"
table_name_new = f"{table_name}_new"
SQL_TABLE_RENAME = f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    ALTER TABLE {table_name_new} RENAME TO {table_name};
    ALTER TABLE {table_name} RENAME CONSTRAINT {table_name_new}_pkey TO {table_name}_pkey;
    ALTER INDEX ix_{table_name_new}_id RENAME TO ix_{table_name}_id;
    ALTER INDEX idx_{table_name_new}_geometry RENAME TO idx_{table_name}_geometry;
"""


def wkt_loads_wrapped(data):
    p = wkt.loads(data)
    if isinstance(p, Polygon):
        p = MultiPolygon([p])
    elif isinstance(p, MultiPolygon):
        pass
    else:
        p = p
    p = WKTElement(p.wkt, srid=4326)
    return p


def load_grex_from_dwh(table_name):
    db_engine = get_engine()
    dwh_ora_engine = get_ora_engine("oracle_dwh_stadsdelen")
    with dwh_ora_engine.connect() as connection:
        df = pd.read_sql(
            """
            SELECT PLANNR as ID
                 , PLANNAAM
                 , STARTDATUM
                 , PLANSTATUS
                 , OPPERVLAKTE
                 , GEOMETRIE_WKT AS GEOMETRY
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
        df["geometry"] = df["geometry"].apply(wkt_loads_wrapped)
        grex_rapportage_dtype = {
            "id": Integer(),
            "plannaam": Text(),
            "startdatum": Date(),
            "planstatus": Text(),
            "oppervlakte": Float(),
            "geometry": Geometry(geometry_type="GEOMETRY", srid=4326),
        }
        df.to_sql(
            table_name, db_engine, if_exists="replace", dtype=grex_rapportage_dtype
        )
        with db_engine.connect() as connection:
            connection.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
            connection.execute(
                f"""
                 UPDATE {table_name}
                 SET geometry = ST_CollectionExtract(ST_Makevalid(geometry), 3)
                 WHERE ST_IsValid(geometry) = False
                 OR ST_GeometryType(geometry) != 'ST_MultiPolygon';
                 COMMIT;
             """
            )
            connection.execute(
                f"""
                 ALTER TABLE {table_name}
                 ALTER COLUMN geometry TYPE geometry(MultiPolygon,28992)
                 USING ST_Transform(geometry,28992);
             """
            )
            connection.execute(f"DELETE FROM {table_name} WHERE geometry is NULL")


with DAG(
    "grex",
    default_args=default_args,
    description="GrondExploitatie",
    schedule_interval="0 6 * * *",
    ) as dag:

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_grex_from_dwh,
        op_args=[table_name_new],
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=table_name_new, mincount=400),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(
            tablename=table_name_new, geotype="ST_MultiPolygon", geo_column="geometry"
        ),
    )

    rename_table = PostgresOperator(task_id="rename_table", sql=SQL_TABLE_RENAME)

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(
        task_id="grants",
        dag_name=dag_id
    )


load_data >> check_count >> check_geo >> rename_table >> grant_db_permissions
