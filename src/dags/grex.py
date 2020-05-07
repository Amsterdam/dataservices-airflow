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
from common.db import get_engine
from swift_operator import SwiftOperator
from postgres_check_operator import PostgresCheckOperator

dag_id = "grex"
dag_config = Variable.get(dag_id, deserialize_json=True)

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
        p = None
    if p:
        p = WKTElement(p.wkt, srid=4326)
    return p


def load_grex(input_csv, table_name):
    db_engine = get_engine()
    df = pd.read_csv(
        input_csv,
        delimiter=";",
        decimal=",",
        encoding="latin_1",
        parse_dates=["STARTDATUM"],
        dayfirst=True,
        index_col="PLANNR",
    )
    df.rename(
        columns=lambda x: "geometry" if x == "GEOMETRIE" else x.lower(), inplace=True
    )
    df.index.name = "id"
    df["geometry"] = df["geometry"].apply(wkt_loads_wrapped)

    grex_rapportage_dtype = {
        "id": Integer(),
        "plannaam": Text(),
        "planbeheercode": Text(),
        "startdatum": Date(),
        "planstatus": Text(),
        "oppervlakte": Float(),
        "geometry": Geometry(geometry_type="MULTIPOLYGON", srid=4326),
    }

    df.to_sql(table_name, db_engine, if_exists="replace", dtype=grex_rapportage_dtype)
    with db_engine.connect() as connection:
        connection.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
        connection.execute(
            f"""
            ALTER TABLE {table_name}
            ALTER COLUMN geometry TYPE geometry(MultiPolygon,28992)
            USING ST_Transform(geometry,28992)
        """
        )
        connection.execute(f"DELETE FROM {table_name} WHERE geometry is NULL")
        connection.execute(
            f"""
            UPDATE {table_name}
            SET geometry = ST_CollectionExtract(ST_Makevalid(geometry), 3)
            WHERE ST_IsValid(geometry) = False;
        """
        )


with DAG("grex", default_args=default_args, description="GrondExploitatie",) as dag:

    csv_file = dag_config["csv_file"]
    tmp_dir = f"/tmp/{dag_id}"

    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    fetch_csv = SwiftOperator(
        task_id="fetch_csv",
        container="grex",
        object_id=csv_file,
        output_path=f"{tmp_dir}/{csv_file}",
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_grex,
        op_args=[f"{tmp_dir}/{csv_file}", table_name_new],
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


mk_tmp_dir >> fetch_csv >> load_data >> check_count >> check_geo >> rename_table
