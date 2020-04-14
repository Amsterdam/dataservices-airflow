from environs import Env
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, Float, Date
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely import wkt

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from common import default_args
from swift_operator import SwiftOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


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
    env = Env()
    user = env("POSTGRES_USER")
    password = env("POSTGRES_PASSWORD")
    host = env("POSTGRES_HOST")
    port = env("POSTGRES_PORT")
    db = env("POSTGRES_DB")

    db_engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    df = pd.read_csv(
        input_csv,
        delimiter=";",
        decimal=",",
        encoding="latin_1",
        parse_dates=["STARTDATUM"],
        dayfirst=True,
    )
    df.rename(columns=lambda x: x.lower(), inplace=True)
    df["geometrie"] = df["geometrie"].apply(wkt_loads_wrapped)

    grex_rapportage_dtype = {
        "plannr": Integer(),
        "plannaam": Text(),
        "planbeheercode": Text(),
        "startdatum": Date(),
        "planstatus": Text(),
        "oppervlakte": Float(),
        "geometrie": Geometry(geometry_type='MULTIPOLYGON', srid=4326),
    }

    df.to_sql(table_name, db_engine, if_exists="replace", dtype=grex_rapportage_dtype)


dag_id = "grex"
dag_config = Variable.get(dag_id, deserialize_json=True)

table_name = dag_id
table_name_new = f"{dag_id}_new"
SQL_TABLE_RENAME = f"""
    BEGIN;
    DROP TABLE IF EXISTS grex CASCADE;
    ALTER TABLE {table_name_new} RENAME TO {table_name};
    COMMIT;
"""


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

    rename_table = PostgresOperator(task_id="rename_table", sql=SQL_TABLE_RENAME)


mk_tmp_dir >> fetch_csv >> load_data >> rename_table
