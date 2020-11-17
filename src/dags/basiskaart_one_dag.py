from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from common.db import get_engine

from common import (
    env,
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from sql.basiskaart import CREATE_TABLE_GEBOUW_VLAK_SQL as CREATE_TABLE_GEBOUW_VLAK_SQL
from sql.basiskaart import CREATE_INRICHTINGSELEMENT_LIJN_SQL as CREATE_INRICHTINGSELEMENT_LIJN_SQL
from sql.basiskaart import CREATE_INRICHTINGSELEMENT_PUNT_SQL as CREATE_INRICHTINGSELEMENT_PUNT_SQL
from sql.basiskaart import CREATE_INRICHTINGSELEMENT_VLAK_SQL as CREATE_INRICHTINGSELEMENT_VLAK_SQL
from sql.basiskaart import CREATE_SPOOR_LIJN_SQL as CREATE_SPOOR_LIJN_SQL
from sql.basiskaart import CREATE_TERREINDEEL_VLAK_SQL as CREATE_TERREINDEEL_VLAK_SQL
from sql.basiskaart import CREATE_WATERDEEL_LIJN_SQL as CREATE_WATERDEEL_LIJN_SQL
from sql.basiskaart import CREATE_WATERDEEL_VLAK_SQL as CREATE_WATERDEEL_VLAK_SQL
from sql.basiskaart import CREATE_WEGDEEL_VLAK_SQL as CREATE_WEGDEEL_VLAK_SQL
from sql.basiskaart import CREATE_WEGDEEL_LIJN_SQL as CREATE_WEGDEEL_LIJN_SQL

dag_id = "basiskaart_one_dag"
variables = Variable.get(dag_id, deserialize_json=True)
tables_to_create = variables["tables_to_create"]
source_connection="AIRFLOW_CONN_POSTGRES_BASISKAART"
import_step = 10000

# SQL_CREATE_TEMP_TABLE = """
#     DROP TABLE IF EXISTS {{ params.base_table }} CASCADE;
#     CREATE TABLE {{ params.base_table }} (
#       identificatie_lokaalid character varying PRIMARY KEY,
#       type character varying,
#       geometrie geometry(Point,28992),      
#       hoek integer,
#       tekst character varying,
#       bron character varying,
#       minzoom integer,
#       maxzoom integer     
#     );
# """

SQL_CREATE_TEMP_TABLES = """
    DROP TABLE IF EXISTS DS$I_{{ params.base_table }} CASCADE;
    CREATE TABLE DS$I_{{ params.base_table }} (
      identificatie_lokaalid character varying PRIMARY KEY,
      type character varying,
      geometrie geometry,
      relatievehoogteligging integer,
      bron character varying,
      minzoom integer,
      maxzoom integer     
    );
"""

def create_tables_from_basiskaartdb_to_masterdb(source_connection, source_select_statement, base_table, *args, **kwargs):
    """
    Source_connection contains the environment variable name as defined in the docker-compose.yml file of the source connection i.e. AIRFLOW_CONN_POSTGRES_BASISKAART
    Select_statement contains the SQL select query that will be executed on the source DB.
    Base_table contains the table in masterDB where the data (the result of select_statement execution) is inserted into.
    """

    try:
        # setup DB source connection 
        source_engine = create_engine(
            env(source_connection).split("?")[0]
        )
    except SQLAlchemyError as e:
        raise Exception(str(e)) from e

    with source_engine.connect() as source_connection:
         # fetch data from source and insert into base table
        count = 0
        cursor = source_connection.execute(source_select_statement)     
        while True:
            fetch_iterator = cursor.fetchmany(size=import_step)
            batch_count = copy_data_in_batch(base_table, fetch_iterator)
            count += batch_count            
            if batch_count < import_step:
                break
        print(f"Imported: {count}")


def copy_data_in_batch(base_table, fetch_iterator):
    items = []
    for row in fetch_iterator:        
        items.append(
            '(' + ','.join([f"'{column_value}'" if  column_value else "NULL" for column_value in row ]) + ')'                        
        )   

    result = len(items)
    if result:
        items_sql = ",".join(items)
        insert_sql = (
            f"INSERT INTO DS$I_{base_table} "
            f"VALUES {items_sql};"
        )

        masterdb_hook = PostgresHook()
        try:
            masterdb_hook.run(insert_sql)
        except Exception as e:
            raise Exception("Failed to insert batch data: {}".format(str(e)[0:150]))
        else:
            print("Created {} records.".format(result))
    return result

with DAG(
    dag_id,
    description="basisregistratie grootschalige topologie (BGT) en kleinschalige basiskaart (KBK10 en 50)",
    default_args=default_args,  
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create target table
    create_temp_tables = [ 
            PostgresOperator(
            task_id=f"create_{table}",
            sql=SQL_CREATE_TEMP_TABLES,
            params=dict(base_table=table),
    )
    for table in tables_to_create.keys()
    ]

    # 3. Copy data
    copy_data = [
            PythonOperator(
            task_id=f"insert_{table}",
            python_callable=create_tables_from_basiskaartdb_to_masterdb,
            op_kwargs={ "source_connection":source_connection, 
                        "source_select_statement":globals()[select_statement], 
                        "base_table":table,                                                 
                        },
            dag=dag,
    )
    for table, select_statement in tables_to_create.items()
    ]


# Due to balancing reasons
# sequential load order of the steps

first_step = False
previous_step = None

for create_temp_table, fetch_data in zip(create_temp_tables, copy_data):
    if previous_step:
        previous_step >> create_temp_table
    
    if not first_step:    
        slack_at_start >> create_temp_table >> fetch_data
        first_step = True
    else:
        create_temp_table >> fetch_data

    previous_step = fetch_data




dag.doc_md = """
    #### DAG summery
    This DAG containts BGT (basisregistratie grootschalige topografie) and KBK10 (kleinschalige basis kaart 10) and KBK50 (kleinschalige basis kaart 50) data
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/basiskaart.html    
"""
