import re
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from pgcomparator_cdc_operator import PgComparatorCDCOperator
from provenance_rename_operator import ProvenanceRenameOperator
from dynamic_dagrun_operator import TriggerDynamicDagRunOperator

from common import (
    env,
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from sql.basiskaart import CREATE_TABLES
from sql.basiskaart import CREATE_MVIEWS

from sql.basiskaart import SELECT_GEBOUW_VLAK_SQL as SELECT_GEBOUW_VLAK_SQL
from sql.basiskaart import SELECT_INRICHTINGSELEMENT_LIJN_SQL as SELECT_INRICHTINGSELEMENT_LIJN_SQL
from sql.basiskaart import SELECT_INRICHTINGSELEMENT_PUNT_SQL as SELECT_INRICHTINGSELEMENT_PUNT_SQL
from sql.basiskaart import SELECT_INRICHTINGSELEMENT_VLAK_SQL as SELECT_INRICHTINGSELEMENT_VLAK_SQL
from sql.basiskaart import SELECT_SPOOR_LIJN_SQL as SELECT_SPOOR_LIJN_SQL
from sql.basiskaart import SELECT_TERREINDEEL_VLAK_SQL as SELECT_TERREINDEEL_VLAK_SQL
from sql.basiskaart import SELECT_WATERDEEL_LIJN_SQL as SELECT_WATERDEEL_LIJN_SQL
from sql.basiskaart import SELECT_WATERDEEL_VLAK_SQL as SELECT_WATERDEEL_VLAK_SQL
from sql.basiskaart import SELECT_WEGDEEL_VLAK_SQL as SELECT_WEGDEEL_VLAK_SQL
from sql.basiskaart import SELECT_WEGDEEL_LIJN_SQL as SELECT_WEGDEEL_LIJN_SQL
from sql.basiskaart import SELECT_LABELS_SQL as SELECT_LABELS_SQL


dag_id = "basiskaart"
owner = "dataservices"
variables = Variable.get(dag_id, deserialize_json=True)
tables_to_create = variables["tables_to_create"]
source_connection="AIRFLOW_CONN_POSTGRES_BASISKAART"
import_step = 10000


def create_tables_from_basiskaartdb_to_masterdb(source_connection, source_select_statement, target_base_table, *args, **kwargs):
    """
    Source_connection contains the environment variable name (as defined in the docker-compose.yml) of the source connection i.e. AIRFLOW_CONN_POSTGRES_BASISKAART
    Source_select_statement contains the SQL select query that will be executed on the source DB.
    Target_base_table contains the table in master DB where the data (the result of source_select_statement execution) is inserted into.
    """

    try:
        # setup the DB source connection
        source_engine = create_engine(
            env(source_connection).split("?")[0]
        )
    except SQLAlchemyError as e:
        raise Exception(str(e)) from e

    # fetch data from source DB
    with source_engine.connect() as source_connection:
        count = 0
        cursor = source_connection.execute(source_select_statement)     
        while True:
            fetch_iterator = cursor.fetchmany(size=import_step)
            batch_count = copy_data_in_batch(target_base_table, fetch_iterator)
            count += batch_count
            if batch_count < import_step:
                break
        print(f"Imported: {count}")


def copy_data_in_batch(target_base_table, fetch_iterator):
    
    items = []
    
    # setup SQL insert values the be executed
    for row in fetch_iterator:

        row_insert = []        
        for column_value in row:
            
            # escape single quote in labels, conflicts with sql insert statement i.e. Jachthaven 't Eiland
            if 'labels' in target_base_table:                
                p = re.compile("([\w\s]{0,})(')([\w\s]+)")
                m = p.match(str(column_value))
                if m:
                    column_value = m.group(1)+'\''+m.group(2)+m.group(3)
            
            row_insert.append(f"'{column_value}'" if column_value else "NULL")
        
        items.append ('(' + ','.join(row_insert) + ')')
            
    result = len(items)

    # execute SQL insert statement
    if result:
        items_sql = ",".join(items)
        insert_sql = (
            f"INSERT INTO {dag_id}_{target_base_table}_TEMP "
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


def create_basiskaart_dag(is_first, table_name, select_statement):
    """ 
    DAG generator: Generates a DAG for each table.
    The table_name is the target table in de masterDB where the data will be inserted.
    The select_statement is one of the imported SQL query selects (see above) that will be executed on the source DB.    
    """
    # start time first DAG
    # Note: the basiskaartimport task in Jenkins runs at an arbitrary but invariant time between 3 and 5 a.m.
    # Because of this, the first DAG starts running at 7 a.m.
    schedule_start_hour = 7

    dag = DAG(
        f"{dag_id}_{table_name}",
        default_args={"owner": owner, **default_args},
        # the first DAG will have the is_first boolean set to True
        # the other DAG's will be triggered to start when the previous DAG is finished (estafette run / relay run)
        schedule_interval=f"0 {schedule_start_hour} * * *" if is_first else None,
        description="""basisregistratie grootschalige topologie (BGT) en kleinschalige basiskaart (KBK10 en 50). 
        The basiskaart data is collected from basiskaart DB.""",
        tags=["basiskaart"],
    )
    
    with dag:

        # 1. Post info message on slack
        slack_at_start = MessageOperator(
            task_id="slack_at_start",
            http_conn_id="slack",
            webhook_token=slack_webhook_token,
            message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
            username="admin",
        )

        # 2. Create temp and target table
        create_tables = PostgresOperator(
                task_id="create_tables",
                sql=CREATE_TABLES,
                params=dict(base_table=table_name, dag_id=dag_id),
        )
        
        # 3. Copy data into temp table
        copy_data = PythonOperator(
                task_id="insert_data",
                python_callable=create_tables_from_basiskaartdb_to_masterdb,
                op_kwargs={ "source_connection":source_connection,
                            "source_select_statement":globals()[select_statement],
                            "target_base_table":table_name,
                            },
                dag=dag,
        )       

        # 4. Check for changes in temp table to merge in target table
        change_data_capture = PgComparatorCDCOperator (
               task_id="change_data_capture", 
               source_table=f"{dag_id}_{table_name}_temp",
               target_table=f"{dag_id}_{table_name}"
        )

        # 5. Create mviews for T-REX tile server
        create_mviews = PostgresOperator(
                task_id="create_mviews",
                sql=CREATE_MVIEWS,
                params=dict(base_table=table_name, dag_id=dag_id),
        )

        # 6. Rename COLUMNS based on Provenance
        provenance_translation = ProvenanceRenameOperator(
                task_id="rename_columns",
                dataset_name=f"{dag_id}",
                prefix_table_name=f"{table_name}",
                rename_indexes=False,
                pg_schema="public",
            )

        # 7. Drop temp table
        clean_up = PostgresOperator(
                task_id="drop_temp_table",
                sql=[f"DROP TABLE IF EXISTS {dag_id}_{table_name}_temp CASCADE",],
        )

        # 8. Trigger next DAG to run (estafette)
        trigger_next_dag = TriggerDynamicDagRunOperator(
            task_id="trigger_next_dag", dag_id_prefix=f"{dag_id}_", trigger_rule="all_done",
        )       

    # Flow
    slack_at_start >> create_tables >> copy_data >> change_data_capture >> create_mviews >> provenance_translation >> clean_up >> trigger_next_dag

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
    NA
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/basiskaart.html
    Note: The basiskaart data is collected from the GOB objectstore and processed in the basiskaart DB => which is the source for this DAG.
    """

    return dag


for i, (table, select_statement) in enumerate(tables_to_create.items()):
    globals()[f"{dag_id}_{table}"] = create_basiskaart_dag(
        i == 0, table, select_statement
    )







