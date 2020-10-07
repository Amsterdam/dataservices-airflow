import json
import pathlib
from airflow import DAG
from dynamic_dagrun_operator import TriggerDynamicDagRunOperator
from postgres_table_init_operator import PostgresTableInitOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from http_gob_operator import HttpGobOperator
from common import default_args, DATAPUNT_ENVIRONMENT, addloopvariables
from schematools import TMP_TABLE_POSTFIX

MAX_RECORDS = 1000 if DATAPUNT_ENVIRONMENT == "development" else None

dag_id = "gob"
owner = "gob"

graphql_path = pathlib.Path(__file__).resolve().parents[0] / "graphql"


def create_gob_dag(is_first, gob_dataset_name, gob_table_name):

    gob_db_table_name = f"{gob_dataset_name}_{gob_table_name}"
    graphql_dir_path = graphql_path / f"{gob_dataset_name}-{gob_table_name}"
    graphql_params_path = graphql_dir_path / "args.json"
    extra_kwargs = {}
    schedule_start_hour = 6
    if graphql_params_path.exists():
        with graphql_params_path.open() as json_file:
            args_from_file = json.load(json_file)
            extra_kwargs = args_from_file.get("extra_kwargs", {})
            # schedule_start_hour += args_from_file.get("schedule_start_hour_offset", 0)

    dag = DAG(
        f"{dag_id}_{gob_db_table_name}",
        default_args={"owner": owner, **default_args},
        schedule_interval=f"0 {schedule_start_hour} * * *" if is_first else None,
        tags=["gob"],
    )

    kwargs = dict(
        task_id=f"load_{gob_db_table_name}",
        endpoint="gob/graphql/streaming/",
        dataset=gob_dataset_name,
        schema=gob_table_name,
        retries=3,
        graphql_query_path=graphql_dir_path / "query.graphql",
        max_records=MAX_RECORDS,
        http_conn_id="gob_graphql",
    )

    with dag:
        init_table = PostgresTableInitOperator(
            task_id=f"init_{gob_db_table_name}",
            table_name=f"{gob_db_table_name}{TMP_TABLE_POSTFIX}",
            drop_table=True,
        )

        load_data = HttpGobOperator(**{**kwargs, **extra_kwargs})

        copy_table = PostgresTableCopyOperator(
            task_id=f"copy_{gob_db_table_name}",
            source_table_name=f"{gob_db_table_name}{TMP_TABLE_POSTFIX}",
            target_table_name=gob_db_table_name,
        )

        trigger_next_dag = TriggerDynamicDagRunOperator(
            task_id="trigger_next_dag", dag_id_prefix="gob_", trigger_rule="all_done",
        )

        init_table >> load_data >> copy_table >> trigger_next_dag

    return dag


for gob_gql_dir, is_first, is_last in addloopvariables(graphql_path.glob("*")):
    gob_dataset_name, gob_table_name = gob_gql_dir.parts[-1].split("-")
    globals()[f"gob_{gob_dataset_name}_{gob_table_name}"] = create_gob_dag(
        is_first, gob_dataset_name, gob_table_name
    )
