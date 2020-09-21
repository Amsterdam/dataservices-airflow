import json
import pathlib
from airflow import DAG
from postgres_table_init_operator import PostgresTableInitOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from http_gob_operator import HttpGobOperator
from common import default_args, DATAPUNT_ENVIRONMENT

MAX_RECORDS = 1000 if DATAPUNT_ENVIRONMENT == "development" else None

dag_id = "gob"
owner = "gob"

graphql_path = pathlib.Path(__file__).resolve().parents[0] / "graphql"


def create_gob_dag(gob_dataset_name, gob_table_name):

    gob_db_table_name = f"{gob_dataset_name}_{gob_table_name}"
    dag = DAG(
        f"{dag_id}_{gob_db_table_name}", default_args={"owner": owner, **default_args}
    )
    graphql_dir_path = graphql_path / f"{gob_dataset_name}-{gob_table_name}"
    graphql_params_path = graphql_dir_path / "args.json"
    extra_kwargs = {}
    if graphql_params_path.exists():
        with graphql_params_path.open() as json_file:
            extra_kwargs = json.load(json_file)

    kwargs = dict(
        task_id=f"load_{gob_db_table_name}",
        endpoint="gob/graphql/streaming/",
        dataset=gob_dataset_name,
        schema=gob_table_name,
        graphql_query_path=graphql_dir_path / "query.graphql",
        max_records=MAX_RECORDS,
        http_conn_id="gob_graphql",
    )

    with dag:
        init_table = PostgresTableInitOperator(
            task_id=f"init_{gob_db_table_name}",
            table_name=f"{gob_db_table_name}_new",
            drop_table=True,
        )

        load_data = HttpGobOperator(**{**kwargs, **extra_kwargs})

        copy_table = PostgresTableCopyOperator(
            task_id=f"copy_{gob_db_table_name}",
            source_table_name=f"{gob_db_table_name}_new",
            target_table_name=gob_db_table_name,
        )

        init_table >> load_data >> copy_table

    return dag


for gob_gql_dir in graphql_path.glob("*"):
    gob_dataset_name, gob_table_name = gob_gql_dir.parts[-1].split("-")
    globals()[f"gob_{gob_dataset_name}_{gob_table_name}"] = create_gob_dag(
        gob_dataset_name, gob_table_name
    )
