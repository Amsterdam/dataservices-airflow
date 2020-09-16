import json
import pathlib
from airflow import DAG
from postgres_drop_operator import PostgresTableDropOperator
from postgres_rename_operator import PostgresTableRenameOperator
from http_gob_operator import HttpGobOperator
from common import default_args, DATAPUNT_ENVIRONMENT

MAX_RECORDS = 10000 if DATAPUNT_ENVIRONMENT == "development" else None

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
        drop_old_tmp_tables = PostgresTableDropOperator(
            task_id=f"drop_tmp_{gob_db_table_name}",
            table_name=f"{gob_db_table_name}_new",
        )

        load_data = HttpGobOperator(**{**kwargs, **extra_kwargs})

        rename_table = PostgresTableRenameOperator(
            task_id=f"rename_{gob_db_table_name}",
            old_table_name=f"{gob_db_table_name}_new",
            new_table_name=gob_db_table_name,
        )

        drop_old_tmp_tables >> load_data >> rename_table

    return dag


for gob_gql_dir in graphql_path.glob("*"):
    gob_dataset_name, gob_table_name = gob_gql_dir.parts[-1].split("-")
    globals()[f"gob_{gob_dataset_name}_{gob_table_name}"] = create_gob_dag(
        gob_dataset_name, gob_table_name
    )
