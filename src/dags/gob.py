import pathlib
from airflow import DAG
from postgres_drop_operator import PostgresTableDropOperator
from postgres_rename_operator import PostgresTableRenameOperator
from http_gob_operator import HttpGobOperator
from common import default_args


dag_id = "gob"
owner = "gob"

graphql_path = pathlib.Path(__file__).resolve().parents[0] / "graphql"


def create_gob_dag(gob_dataset_name, gob_table_name):

    gob_db_table_name = f"{gob_dataset_name}_{gob_table_name}"
    dag = DAG(
        f"{dag_id}_{gob_db_table_name}", default_args={"owner": owner, **default_args}
    )

    with dag:
        drop_old_tmp_tables = PostgresTableDropOperator(
            task_id=f"drop_tmp_{gob_db_table_name}",
            table_name=f"{gob_db_table_name}_new",
        )

        load_data = HttpGobOperator(
            task_id=f"load_{gob_db_table_name}",
            endpoint="gob/graphql/streaming/",
            dataset=gob_dataset_name,
            schema=gob_table_name,
            id_fields="identificatie,volgnummer",
            geojson_field="geometrie",  # XXX not always relevant
            graphql_query_path=graphql_path
            / f"{gob_dataset_name}-{gob_table_name}.graphql",
            http_conn_id="gob_graphql",
        )

        rename_table = PostgresTableRenameOperator(
            task_id=f"rename_{gob_db_table_name}",
            old_table_name=f"{gob_db_table_name}_new",
            new_table_name=gob_db_table_name,
        )

        drop_old_tmp_tables >> load_data >> rename_table

    return dag


for gob_gql_file in graphql_path.glob("*.graphql"):
    gob_dataset_name, gob_table_name = gob_gql_file.stem.split("-")
    globals()[f"gob_{gob_dataset_name}_{gob_table_name}"] = create_gob_dag(
        gob_dataset_name, gob_table_name
    )
