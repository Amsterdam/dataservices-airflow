import json
import pathlib
from airflow import DAG
from dynamic_dagrun_operator import TriggerDynamicDagRunOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from postgres_table_init_operator import PostgresTableInitOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from http_gob_operator import HttpGobOperator
from common import (
    default_args,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
    slack_webhook_token,
    env,
)
from schematools import TMP_TABLE_POSTFIX

MAX_RECORDS = 1000 if DATAPUNT_ENVIRONMENT == "development" else None
GOB_PUBLIC_ENDPOINT = env("GOB_PUBLIC_ENDPOINT")
GOB_SECURE_ENDPOINT = env("GOB_SECURE_ENDPOINT")

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
            protected = extra_kwargs.get("protected", False)
            if protected:
                extra_kwargs["endpoint"] = GOB_SECURE_ENDPOINT

    dag = DAG(
        f"{dag_id}_{gob_db_table_name}",
        default_args={"owner": owner, **default_args},
        schedule_interval=f"0 {schedule_start_hour} * * *" if is_first else None,
        tags=["gob"],
    )

    kwargs = dict(
        task_id=f"load_{gob_db_table_name}",
        endpoint=GOB_PUBLIC_ENDPOINT,
        dataset=gob_dataset_name,
        schema=gob_table_name,
        retries=3,
        graphql_query_path=graphql_dir_path / "query.graphql",
        max_records=MAX_RECORDS,
        http_conn_id="gob_graphql",
    )

    with dag:

        # 1. Post info message on slack
        slack_at_start = MessageOperator(
            task_id=f"slack_at_start_{gob_db_table_name}",
            http_conn_id="slack",
            webhook_token=slack_webhook_token,
            message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
            username="admin",
        )

        # 2. drop temp table if exists
        init_table = PostgresTableInitOperator(
            task_id=f"init_{gob_db_table_name}",
            table_name=f"{gob_db_table_name}{TMP_TABLE_POSTFIX}",
            drop_table=True,
        )

        # 3. load data into temp table
        load_data = HttpGobOperator(**{**kwargs, **extra_kwargs})

        # 4. truncate target table and insert data from temp table
        copy_table = PostgresTableCopyOperator(
            task_id=f"copy_{gob_db_table_name}",
            source_table_name=f"{gob_db_table_name}{TMP_TABLE_POSTFIX}",
            target_table_name=gob_db_table_name,
        )

        # 5. create an index on the identifier fields (as specified in the JSON data schema)
        create_extra_index = SqlAlchemyCreateObjectOperator(
            task_id=f"create_extra_index_{gob_db_table_name}",
            data_schema_name=kwargs.get("dataset", None),
            data_table_name=f"{gob_db_table_name}",
            # when set to false, it doesn't create the tables; only the index
            ind_table=False,
            ind_extra_index=True,
        )

        # 6. trigger next DAG (estafette / relay run)
        trigger_next_dag = TriggerDynamicDagRunOperator(
            task_id="trigger_next_dag",
            dag_id_prefix="gob_",
            trigger_rule="all_done",
        )

        # FLOW
        (
            slack_at_start
            >> init_table
            >> load_data
            >> copy_table
            >> create_extra_index
            >> trigger_next_dag
        )

    return dag


for i, gob_gql_dir in enumerate(sorted(graphql_path.glob("*"))):
    gob_dataset_name, gob_table_name = gob_gql_dir.parts[-1].split("-")
    globals()[f"gob_{gob_dataset_name}_{gob_table_name}"] = create_gob_dag(
        i == 0, gob_dataset_name, gob_table_name
    )
