import json
import pathlib
from dataclasses import dataclass
from typing import Any, Final

from airflow import DAG
from airflow.operators.python import PythonOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, env, slack_webhook_token
from contact_point.callbacks import get_contact_point_on_failure_callback
from dynamic_dagrun_operator import TriggerDynamicDagRunOperator
from http_gob_operator import HttpGobOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from postgres_table_init_operator import PostgresTableInitOperator
from schematools import TMP_TABLE_POSTFIX
from schematools.utils import schema_def_from_url
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

MAX_RECORDS: Final = 1000 if DATAPUNT_ENVIRONMENT == "development" else None
GOB_PUBLIC_ENDPOINT: Final = env("GOB_PUBLIC_ENDPOINT")
GOB_SECURE_ENDPOINT: Final = env("GOB_SECURE_ENDPOINT")
OAUTH_TOKEN_EXPIRES_MARGIN: Final = env.int("OAUTH_TOKEN_EXPIRES_MARGIN", 5)
SCHEMA_URL: Final = env("SCHEMA_URL")

dag_id = "gob"
owner = "gob"

graphql_path = pathlib.Path(__file__).resolve().parents[0] / "graphql"


@dataclass
class DatasetInfo:
    """Dataclass to provide canned infomation about the dataset for
    other operators to work with."""

    schema_url: str
    dataset_id: str
    table_id: str
    dataset_table_id: str
    db_table_name: str


def create_gob_dag(is_first: bool, gob_dataset_id: str, gob_table_id: str) -> DAG:

    dataset_table_id = f"{gob_dataset_id}_{gob_table_id}"
    graphql_dir_path = graphql_path / f"{gob_dataset_id}-{gob_table_id}"
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
        f"{dag_id}_{dataset_table_id}",
        default_args={"owner": owner, **default_args},
        schedule_interval=f"0 {schedule_start_hour} * * *" if is_first else None,
        tags=["gob"],
        on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
    )

    kwargs = dict(
        task_id=f"load_{dataset_table_id}",
        endpoint=GOB_PUBLIC_ENDPOINT,
        retries=3,
        graphql_query_path=graphql_dir_path / "query.graphql",
        max_records=MAX_RECORDS,
        http_conn_id="gob_graphql",
        token_expires_margin=OAUTH_TOKEN_EXPIRES_MARGIN,
        xcom_table_info_task_ids=f"mkinfo_{dataset_table_id}",
    )

    with dag:

        # 1. Post info message on slack
        slack_at_start = MessageOperator(
            task_id=f"slack_at_start_{dataset_table_id}",
            http_conn_id="slack",
            webhook_token=slack_webhook_token,
            message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
            username="admin",
        )

        def _create_dataset_info(dataset_id: str, table_id: str) -> DatasetInfo:
            dataset = schema_def_from_url(SCHEMA_URL, dataset_id, prefetch_related=True)
            # Fetch the db_name for this dataset and table
            db_table_name = dataset.get_table_by_id(table_id).db_name()

            # We do not pass the dataset through xcom, but only the id.
            # The methodtools.lru_cache decorator is not pickleable
            # (Airflow uses pickle for (de)serialization).
            # provide the dataset_table_id as fully qualified name, for convenience
            dataset_table_id = f"{dataset_id}_{table_id}"
            return DatasetInfo(SCHEMA_URL, dataset_id, table_id, dataset_table_id, db_table_name)

        # 2. Create Dataset info to put on the xcom channel for later use
        # by operators
        create_dataset_info = PythonOperator(
            task_id=f"mkinfo_{dataset_table_id}",
            python_callable=_create_dataset_info,
            op_args=(gob_dataset_id, gob_table_id),
        )

        def init_assigner(o: Any, x: Any) -> None:
            o.table_name = f"{x.db_table_name}{TMP_TABLE_POSTFIX}"

        # 3. drop temp table if exists
        init_table = PostgresTableInitOperator(
            task_id=f"init_{dataset_table_id}",
            table_name=None,
            xcom_task_ids=f"mkinfo_{dataset_table_id}",
            xcom_attr_assigner=init_assigner,
            drop_table=True,
        )

        # 4. load data into temp table
        load_data = HttpGobOperator(**{**kwargs, **extra_kwargs})

        def copy_assigner(o: Any, x: Any) -> None:
            o.source_table_name = f"{x.db_table_name}{TMP_TABLE_POSTFIX}"
            o.target_table_name = x.db_table_name

        # 5. truncate target table and insert data from temp table
        copy_table = PostgresTableCopyOperator(
            task_id=f"copy_{dataset_table_id}",
            source_table_name=None,
            target_table_name=None,
            xcom_task_ids=f"mkinfo_{dataset_table_id}",
            xcom_attr_assigner=copy_assigner,
        )

        def index_assigner(o: Any, x: Any) -> None:
            o.data_table_name = x.db_table_name

        # 6. create an index on the identifier fields (as specified in the JSON data schema)
        create_extra_index = SqlAlchemyCreateObjectOperator(
            task_id=f"create_extra_index_{dataset_table_id}",
            data_schema_name=gob_dataset_id,
            data_table_name=None,
            # when set to false, it doesn't create the tables; only the index
            ind_table=False,
            ind_extra_index=True,
            xcom_task_ids=f"mkinfo_{dataset_table_id}",
            xcom_attr_assigner=index_assigner,
        )

        # 7. trigger next DAG (estafette / relay run)
        trigger_next_dag = TriggerDynamicDagRunOperator(
            task_id="trigger_next_dag",
            dag_id_prefix="gob_",
            trigger_rule="all_done",
        )

        # 9. Grant database permissions
        grant_db_permissions = PostgresPermissionsOperator(
            task_id="grants", dag_name=f"{dag_id}_{dataset_table_id}"
        )

        # FLOW
        (
            slack_at_start
            >> create_dataset_info
            >> init_table
            >> load_data
            >> copy_table
            >> create_extra_index
            >> trigger_next_dag
            >> grant_db_permissions
        )

    return dag


for i, gob_gql_dir in enumerate(sorted(graphql_path.glob("*"))):
    gob_dataset_id, gob_table_id = gob_gql_dir.parts[-1].split("-")
    globals()[f"gob_{gob_dataset_id}_{gob_table_id}"] = create_gob_dag(
        i == 0, gob_dataset_id, gob_table_id
    )
