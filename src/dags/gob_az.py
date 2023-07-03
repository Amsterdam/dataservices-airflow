import json
import logging
import pathlib
import os
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Callable, DefaultDict, Final, Optional

import pendulum
from airflow import DAG
from airflow.models.dag import DagModel
from airflow.operators.python import PythonOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, env
from contact_point.callbacks import get_contact_point_on_failure_callback
from dynamic_dagrun_operator import TriggerDynamicDagRunOperator
from http_gob_operator import HttpGobOperator
from more_itertools import first
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from postgres_table_init_operator import PostgresTableInitOperator
from schematools import TMP_TABLE_POSTFIX
from schematools.loaders import get_schema_loader as dataset_schema_from_url
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

MAX_RECORDS: Final = 1000 if DATAPUNT_ENVIRONMENT == "development" else None
GOB_PUBLIC_ENDPOINT: Final = env("GOB_PUBLIC_ENDPOINT")
GOB_SECURE_ENDPOINT: Final = env("GOB_SECURE_ENDPOINT")
OAUTH_TOKEN_EXPIRES_MARGIN: Final = env.int("OAUTH_TOKEN_EXPIRES_MARGIN", 5)
SCHEMA_URL: Final = env("SCHEMA_URL")

# Schedule information is stored in an environment variable,
# and not an Airflow variable. According to the Airflow
# docs, variables should be used sparingly outside of operators.

# There are several crontab-based schedules,
# and every schedule has a list of datasets
# that need to run in a sequential fashion (the estafette run).
# Every first item of the lists of datasets should be
# unique, because this should be the dataset that starts
# a particular estafette run.
GOB_SCHEDULES = """meetbouten,gebieden,bag,nap:15 7 * * 2
nap,gebieden,meetbouten,brk:0 15 * * 3
bag,gebieden,meetbouten,nap:0 23 * * 4
gebieden,meetbouten,nap,woz:0 18 * * 5
brk:0 23 * * 0"""
SCHEDULES: Final[str] = env("GOB_SCHEDULES", ";".join(GOB_SCHEDULES.split("\n")))

# Lookup for dataset ids mapped to a cron-style schedule.
# This lookup is only used for the first dataset in a series with an identical schedule.
SCHEDULE_LOOKUP: dict[str, str] = {}


# Lookup that maps a cron-style schedule to a list of datasets.
# This lookup is needed for the ordering of dagruns, that needs
# to reflect the ordering in the list of datasets.
SCHEDULE2DATASETS: dict[str, list[str]] = {}

# Lookup that maps every dataset to a set of cron-style schedules.
# These schedules are attached to the dataset as labels (aka tags).
# Those labels are needed by the "estafette" system to determine
# the next dag that needs to be run.
SCHEDULE_LABELS_LOOKUP: DefaultDict[str, set[str]] = defaultdict(set)

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "gob_az"
DATASET_ID: Final = "gob"

# owner: Only needed for CloudVPS. On Azure
# each team has its own Airflow instance.
owner = "team_benk"

graphql_path = pathlib.Path(__file__).resolve().parents[0] / "graphql"

# Convenience logger, to be able to log to the logs that
# are visible from the Web UI.
logger = logging.getLogger("airflow.task")


@dataclass
class DatasetInfo:
    """Dataclass to provide canned dataset information.

    Information about the dataset for other operators to work with.
    """

    schema_url: str
    dataset_id: str
    table_id: str
    sub_table_id: str
    dataset_table_id: str
    db_table_name: str
    nested_db_table_names: list[str]


def create_gob_dag(
    schedule_interval: Optional[str],
    gob_dataset_id: str,
    gob_table_id: str,
    sub_table_id: Optional[str] = None,
    *,
    schedule_labels: Iterable[str] = (),
) -> DAG:
    """Create the DAG."""
    dataset_table_id = f"{gob_dataset_id}_{gob_table_id}"
    if sub_table_id is not None:
        dataset_table_id = f"{dataset_table_id}_{sub_table_id}"
    graphql_dir_name = f"{gob_dataset_id}-{gob_table_id}"
    if sub_table_id is not None:
        graphql_dir_name = f"{graphql_dir_name}-{sub_table_id}"
    graphql_dir_path = graphql_path / graphql_dir_name
    graphql_params_path = graphql_dir_path / "args.json"
    extra_kwargs = {}

    if graphql_params_path.exists():
        with graphql_params_path.open() as json_file:
            args_from_file = json.load(json_file)
            extra_kwargs = args_from_file.get("extra_kwargs", {})
            protected = extra_kwargs.get("protected", False)
            if protected:
                extra_kwargs["endpoint"] = GOB_SECURE_ENDPOINT

    new_default_args = default_args.copy()
    new_default_args["owner"] = owner

    # The default `start_date` parameter is two days in the past,
    # however, that does not work for the GOB schedules
    # because those are running less frequently.
    # Furthermore, setting `start_date` to a dynamic data is
    # currently frowned upon in the Airflow documentation.
    # The `catchup` parameter should be set to `False` though,
    # to avoid lots of DAG runs.
    new_default_args["start_date"] = pendulum.datetime(2022, 1, 1, tz="UTC")

    dag = DAG(
        f"{DAG_ID}_{dataset_table_id}",
        default_args=new_default_args,
        # the access_control defines perms on DAG level. Not needed in Azure
        # since each datateam will get its own instance.
        access_control={owner: {"can_dag_read", "can_dag_edit"}},
        schedule_interval=schedule_interval,
        tags=["gob"] + [gob_dataset_id] + list(schedule_labels),
        on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
        render_template_as_native_obj=True,
        params={"schedule": schedule_interval},
    )

    kwargs = {
        "task_id": f"load_{dataset_table_id}",
        "endpoint": GOB_PUBLIC_ENDPOINT,
        "retries": 3,
        "graphql_query_path": graphql_dir_path / "query.graphql",
        "max_records": MAX_RECORDS,
        "http_conn_id": "gob_graphql",
        "token_expires_margin": OAUTH_TOKEN_EXPIRES_MARGIN,
        "xcom_table_info_task_ids": f"mkinfo_{dataset_table_id}",
    }

    with dag:

        # 1. Post info message on slack
        slack_at_start = MessageOperator(
            task_id="slack_at_start",
        )

        def _create_dataset_info(dataset_id: str, table_id: str, sub_table_id: str) -> DatasetInfo:
            dataset = dataset_schema_from_url(SCHEMA_URL).get_dataset(dataset_id, prefetch_related=True)

            # Fetch the db_name for this dataset and table
            if sub_table_id is not None:
                table_id = f"{table_id}_{sub_table_id}"
            db_table_name = dataset.get_table_by_id(table_id).db_name
            nested_db_table_names = [t.db_name for t in dataset.nested_tables]
            nested_db_table_names = [
                db_name for db_name in nested_db_table_names if db_name.startswith(db_table_name)
            ]

            # We do not pass the dataset through xcom, but only the id.
            # The methodtools.lru_cache decorator is not pickleable
            # (Airflow uses pickle for (de)serialization).
            # provide the dataset_table_id as fully qualified name, for convenience
            dataset_table_id = f"{dataset_id}_{table_id}"
            return DatasetInfo(
                SCHEMA_URL,
                dataset_id,
                table_id,
                sub_table_id,
                dataset_table_id,
                db_table_name,
                nested_db_table_names,
            )

        # 2. Create Dataset info to put on the xcom channel for later use
        # by operators
        create_dataset_info = PythonOperator(
            task_id=f"mkinfo_{dataset_table_id}",
            python_callable=_create_dataset_info,
            op_args=[gob_dataset_id, gob_table_id, sub_table_id],
        )

        def init_assigner(o: Any, x: Any) -> None:
            o.table_name = f"{x.db_table_name}{TMP_TABLE_POSTFIX}"
            o.nested_db_table_names = [f"{n}{TMP_TABLE_POSTFIX}" for n in x.nested_db_table_names]

        # 3. drop temp table if exists
        init_table = PostgresTableInitOperator(
            task_id=f"init_{dataset_table_id}",
            dataset_name=gob_dataset_id,
            table_name=None,
            xcom_task_ids=f"mkinfo_{dataset_table_id}",
            xcom_attr_assigner=init_assigner,
            drop_table=True,
        )

        # 4. load data into temp table
        load_data = HttpGobOperator(**kwargs | extra_kwargs)

        def copy_assigner(o: Any, x: Any) -> None:
            o.source_table_name = f"{x.db_table_name}{TMP_TABLE_POSTFIX}"
            o.nested_db_table_names = [f"{n}{TMP_TABLE_POSTFIX}" for n in x.nested_db_table_names]
            o.target_table_name = x.db_table_name

        # 5. truncate target table and insert data from temp table
        copy_table = PostgresTableCopyOperator(
            task_id=f"copy_{dataset_table_id}",
            source_table_name=None,
            target_table_name=None,
            drop_target_if_unequal=True,
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

        def _get_labels_group(schedule: Optional[str]) -> Optional[set[str]]:
            if schedule is not None:
                return {schedule}
            # We need a fallback if there is no schedule
            # in the dag_run.
            if not schedule_labels:
                return None
            return set(schedule_labels)

        def _get_sort_key(schedule: Optional[str]) -> Callable[[DagModel], Any]:
            def dag_sort_key(dag: DagModel) -> Any:
                # For GOB datasets not included in a schedule (eg HR)
                # we order by `DATASET_ID`, the default.
                if schedule is None:
                    return dag.DATASET_ID

                # For GOB dags, dataset_id does not have `_`
                # so this split is safe.
                dataset_id = dag.DATASET_ID.split("_")[1]
                ordered_datasets = SCHEDULE2DATASETS[schedule]
                position = ordered_datasets.index(dataset_id)

                # A tuple is used for ordering
                # Dominant order is the position in the list of dataset
                # for a particular schedule. Second ordering is done
                # on the `DATASET_ID`.
                return position, dag.DATASET_ID

            return dag_sort_key

        # 7. trigger next DAG (estafette / relay run)

        # The `conf` parameter is jinja-parametrised.
        # So, we can use the `dag_run.conf` information
        # to get the schedule that could have been passed in
        # when the DAG has been triggered (typically in the triggering
        # of the previous DAG in the estafette run).
        # If no schedule is provided, the schedule
        # will be taken from `params`, where `params`
        # is initialized from `schedule_interval` when the DAG
        # is instantiated.

        trigger_next_dag = TriggerDynamicDagRunOperator(
            task_id="trigger_next_dag",
            dag_id_prefix="gob_",
            trigger_rule="all_done",
            labels_group_getter=_get_labels_group,
            sort_key_getter=_get_sort_key,
            conf={"schedule": "{{ dag_run.conf.get('schedule', params.get('schedule')) }}"},
        )

        # 9. Grant database permissions
        grant_db_permissions = PostgresPermissionsOperator(
            task_id="grants", dag_name=f"{DATASET_ID}_{dataset_table_id}", create_roles=False
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


# We need to initialized the lookups that have been
# defined at the top of this DAG module from the
# `SCHEDULES` variable with the schedule definitions.
first_in_group_datasets = set()
for dss_schedule in SCHEDULES.split(";"):
    dataset_names_str, schedule = dss_schedule.split(":")
    dataset_names = dataset_names_str.split(",")
    SCHEDULE2DATASETS[schedule] = dataset_names
    first_in_group_dataset = first(dataset_names)
    # Check if SCHEDULES are set up correctly.
    if first_in_group_dataset in first_in_group_datasets:
        raise ValueError("First datasets should be unique among all groups.")
    first_in_group_datasets.add(first_in_group_dataset)
    for dataset_name in dataset_names:
        if dataset_name == first_in_group_dataset:
            SCHEDULE_LOOKUP[dataset_name] = schedule
        SCHEDULE_LABELS_LOOKUP[dataset_name].add(schedule)

# We need to group the dags on the name of the dataset,
# because some datasets are sharing the same schedule
# and need to be initialized properly.
grouped_dag_params = defaultdict(list)
for gob_gql_dir in sorted(graphql_path.glob("*")):
    ds_table_parts = gob_gql_dir.parts[-1].split("-")
    grouped_dag_params[ds_table_parts[0]].append(ds_table_parts)


for dataset_name, dag_params in grouped_dag_params.items():
    for i, ds_table_parts in enumerate(dag_params):
        schedule_for_ds = SCHEDULE_LOOKUP.get(dataset_name)
        # The schedule_interval is the parameter that Airflow needs
        # for its scheduler.
        # This should only be set for the first DAG in series
        schedule_interval = None if i > 0 else schedule_for_ds
        dag_id_postfix = "_".join(ds_table_parts)
        # Every DAG is tagged with a set of labels
        # representing the other schedules that this
        # particular DAG is part of.
        globals()[f"gob_{dag_id_postfix}"] = create_gob_dag(
            schedule_interval,
            *ds_table_parts,
            schedule_labels=tuple(SCHEDULE_LABELS_LOOKUP.get(dataset_name, ())),
        )
