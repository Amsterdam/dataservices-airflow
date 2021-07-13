import logging
from datetime import timedelta
from typing import Any, Dict, Final, Optional, Union

import pendulum
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.settings import TIMEZONE, Session
from environs import Env
from requests.exceptions import HTTPError
from schematools.cli import _get_engine
from schematools.permissions.db import apply_schema_and_profile_permissions
from schematools.utils import schema_defs_from_url

env = Env()

# split is used to remove url params, if exists.
# For database connection the url params must be omitted.
default_db_conn = env("AIRFLOW_CONN_POSTGRES_DEFAULT").split("?")[0]
default_schema_url = env("SCHEMA_URL")
datapunt_evironment = env("DATAPUNT_ENVIRONMENT")

# some DAG's do not match dag_id with dataschema name.
# the reason can be that for one dataset multiple DAG's are implemented i.e.
# for each table one DAG.
# Since the dataschema is the basis for the database permissions, for the situation
# where dag_id does not match dataschema name, the relation between the dag_id and
# schema name is held in this constant.
DAG_DATASET: Final = {
    "basiskaart_": "basiskaart",
    "bag_": "bag",
    "brk_": "brk",
    "gebieden_": "gebieden",
    "nap_": "nap",
    "meetbouten_": "meetbouten",
    "woz_": "woz",
    "hr_": "hr",
    "horeca_": "horeca",
    "beschermde_": "beschermdestadsdorpsgezichten",
    "hoofdroutes_": "hoofdroutes",
    "reclamebelasting": "belastingen",
    "processenverbaalverkiezingen": "verkiezingen",
    "financien_": "financien",
}


class PostgresPermissionsOperator(BaseOperator):
    """This operator executes DB grants << on >> DB tables / fields << to >> DB roles.

    It can be envoked based upon a single dataset or a list of datasets.
    It uses the dag_id to identify the dataset.

    The DB roles are derived from the AUTH element as defined in the Amsterdam schema.
    When no AUTH element is provided in the Amsterdam schema, it default to DB role SCOPE_OPENBAAR.
    DB users can be assigned as member to the DB roles to get access to DB objects.

    Caveat:
    When objects are dropped in the DB, the grants to the roles are removed as well.
    This results that grants must be (re)applied again to the roles.

    An other way to deal with is, is to update tables based upon the pgcomparator_cdc_operator.py.
    Which detects differences between source and target table, and upserts the differences into
    target table if nessacery. In stead of dropping, recreating the target table and inserting the
    records on each run.
    """

    def __init__(
        self,
        batch_ind: bool = False,
        batch_timewindow: str = "0:30",
        dag_name: Union[str, None] = None,
        db_conn: str = default_db_conn,
        schema_url: str = default_schema_url,
        db_schema: str = "public",
        profiles: Union[str, None] = None,
        role: str = "AUTO",
        scope: Union[str, None] = None,
        dry_run: bool = False,
        create_roles: bool = True,
        revoke: bool = False,
        *args: Any,
        **kwargs: Dict,
    ):
        """Initialize paramaters."""
        super().__init__(*args, **kwargs)
        self.batch_ind = batch_ind
        self.batch_timewindow = batch_timewindow
        self.dataset_name = dag_name
        self.db_conn = db_conn
        self.schema_url = schema_url
        self.db_schema = db_schema
        self.profiles = profiles
        self.role = role
        self.scope = scope
        self.dry_run = dry_run
        self.create_roles = create_roles
        self.revoke = revoke

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:  # noqa: C901
        """Executes 'apply_schema_and_profile_permissions' method  from \
            schema-tools to set the database permissions on objects to roles.

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.

        Executes:
            SQL grant statements on database tables to database roles

        """
        # setup logger so output can be added to the Airflow logs
        logger = logging.getLogger(__name__)

        # setup database connection where the database objects are present
        engine = _get_engine(self.db_conn)

        # Option ONE: batch grant
        if self.batch_ind:

            # get current datetime and make it aware of the TZ (mandatory)
            now = pendulum.now(TIMEZONE)

            # calculate the delta between current datetime and specified time window
            time_window_hour = int(self.batch_timewindow.split(":")[0])
            time_window_minutes = int(self.batch_timewindow.split(":")[1])
            delta = now - timedelta(hours=time_window_hour, minutes=time_window_minutes)

            logger.info("the time window is set starting at: %s till now", delta)

            # setup an Airflow session to access Airflow repository data
            session = Session()

            # get list of dags that meet time window and state outcome
            # it uses the Airflow DagRun class to get data
            executed_dags_after_delta = [
                dag.dag_id
                for dag in session.query(DagRun)
                .filter(DagRun.end_date > delta)
                .filter(DagRun._state == "success")
                # exclude the dag itself that calls this batch grant method
                .filter(DagRun.dag_id != "airflow_db_permissions")
                # exclude the update_dag, it does not contain DB objects to grant
                .filter(DagRun.dag_id != "update_dags")
                # exclude the log_cleanup dag, it does not contain DB objects to grant
                .filter(DagRun.dag_id != "airflow_log_cleanup")
            ]

            if executed_dags_after_delta:

                for dataset_name in executed_dags_after_delta:

                    # get real datasetname from DAG_DATASET constant, if dag_id != dataschema name
                    for key in DAG_DATASET.keys():
                        if key in dataset_name:
                            dataset_name = DAG_DATASET[key]
                            break

                    logger.info("set grants for %s", dataset_name)

                    try:
                        ams_schema = schema_defs_from_url(
                            schemas_url=self.schema_url,
                            dataset_name=dataset_name,
                            prefetch_related=True,
                        )

                        apply_schema_and_profile_permissions(
                            engine=engine,
                            pg_schema=self.db_schema,
                            ams_schema=ams_schema,
                            profiles=self.profiles,
                            role=self.role,
                            scope=self.scope,
                            dry_run=self.dry_run,
                            create_roles=self.create_roles,
                            revoke=self.revoke,
                        )

                    except HTTPError:
                        logger.error("Could not get data schema for %s", dataset_name)
                        continue

            else:
                logger.error(
                    "Nothing to grant, no finished dags detected within time window of %s",
                    self.batch_timewindow,
                )

        # Option TWO: grant on single dataset (can be used as a final step within a dag run)
        elif self.dataset_name and not self.batch_ind:

            # get real datasetname from DAG_DATASET constant, if dag_id != dataschema name
            for key in DAG_DATASET.keys():
                if key in self.dataset_name:
                    self.dataset_name = DAG_DATASET[key]
                    break

            logger.info("set grants for %s", self.dataset_name)

            try:
                ams_schema = schema_defs_from_url(
                    schemas_url=self.schema_url,
                    dataset_name=self.dataset_name,
                    prefetch_related=True,
                )

                apply_schema_and_profile_permissions(
                    engine=engine,
                    pg_schema=self.db_schema,
                    ams_schema=ams_schema,
                    profiles=self.profiles,
                    role=self.role,
                    scope=self.scope,
                    dry_run=self.dry_run,
                    create_roles=self.create_roles,
                    revoke=self.revoke,
                )

            except HTTPError:
                logger.error("Could not get data schema for %s", self.dataset_name)
                pass
