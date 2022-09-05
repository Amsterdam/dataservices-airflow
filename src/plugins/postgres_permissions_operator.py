import logging
import os
from datetime import timedelta
from typing import Any, Final, Optional, Union

import pendulum
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.settings import TIMEZONE, Session
from airflow.utils.context import Context
from common.db import define_dataset_name_for_azure_dbuser, pg_params
from environs import Env
from requests.exceptions import HTTPError
from schematools.cli import _get_engine
from schematools.permissions.db import apply_schema_and_profile_permissions
from schematools.utils import dataset_schema_from_url

env = Env()

default_schema_url = env("SCHEMA_URL")

# some DAG's do not match dag_id with dataschema name.
# the reason can be that for one dataset multiple DAG's are implemented i.e.
# for each table one DAG.
# Since the dataschema is the basis for the database permissions, for the situation
# where dag_id does not match dataschema name, the relation between the dag_id and
# schema name is held in this constant.
DAG_DATASET: Final = {
    "basiskaart_": ["basiskaart"],
    "bag_": ["bag"],
    "brk_": ["brk"],
    "gebieden_": ["gebieden"],
    "nap_": ["nap"],
    "meetbouten_": ["meetbouten"],
    "woz_": ["woz"],
    "hr_": ["hr"],
    "horeca_": ["horeca"],
    "beschermde_": ["beschermdestadsdorpsgezichten"],
    "hoofdroutes_": ["hoofdroutes"],
    "reclamebelasting": ["belastingen"],
    "processenverbaalverkiezingen": ["verkiezingen"],
    "financien_": ["financien"],
    "wegenbestand": ["wegenbestandZoneZwaarVerkeer", "wegenbestand"],
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
    """

    def __init__(
        self,
        dag_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        batch_ind: bool = False,
        batch_timewindow: str = "0:30",
        schema_url: str = default_schema_url,
        db_schema: str = "public",
        profiles: Union[str, None] = None,
        role: str = "AUTO",
        scope: Union[str, None] = None,
        dry_run: bool = False,
        create_roles: bool = True,
        revoke: bool = False,
        *args: Any,
        **kwargs: dict,
    ):
        """Initialize paramaters."""
        super().__init__(*args, **kwargs)
        self.batch_ind = batch_ind
        self.batch_timewindow = batch_timewindow
        self.dag_name = dag_name
        self.dataset_name = dataset_name
        self.schema_url = schema_url
        self.db_schema = db_schema
        self.profiles = profiles
        self.role = role
        self.scope = scope
        self.dry_run = dry_run
        self.create_roles = create_roles
        self.revoke = revoke

    def execute(self, context: Context) -> None:  # noqa: C901
        """Executes 'apply_schema_and_profile_permissions' method  from \
            schema-tools to set the database permissions on objects to roles.

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.

        Executes:
            SQL grant statements on database tables to database roles

        """
        # If Azure ignore this operator. Since permissions are regulated
        # by Amsterdam schema and is centralized by DaDi.
        # If CloudVPS is not used anymore, then this extra route can be removed.
        if os.environ.get("AZURE_TENANT_ID") is None:

            # chop off -X and remove all trailing spaces
            # for database connection extra params must be omitted.
            default_db_conn = pg_params(dataset_name=self.dataset_name).split("-X")[0].strip()

            # setup logger so output can be added to the Airflow logs
            logger = logging.getLogger(__name__)

            # setup database connection where the database objects are present
            engine = _get_engine(default_db_conn)

            # Option ONE: batch grant (for a single Airflow DAG that sets permissions)
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

                    for self.dag_name in executed_dags_after_delta:
                        dataset_name = []

                        # get real datasetname from DAG_DATASET constant, if dag_id != dataschema name
                        for key in DAG_DATASET.keys():
                            if self.dag_name and key in self.dag_name:
                                for item in DAG_DATASET[key]:
                                    dataset_name.append(item)
                        if not dataset_name and self.dag_name:
                            dataset_name.append(self.dag_name)

                        for dataset in dataset_name:

                            logger.info("set grants for %s", dataset)

                            try:
                                ams_schema = dataset_schema_from_url(
                                    schemas_url=self.schema_url,
                                    dataset_name=dataset,
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
                                logger.error("Could not get data schema for %s", dataset)
                                continue

                else:
                    logger.error(
                        "Nothing to grant, no finished dags detected within time window of %s",
                        self.batch_timewindow,
                    )

            # Option TWO: grant on single dataset (can be used as a final step within a single DAG run)
            elif self.dag_name and not self.batch_ind:
                dataset_name = []

                # get real datasetname from DAG_DATASET constant, if dag_id != dataschema name
                for key in DAG_DATASET.keys():
                    if key in self.dag_name:
                        for item in DAG_DATASET[key]:
                            dataset_name.append(item)
                if not dataset_name:
                    dataset_name.append(self.dag_name)

                for dataset in dataset_name:

                    logger.info("set grants for %s", dataset)

                    try:
                        ams_schema = dataset_schema_from_url(
                            schemas_url=self.schema_url,
                            dataset_name=dataset,
                            prefetch_related=True,
                        )

                        # TODO: there is no option yet to
                        # grant permissions on a single table
                        # only on the whole dataset. Need to
                        # adjust package schema-tools for it.
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
                        logger.error("Could not get data schema for %s", dataset)
                        pass
