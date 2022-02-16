import logging
from typing import Final, cast

from airflow.models.dag import DagStateChangeCallback
from airflow.models.taskinstance import Context, TaskInstance
from airflow.settings import TIMEZONE
from airflow.utils.email import send_email
from common import OTAP_ENVIRONMENT, ELIGIBLE_EMAIL_ENVIRONMENTS
from contact_point.models import ContactPoint
from environs import Env
from more_ds.network.url import URL
from pendulum import DateTime
from schematools.types import DatasetSchema
from schematools.utils import schema_from_url

env = Env()
SCHEMA_URL: Final = URL(env("SCHEMA_URL"))
DEFAULT_CONTACT_POINT_NAME: Final = "broneigenaar"

logger = logging.getLogger(__name__)


def get_contact_point_on_failure_callback(
    dataset_id: str, eligible_environments: tuple[str, ...] = ELIGIBLE_EMAIL_ENVIRONMENTS
) -> DagStateChangeCallback:
    """Return a ``contact_point_on_failure_callback`` initialized for a specific dataset.

    Both parameters are implicitly passed into the actual callback by virtue of the closure
    that Python creates for us.

    The callback, when specified on a :class:`~airflow.models.dag.DAG` or
    :class:`~airflow.models.baseoperator.BaseOperator` as the value to the argument
    ``on_failure_callback``, will be invoked when an unhandled exception is raised during the
    execution of the DAG or operator. It will try to retrieve a contact point for the dataset
    it was initialized with and, if found, send a email to that contact point with a notification
    of the failure.

    The email will only be sent if the environment we are running in matches one of the
    environments specified in ``eligible_environments``

    Args:
        dataset_id: The dataset we want to retrieve the contact point from
        eligible_environments: Define the environments in which we are allowed to send email.

    Returns:
        a ``contact_point_on_failure_callback``
    """
    def _contact_point_on_failure_callback(context: Context) -> None:
        logger.info("Import of dataset '%s' failed.", dataset_id)

        ti = cast(TaskInstance, context["ti"])
        logger.debug("Trying to retrieve contact point.")

        dataset = schema_from_url(SCHEMA_URL, DatasetSchema, dataset_id)
        if "contactPoint" in dataset:
            cp = dataset["contactPoint"]
            contact_point = ContactPoint(**cp)
            logger.debug("Contact point found: '%s'", contact_point)
        else:
            contact_point = ContactPoint()
            logger.debug("Contact point not found!")

        if contact_point.email is None:
            logger.warning(
                "No contact point email address was found to sent failure notification to."
            )
            return

        name = contact_point.name if contact_point.name else DEFAULT_CONTACT_POINT_NAME
        execution_date = (
            cast(DateTime, context["execution_date"])
            .in_tz(TIMEZONE)
            .format("dddd D MMMM YYYY, hh:mm:ss", locale="nl")
        )

        # ``context`` has an ``exception`` key. However, as it turns out, the value stored under
        # that key is always set to ``None`` if this callback is specified on the DAG (as in our
        # use case) instead of on the task (as is not our use case). Hence we don't even bother
        # retrieving it. It does mean, however, that we can't inform the contact point of the
        # nature of the failure that occurred.

        subject = f"Import mislukt voor dataset: '{dataset_id}'"
        body = f"""
            Beste {name},<br>
            <br>
            De import van de dataset '{dataset_id}' is mislukt.<br>
            <br>
            Tijdstip: '{execution_date}'<br>
            DAG: '{ti.dag_id}'<br>
            Taak: '{ti.task_id}' (operator: '{ti.operator}')<br>
            <br>
            Neemt contact op met het team Datadiensten op het Slack kanaal #datadiensten om de
            oorzaak te achterhalen.<br>
            <br>
            mvg,<br>
            Team Datadiensten<br>
        """
        logger.debug("Failure notification message:\n%s", body)
        if OTAP_ENVIRONMENT in eligible_environments:
            logger.info("Notifying contact point '%s' of import failure by email.", contact_point)
            send_email((contact_point.email,), subject, html_content=body)
        else:
            logger.info(
                "Not notifying contact point '%s' by email"
                " as we do not run in an eligible environment."
                " Current environment: '%s'."
                " Eligible environment(s): '%s'.",
                contact_point,
                OTAP_ENVIRONMENT,
                ", ".join(eligible_environments),
            )

    return _contact_point_on_failure_callback
