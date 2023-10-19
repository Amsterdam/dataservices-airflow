import logging

from airflow.utils.context import Context

logger = logging.getLogger(__name__)


def _pick_branch(**context: Context) -> str:
    """Select next step to be executed based on existence of duplicate records.

    Args:
        context: the context given in Airflow, so it can retrieve xcom pushes from previous steps.

    Returns:
        string containing name of desired step to execute next.
    """
    ti = context["ti"]  # get task instance object to do an xcom_pull.
    duplicate_ids = [element for element in ti.xcom_pull(task_ids="load_data")]
    if duplicate_ids:
        for id_ in duplicate_ids:
            logger.info("Duplicate found!: %d", id_)
        return "duplicates_found"

    logger.info("No duplicate found!: Resuming happy flow")
    return "no_duplicates_found"
