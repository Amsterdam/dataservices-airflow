from datetime import datetime
from operator import itemgetter
from typing import Any, Callable, Optional

from airflow.models.dag import DagModel, DagTag
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.settings import Session
from airflow.utils.state import State
from sqlalchemy import not_


class TriggerDynamicDagRunOperator(TriggerDagRunOperator):
    """Trigger the next dag in a series of DAGs with the same dag_id prefix.

    This operator makes it possible to trigger a series of DAGs with the same
    prefix in a serial fashion, head to tail. It is some kind of estafette run.

    Add this operator as the last one in the DAGs that are part of this estafette run.
    Usually these are dynamically generated dags.

    Make sure that the DAGs are not scheduled automatically. Only the first DAG in
    the series needs to be scheduled, it will trigger the next DAG in the series (the estafette).

    This trigger individual DAGs in the series manually, a configuration key `no_next_dag`
    with a truthy value can be provided that prevents the start of the estafette run.

    When the next DAGs in the chain is still running, it will not be triggered!
    """

    def __init__(
        self,
        dag_id_prefix: str = "",
        labels_group_getter: Optional[Callable[[str], set[str]]] = None,
        execution_date: Optional[datetime] = None,
        sort_key_getter: Optional[Callable[[str], Callable[[DagModel], Any]]] = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Initialize the TriggerDynamicDagRunOperator.

        Args:
            dag_id_prefix: Prefix that bundles a series of DAGs that need to run
                in a serialized (estafette) fashion.
            labels_group_getter: Callback function that gets a set of labels
                representing datasets that are grouped in the same schedule.
            execution_date: execution date for the dag (templated)
            sort_key_getter: Callback function that gets a callable.
                This callable is the `key` argument to the python built-in `sorted` function.
                Using an alternative `sort_key` can be convenient
                to apply alternative sorting on the dags in an estafette series.
        """
        super().__init__(
            trigger_dag_id="",
            execution_date=execution_date,
            *args,
            **kwargs,
        )
        assert len(dag_id_prefix) > 0, "A dag_id_prefix is mandatory to use this Operator."
        self.sort_key_getter = sort_key_getter
        self.dag_id_prefix = dag_id_prefix
        self.labels_group_getter = labels_group_getter

    def execute(self, context: dict[str, Any]) -> None:
        """Executes the Operator."""
        # Only now the `schedule` that is passed in via the `conf` from the Operator call
        # has been rendered.
        schedule = self.conf.get("schedule")
        self.log.info("schedule: %s", schedule)
        if self.sort_key_getter is None:
            sort_key = itemgetter("dag_id")
        else:
            sort_key = self.sort_key_getter(schedule)

        if self.labels_group_getter is None:
            labels_group = None
        else:
            labels_group = self.labels_group_getter(schedule)
        self.log.info("labels_group: %s", labels_group)

        dag_run = context["dag_run"]
        self.log.info("Conf: %r", dag_run.conf)

        # Do not trigger next dag when param no_next_dag is available
        # Due to bug in Airflow, dagrun misses `conf` attribute
        # when DAG is triggered from another DAG
        if dag_run is not None and (dag_run.conf or {}).get("no_next_dag"):
            self.log.info("Not starting next dag ('no_next_dag' in dag_run config)!")
            return
        current_dag_id = self.dag.dag_id
        self.log.info("Starting dag %s", current_dag_id)  # noqa: G004
        session = Session()
        query = (
            session.query(DagModel)
            .filter(not_(DagModel.is_paused))
            .filter(DagModel.dag_id.like(f"{self.dag_id_prefix}%"))
        )
        if labels_group is not None:
            query = query.filter(DagModel.tags.any(DagTag.name.in_(labels_group)))

        active_dag_ids = [d.dag_id for d in sorted(query, key=sort_key)]
        self.log.info("active_dag_ids: %r", active_dag_ids)

        try:
            current_dag_idx = active_dag_ids.index(current_dag_id)
        except ValueError:
            self.log.error("Current dag %s is not active.", current_dag_id)
            return

        try:
            self.trigger_dag_id = active_dag_ids[current_dag_idx + 1]
            self.log.info("Next dag to trigger %s", self.trigger_dag_id)
        except IndexError:
            self.log.info("Current dag %s is the last dag.", current_dag_id)
            return

        # If the next Dag is currently running, we do not trigger it
        if DagRun.find(dag_id=self.trigger_dag_id, state=State.RUNNING):
            self.log.info("Not starting next dag %s, it is still running.", self.trigger_dag_id)
            return

        super().execute(context)
