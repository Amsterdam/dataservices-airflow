from datetime import datetime
from typing import Any, Dict, Optional

from airflow.models.dag import DagModel, DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.settings import Session
from airflow.utils.decorators import apply_defaults
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

    @apply_defaults
    def __init__(
        self,
        dag_id_prefix: str = "",
        execution_date: Optional[datetime] = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Initialize the TriggerDynamicDagRunOperator.

        Args:
            dag_id_prefix: Prefix that bundles a series of DAGs that need to run
                in a serialized (estafette) fashion.
            execution_date: execution date for the dag (templated)
        """
        super().__init__(
            trigger_dag_id=None,
            execution_date=execution_date,
            *args,
            **kwargs,
        )
        assert len(dag_id_prefix) > 0, "A dag_id_prefix is mandatory to use this Operator."
        self.dag_id_prefix = dag_id_prefix

    def execute(self, context: Dict[str, Any]) -> None:
        # Do not trigger next dag when param no_next_dag is available
        # Due to bug in Airflow, dagrun misses 'conf' attribute
        # when DAG is triggered from another DAG
        dag_run = context["dag_run"]
        if dag_run is not None and (getattr(dag_run, "conf") or {}).get("no_next_dag"):
            self.log.info("Not starting next dag ('no_next_dag' in dag_run config)!")
            return
        current_dag_id = self.dag.dag_id
        self.log.info("Starting dag %s", current_dag_id)
        session = Session()
        active_dag_ids = [
            d.dag_id
            for d in session.query(DagModel)
            .filter(not_(DagModel.is_paused))
            .filter(DagModel.dag_id.like(f"{self.dag_id_prefix}%"))
            .order_by("dag_id")
        ]

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
