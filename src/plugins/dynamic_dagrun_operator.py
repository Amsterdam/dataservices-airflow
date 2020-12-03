from sqlalchemy import not_
from airflow.settings import Session
from airflow.models.dag import DagModel
from airflow.utils.decorators import apply_defaults
from airflow.operators.dagrun_operator import TriggerDagRunOperator


class TriggerDynamicDagRunOperator(TriggerDagRunOperator):
    pass

    @apply_defaults
    def __init__(
        self,
        dag_id_prefix="",
        python_callable=None,
        execution_date=None,
        *args,
        **kwargs,
    ):
        super().__init__(
            trigger_dag_id=None,
            python_callable=None,
            execution_date=None,
            *args,
            **kwargs,
        )
        self.dag_id_prefix = dag_id_prefix

    def execute(self, context):
        # Do not trigger next dag when param no_next_dag is available
        # Due to bug in Airflow, dagrun misses 'conf' attribute
        # when DAG is triggered from another DAG
        dag_run = context["dag_run"]
        if dag_run is not None and getattr(dag_run, "conf", {}).get("no_next_dag"):
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

        super().execute(context)
