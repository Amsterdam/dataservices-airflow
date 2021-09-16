from typing import Any


class XComAttrAssignerMixin:
    def _assign(self, context: dict[str, Any]) -> None:

        # If an xcom task_ids is provided, use it to do the assigments
        if self.xcom_task_ids is not None:  # type: ignore
            xcom_info = context["ti"].xcom_pull(
                task_ids=self.xcom_task_ids, key=self.xcom_key  # type: ignore
            )
            self.xcom_attr_assigner(self, xcom_info)  # type: ignore
