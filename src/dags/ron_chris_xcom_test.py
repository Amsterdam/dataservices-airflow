import json
import os
from datetime import timedelta
from typing import Final, Optional

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# below the modules as located at github.com/Amsterdam/dataservices-airflow
from common import default_args

# from contact_point.callbacks import get_contact_point_on_failure_callback


# environment where this DAG is loaded from
AZURE_OTAP_ENVIRONMENT: Optional[str] = os.getenv("AZURE_OTAP_ENVIRONMENT")

# [registry]/[imagename]:[tag]
CONTAINER_IMAGE: Optional[str] = "crdavebbn1ontweu01.azurecr.io/airflow-benk-iburgerzaken:test" if 'ont' in AZURE_OTAP_ENVIRONMENT else "alpine:latest"

# Command that you want to run on container start
# COMMAND_TO_EXECUTE: list = ["sh", "-c", "tail", "-f", "/dev/null"]
COMMAND_TO_EXECUTE: list = [
    "sh",
    "-c",
    "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json && tail -f /dev/null",
]
COMMAND_TO_EXECUTE: list = [
    "sh",
    "-c",
    "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json",
]
DAG_ID: Final = "test_xcom_always_running"
DATATEAM_OWNER: Final = "davebbn1"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = "davebbn1work"


with DAG(
    DAG_ID,
    description="test-dag",
    default_args=default_args,
    template_searchpath=["/"],
    catchup=False,
) as dag:

    # Be ware! The resource limits below are based on a F8_V2 machine (8 cpu, 16GB mem)
    # and sould be ajusted accordinly if changed.

    # 2. Excuting containers
    # The KubernetesPodOperator enables you to run containerized workloads as
    # pods on Kubernetes from your DAG.
    # Each task will be executed by a dedicated pod.
    test_xcom_operator = KubernetesPodOperator(
            task_id='test_xcom_step',
            name="test_xcom_step",
            do_xcom_push=True,
            namespace=AKS_NAMESPACE,
            image=CONTAINER_IMAGE,
            cmds=COMMAND_TO_EXECUTE,
            # cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
            labels=DAG_LABEL,
            # Determines when to pull a fresh image, if 'IfNotPresent' will cause
            # the Kubelet to skip pulling an image if it already exists. If you
            # want to always pull a new image, set it to 'Always'.
            image_pull_policy="Always",
            # Known issue in the KubernetesPodOperator
            # https://stackoverflow.com/questions/55176707/airflow-worker-connection-broken-incompleteread0-bytes-read
            # set get_logs to false
            # If true, logs stdout output of container. Defaults to True.
            get_logs=True,
            in_cluster=True,  # if true uses our service account token as aviable in Airflow on K8
            is_delete_operator_pod=True,  # if true delete pod when pod reaches its final state.
            log_events_on_failure=True,  # if true log the pod’s events if a failure occurs
            hostnetwork=True,  # If True enable host networking on the pod. Be ware, this value must be
            # set to true if you want to make use of the pod-identity facilities like managed identity.
            reattach_on_restart=True,
            dag=dag,
            # Timeout to start up the Pod, default is 120.
            startup_timeout_seconds=3600,
            # to prevent tasks becoming marked as failed when taking longer
            # and deleting them if staling
            execution_timeout=timedelta(hours=4),
            # Select a specific nodepool to use. Could also be specified by nodeAffinity.
            node_selector={"nodetype": AKS_NODE_POOL},
            # List of Volume objects to pass to the Pod.
            volumes=[],
            # List of VolumeMount objects to pass to the Pod.
            volume_mounts=[],
        )
        # get containers that are the first (marked with `_1` as a posfix) that will also
        # add the header to the .csv file to be created

    pod_task_xcom_result = BashOperator(
        bash_command="echo ******************************* \"{{ task_instance.xcom_pull('test_xcom_step') }}\"",
        task_id="pod_task_xcom_result",
    )

    # test_xcom_push = KubernetesPodOperator(
    #     namespace=AKS_NAMESPACE,
    #     image=CONTAINER_IMAGE,
    #     # cmds=COMMAND_TO_EXECUTE,
    #     cmds=["bash", "-cx"],
    #     arguments=[
    #         "mkdir -p /airflow/xcom/; echo {} > /airflow/xcom/return.json".format(
    #             '{"foo":"bar","buzz":"2"}'
    #         )
    #     ],
    #     # labels={"foo": "bar"},
    #     name="test_xcom_step",
    #     task_id="test_xcom_step",
    #     random_name_suffix=True,
    #     node_selector={"nodetype": AKS_NODE_POOL},
    #     do_xcom_push=True,
    # )


# FLOW
(
    test_xcom_operator
    >> pod_task_xcom_result
    # test_xcom_operator >> pod_task_xcom_result
)
