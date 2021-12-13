import os
from datetime import timedelta
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    slack_webhook_token,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_brp_iburgerzaken import setup_containers

# SETUP the use of mounting secrets instead of using environment variables (the latter
# being prone for unwanted secret exposure if someone can describe the pod)
# https://github.com/VBhojawala/airflow/blob/k8s-docs/docs/apache-airflow-providers-cncf-kubernetes/operators.rst#mounting-secrets-as-volume
# The instantiating of Secret needs a type (volume or env), a location path and the object secret name as stated in the values.yml in HELM.
secret_file = Secret("volume", "/TMP/secrets", "db-iburgerzaken-db-uid-pwd")
# secret_file2 = Secret('volume', '/TMP/secrets', 'db-iburgerzaken-server')
# secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')

# SETUP GENERAL DAG RELATED VARIABLES
CONTAINER_IMAGE: Final = "crdaveontweu01.azurecr.io/airflow-workload-iburgerzaken:latest"  # [registry]/[imagename]:[tag]
COMMAND_TO_EXECUTE: list = ["python"]  # Command that you want to run on container start
COMMAND_ARGS: list = [
    "/scripts/data_processor.py"
]  # Command arguments that will be used with command to execute on start
DATATEAM_OWNER: Final = "datateam_basis_kernregistraties"
DAG_ID: Final = "brp_iburgerzaken"
DAG_LABEL: Final = {"team_name": DATATEAM_OWNER}
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
AKS_NAMESPACE: Final = os.getenv("AIRFLOW__KUBERNETES__NAMESPACE")
AKS_NODE_POOL: Final = [
    "benkbbn1ibur"
]

# SETUP CONTAINER SPECIFIC ENV VARS
CONTAINERS_TO_RUN_IN_PARALLEL: dict[str, dict] = setup_containers()

with DAG(
    DAG_ID,
    description="""Running a containerized workload that collects BRP (basis registratie personen)
                    data from source iBurgerzaken.""",
    default_args=default_args,
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
    catchup=False,
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Excuting containers
    # The KubernetesPodOperator enables you to run containerized workloads as pods on Kubernetes from your DAG.
    procesdata = [
        KubernetesPodOperator(
            task_id=container_name,
            namespace=AKS_NAMESPACE,
            image=CONTAINER_IMAGE,
            cmds=COMMAND_TO_EXECUTE,
            arguments=COMMAND_ARGS,
            labels=DAG_LABEL,
            env_vars=container_vars,
            name=DAG_ID,
            image_pull_policy="Always",
            # Known issue in the KubernetesPodOperator
            # https://stackoverflow.com/questions/55176707/airflow-worker-connection-broken-incompleteread0-bytes-read
            # set get_logs to false
            get_logs=False,
            in_cluster=True,  # if true uses our service account token as aviable in Airflow on K8
            is_delete_operator_pod=True,  # if true delete pod when pod reaches its final state.
            log_events_on_failure=True,  # if true log the pod’s events if a failure occurs
            hostnetwork=False,  # If True enable host networking on the pod.
            secrets=[
                secret_file,
            ],  # Uses a mount to get to secret
            reattach_on_restart=True,
            dag=dag,
            startup_timeout_seconds=3600,
            # execution_timeout=timedelta(
            #     hours=4
            # ),  # to prevent taks becoming marked as failed when taking longer
            # Resource specifications for Pod, this will allow you to set both cpu
            # and memory limits and requirements.
            # resources={'limit_memory': "250M", 'limit_cpu': "100m"},
            node_selector={'nodetype': AKS_NODE_POOL},
            resources={
                'request_memory': '2Gi',
                'request_cpu': 2,
                'limit_memory': '4Gi',
                'limit_cpu': 8},
            pool=AKS_NODE_POOL,
            # affinity={
            #     "nodeAffinity": {
            #         # requiredDuringSchedulingIgnoredDuringExecution means in order
            #         # for a pod to be scheduled on a node, the node must have the
            #         # specified labels. However, if labels on a node change at
            #         # runtime such that the affinity rules on a pod are no longer
            #         # met, the pod will still continue to run on the node.
            #         "requiredDuringSchedulingIgnoredDuringExecution": {
            #             "nodeSelectorTerms": [
            #                 {
            #                     "matchExpressions": [
            #                         {
            #                             # When nodepools are created by TerraForm,
            #                             # the nodes inside of that nodepool are
            #                             # automatically assigned the label
            #                             # 'nodetype' with the value of
            #                             # the nodepool's name.
            #                             "key": "nodetype",
            #                             "operator": "In",
            #                             # The label key's value that pods can be scheduled
            #                             # on.
            #                             "values": AKS_NODE_POOL,
            #                         }
            #                     ]
            #                 }
            #             ]
            #         }
            #     }
            # },
        )
        for container_name, container_vars in CONTAINERS_TO_RUN_IN_PARALLEL.items()
    ]

# FLOW
slack_at_start >> procesdata
