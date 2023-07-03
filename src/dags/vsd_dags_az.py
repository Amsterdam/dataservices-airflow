import os
import pathlib
import sys
from typing import Final
from common.db import DatabaseEngine
from airflow import DAG
from bash_env_operator import BashEnvOperator
from common import default_args as common_default_args

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_BOR"]

vsd_dir = pathlib.Path(__file__).resolve().parents[1] / "vsd"

SCHEDULE_INTERVAL_EXCEPTIONS: Final = {"grootstedelijke_projecten": "@monthly"}


def fetch_env_vars(*args):
    pg_env_vars = DatabaseEngine(context=args).fetch_pg_env_vars()
    return {
        **pg_env_vars,
        "PYTHONPATH": ":".join([str(vsd_dir)] + sys.path),
        **os.environ,
    }


def create_vsd_dag(vsd_id, default_args):

    script_dir = vsd_dir / vsd_id / "import"
    shared_dir = vsd_dir / "shared"
    data_dir = vsd_dir / vsd_id / "data"

    kwargs = {"default_args": default_args, "template_searchpath": ["/"]}
    schedule_interval = SCHEDULE_INTERVAL_EXCEPTIONS.get(vsd_id)
    if schedule_interval:
        kwargs["schedule_interval"] = schedule_interval

    dag = DAG(f"vsd_{vsd_id}_az", **kwargs)

    with dag:
        BashEnvOperator(
            task_id=f"{vsd_id}_task",
            bash_command=str(script_dir / "import.sh"),
            env={
                "SCRIPT_DIR": script_dir,
                "SHARED_DIR": shared_dir,
                "DATA_DIR": data_dir,
            },
            env_expander= fetch_env_vars,
        )

    return dag


for sub_path in vsd_dir.iterdir():
    if sub_path.is_dir():
        vsd_id = sub_path.name
        if vsd_id == "shared":
            continue
        globals()[f"vsd_{vsd_id}"] = create_vsd_dag(vsd_id, common_default_args)
