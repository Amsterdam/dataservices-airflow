import pathlib
from airflow.operators.bash_operator import BashOperator

# from airflow.operators.python_operator import PythonOperator
from environs import Env

from airflow import DAG
from common import default_args, SHARED_DIR

env = Env()


AIRFLOW_HOME = env("AIRFLOW_HOME")
DAG_SYNC_PATH = env("DAG_SYNC_PATH", pathlib.Path(AIRFLOW_HOME) / "dags")


def doit():
    pass


with DAG(
    "update_dags", default_args=default_args, schedule_interval="0 * * * *"
) as dag:

    github_url = "https://github.com/Amsterdam/dataservices-airflow/archive/master.zip"

    fetch_dags_from_github = BashOperator(
        task_id="fetch_dags_from_github",
        bash_command=f"wget {github_url} -O {SHARED_DIR}/repo.zip",
    )

    extract_zip = BashOperator(
        task_id="extract_zip", bash_command=f"unzip -o {SHARED_DIR}/repo.zip -d {SHARED_DIR}"
    )

    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {DAG_SYNC_PATH}")

    move_dags = BashOperator(
        task_id="move_dags",
        bash_command=f"rsync -av {SHARED_DIR}/dataservices-airflow-master/src/dags/ {DAG_SYNC_PATH}",
    )

    # show = PythonOperator(task_id="show", python_callable=doit)

fetch_dags_from_github >> extract_zip >> mkdir >> move_dags
