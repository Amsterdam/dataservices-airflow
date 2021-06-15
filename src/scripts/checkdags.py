"""Python script to check for broken DAGs.

This scripts is running during the initialization and startup of Airflow during deployment.
When DAGs are broken, this results in Airflow not being able to start.
So, broken dags are detected during deployment.
"""

import subprocess
from pathlib import Path

dagsfolder = Path(__file__).parent / ".." / "dags"

broken_dags = []
for dagfile in dagsfolder.glob("*.py"):
    proc = subprocess.run(["python", dagfile])
    if proc.returncode != 0:
        broken_dags.append(dagfile.name)

if broken_dags:
    raise Exception("These dags are broken: {}".format(", ".join(broken_dags)))
