# Airflow depends on cattrs~=0.9, which makes it <= python3.7 
FROM amsterdam/python:3.7-buster AS builder
MAINTAINER datapunt@amsterdam.nl

# Seems that pg_comparator installs full postgresql, that's not what we want
# RUN apt-get update && apt-get install -y postgresql-comparator

COPY requirements* ./
ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS

# Start runtime image,
FROM amsterdam/python:3.7-slim-buster

# Copy python build artifacts from builder image
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /usr/local/lib/python3.7/site-packages/ /usr/local/lib/python3.7/site-packages/

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

# copy dags and plugins here

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]
# CMD ["sleep", "infinity"]
# Do not use -D to run processes daemonized, does not work!
# The & puts scheduler in background (not &&)
CMD ["sh", "-c", "airflow scheduler & airflow webserver"]
