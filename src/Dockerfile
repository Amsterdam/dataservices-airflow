# Airflow depends on cattrs~=0.9, which makes it <= python3.7 
FROM amsterdam/python:3.7-buster AS builder
MAINTAINER datapunt@amsterdam.nl

# Seems that pg_comparator installs full postgresql, that's not what we want
# RUN apt-get update && apt-get install -y postgresql-comparator
RUN apt-get update \
 && apt-get dist-upgrade -y \
 && apt-get autoremove -y \
 && apt-get install --no-install-recommends -y build-essential
COPY requirements* ./
ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS

# Start runtime image,
FROM amsterdam/python:3.7-slim-buster


# Copy python build artifacts from builder image
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /usr/local/lib/python3.7/site-packages/ /usr/local/lib/python3.7/site-packages/

# Copy patch binary, needed to patch airflow bug
COPY --from=builder /usr/bin/patch /usr/bin/patch

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Fix problem with env vars that prevents useage of 'airflow shell'
COPY patches/cli.py.patch /tmp/cli.py.patch
RUN patch -d/ -p0 < /tmp/cli.py.patch

COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
COPY scripts/run.sh /run.sh
COPY scripts/mkuser.py ${AIRFLOW_USER_HOME}/mkuser.py
RUN chmod +x /docker-entrypoint.sh /run.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

# copy dags and plugins here
COPY dags ${AIRFLOW_USER_HOME}/dags/
COPY plugins ${AIRFLOW_USER_HOME}/plugins/
COPY vars ${AIRFLOW_USER_HOME}/vars/

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]
# CMD ["sleep", "infinity"]
# Do not use -D to run processes daemonized, does not work!
# The & puts scheduler in background (not &&)
# CMD ["sh", "-c", "airflow variables -i vars/vars.json & airflow scheduler & airflow webserver"]
CMD /run.sh
