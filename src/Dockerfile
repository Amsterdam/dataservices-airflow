# Airflow depends on cattrs~=0.9, which makes it <= python3.7
FROM amsterdam/python:3.7.6-buster AS builder
MAINTAINER datapunt@amsterdam.nl

# Seems that pg_comparator installs full postgresql, that's not what we want
# RUN apt-get update && apt-get install -y postgresql-comparator
RUN apt-get update \
 && apt-get install --no-install-recommends -y rsync libaio1 supervisor \
 curl libcurl4 libdbd-pg-perl postgresql-server-dev-all postgresql-common
COPY requirements* ./
ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS
WORKDIR /tmp
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basiclite-linux.x64-19.6.0.0.0dbru.zip
RUN unzip instantclient-basiclite-linux.x64-19.6.0.0.0dbru.zip
RUN cp -r  instantclient_19_6/* /usr/local/lib

# Add pg_comparator from sources, installable package has way too much dependencies
COPY vendor/pg_comparator-2.3.1.tgz /tmp
WORKDIR /tmp
RUN tar xzf pg_comparator-2.3.1.tgz
WORKDIR /tmp/pg_comparator-2.3.1
RUN make pgsql_install

# Start runtime image,
FROM amsterdam/python:3.7-slim-buster


# Copy python build artifacts from builder image
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /usr/local/lib/ /usr/local/lib/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libaio* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/local/lib/python3.7/site-packages/ /usr/local/lib/python3.7/site-packages/
COPY --from=builder /usr/bin/ /usr/bin/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcurl* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/perl5/. /usr/lib/x86_64-linux-gnu/perl5/
COPY --from=builder /usr/lib/x86_64-linux-gnu/perl/. /usr/lib/x86_64-linux-gnu/perl/
COPY --from=builder /usr/lib/postgresql/. /usr/lib/postgresql/
COPY --from=builder /usr/share/postgresql/. /usr/share/postgresql/
COPY --from=builder /usr/share/perl/. /usr/share/perl/
RUN ldconfig

# Copy patch binary, needed to patch airflow bug
COPY --from=builder /usr/bin/patch /usr/bin/patch
COPY --from=builder /usr/bin/rsync /usr/bin/rsync
COPY --from=builder /usr/lib/postgresql/11/bin/pg_comparator /usr/bin/pg_comparator

# Installing postgresql-server-dev-all introduces the lsb-release package
# However, this is crashing (because of unconfigured python2 environment)
# pip checks for the lsb_release script and uses it if available.
# So, we need to remove the lsb_release script to have a working pip inside our container.
# A working pip is needed to switch to a development version of the schematools package.
RUN rm /usr/bin/lsb_release


# Disable an expired root certificate (temporary solution)
RUN sed -i '/^mozilla\/AddTrust_External_Root.crt/c\!mozilla\/AddTrust_External_Root.crt' /etc/ca-certificates.conf
RUN update-ca-certificates

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Fix problem with env vars that prevents useage of 'airflow shell'
COPY patches/cli.py.patch /tmp/cli.py.patch
RUN patch -d/ -p0 < /tmp/cli.py.patch

COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
COPY scripts/run.sh /run.sh
COPY scripts/mkvars.py ${AIRFLOW_USER_HOME}/scripts/mkvars.py
COPY scripts/mkuser.py ${AIRFLOW_USER_HOME}/scripts/mkuser.py
RUN chmod +x /docker-entrypoint.sh /run.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
RUN mkdir ${AIRFLOW_USER_HOME}/etc
COPY config/supervisord.conf ${AIRFLOW_USER_HOME}/etc/supervisord.conf
COPY data/ ${AIRFLOW_USER_HOME}/data/

# copy dags and plugins here
COPY dags ${AIRFLOW_USER_HOME}/dags/
COPY plugins ${AIRFLOW_USER_HOME}/plugins/
COPY vars ${AIRFLOW_USER_HOME}/vars/
COPY vsd ${AIRFLOW_USER_HOME}/vsd

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]
# CMD ["sleep", "infinity"]
# Do not use -D to run processes daemonized, does not work!
# The & puts scheduler in background (not &&)
# CMD ["sh", "-c", "airflow variables -i vars/vars.json & airflow scheduler & airflow webserver"]
CMD /run.sh
