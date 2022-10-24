FROM amsterdam/python:3.9-buster AS builder
MAINTAINER datapunt@amsterdam.nl

RUN apt-get update \
 && apt-get install --no-install-recommends -y rsync libaio1 supervisor \
 curl libcurl4 postgresql-server-dev-all postgresql-common zip libdigest-sha3-perl \
 g++ unixodbc unixodbc-dev
COPY requirements* ./
ARG PIP_REQUIREMENTS=requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r $PIP_REQUIREMENTS
WORKDIR /tmp
RUN wget -nv https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basiclite-linux.x64-19.6.0.0.0dbru.zip
RUN unzip instantclient-basiclite-linux.x64-19.6.0.0.0dbru.zip
RUN cp -r  instantclient_19_6/* /usr/local/lib
# Oracle DB connections to use encrypted connection
RUN cd /usr/local/lib && echo SQLNET.ENCRYPTION_CLIENT=REQUIRED > ./network/admin/sqlnet.ora

# Install sqlite3 from source, the default version for this Ubuntu-based image is too old
RUN wget -c https://www.sqlite.org/2021/sqlite-autoconf-3360000.tar.gz
# NB. input to sha3sum -c needs to be separated with two spaces!
RUN bash -c "sha3sum -a 256 -c <(echo 'fdc699685a20284cb72efe3e3ddfac58e94d8ffd5b229a8235d49265aa776678  sqlite-autoconf-3360000.tar.gz')"
RUN tar xzf sqlite-autoconf-3360000.tar.gz
WORKDIR /tmp/sqlite-autoconf-3360000
RUN CFLAGS="-DSQLITE_ENABLE_COLUMN_METADATA=1" ./configure --disable-static --disable-static-shell && make install distclean

# Start runtime image,
FROM amsterdam/python:3.9-slim-buster

# Copy python build artifacts from builder image
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /usr/local/lib/ /usr/local/lib/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libaio* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcurl.so.4 /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/local/lib/python3.9/site-packages/ /usr/local/lib/python3.9/site-packages/
COPY --from=builder /usr/bin/ /usr/bin/
COPY --from=builder /usr/lib/postgresql/. /usr/lib/postgresql/
COPY --from=builder /usr/share/postgresql/. /usr/share/postgresql/

RUN ldconfig

# Copy patch binary, needed to patch airflow bug
COPY --from=builder /usr/bin/patch /usr/bin/patch
COPY --from=builder /usr/bin/rsync /usr/bin/rsync

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

COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
COPY scripts/airflow_high_level_role_perms.sh /airflow_high_level_role_perms.sh
COPY scripts/run.sh /run.sh
COPY scripts/mkvars.py ${AIRFLOW_USER_HOME}/scripts/mkvars.py
COPY scripts/checkdags.py ${AIRFLOW_USER_HOME}/scripts/checkdags.py
RUN chmod +x /docker-entrypoint.sh /run.sh /airflow_high_level_role_perms.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
RUN mkdir ${AIRFLOW_USER_HOME}/etc
COPY config/supervisord.conf ${AIRFLOW_USER_HOME}/etc/supervisord.conf
COPY data/ ${AIRFLOW_USER_HOME}/data/

# copy dags and plugins here
COPY structured_logging ${AIRFLOW_USER_HOME}/structured_logging/
COPY dags ${AIRFLOW_USER_HOME}/dags/
COPY plugins ${AIRFLOW_USER_HOME}/plugins/
COPY vars ${AIRFLOW_USER_HOME}/vars/
COPY vsd ${AIRFLOW_USER_HOME}/vsd

# add the AIRFLOW_USER_HOME to PythonPath so structured_logging
# for instance can be located and used as a reference.
ENV PYTHONPATH=$PYTHONPATH:$AIRFLOW_USER_HOME

# temporary path adjustment for vsd biz and meldingen specific (TODO: need to convert it to standard dags)
ENV PYTHONPATH=$PYTHONPATH:$AIRFLOW_USER_HOME/dags:$AIRFLOW_USER_HOME/vsd

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]
# CMD ["sleep", "infinity"]
# Do not use -D to run processes daemonized, does not work!
# The & puts scheduler in background (not &&)
# CMD ["sh", "-c", "airflow variables -i vars/vars.json & airflow scheduler & airflow webserver"]
#
# Do an DB upgrade first and only for bumbing Airflow
#
CMD ["sh", "-c", "airflow db upgrade"]
#CMD ["sh", "-c", "/airflow_high_level_role_perms.sh & /run.sh"]
