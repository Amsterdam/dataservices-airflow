#!/bin/bash

# XXX Should we run airflow as root ?
echo "$POSTGRES_HOST:$POSTGRES_PORT:$POSTGRES_DB:$POSTGRES_USER:$POSTGRES_PASSWORD" > /root/.pgpass
chmod 500 /root/.pgpass

python scripts/mkvars.py
# Airflow does not support slack connection config through environment var
# So we (re-)create the slack connection on startup.
airflow connections --delete --conn_id slack
airflow connections --add  --conn_id slack --conn_host $SLACK_WEBHOOK_HOST \
    --conn_password "/$SLACK_WEBHOOK" --conn_type http
airflow connections --delete --conn_id geozet_conn_id
airflow connections --add  --conn_id geozet_conn_id --conn_host http://geozet.koop.overheid.nl \
    --conn_type http
airflow variables -i vars/vars.json & 
airflow scheduler & 
airflow webserver
