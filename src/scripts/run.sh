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

airflow connections --delete --conn_id hior_conn_id
airflow connections --add  --conn_id hior_conn_id \
    --conn_host http://131f4363709c46b89a6ba5bc764b38b9.objectstore.eu \
    --conn_type http

airflow connections --delete --conn_id ams_maps_conn_id
airflow connections --add  --conn_id ams_maps_conn_id \
    --conn_host https://maps.amsterdam.nl \
    --conn_type http

airflow connections --delete --conn_id fietspaaltjes_conn_id
airflow connections --add  --conn_id fietspaaltjes_conn_id \
    --conn_host https://cdn.endora.nl \
    --conn_type http
# airflow variables -i vars/vars.json & 
# airflow scheduler & 
# airflow webserver
airflow variables -i vars/vars.json
/usr/local/bin/supervisord --config /usr/local/airflow/etc/supervisord.conf
