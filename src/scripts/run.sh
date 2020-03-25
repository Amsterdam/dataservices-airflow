#!/bin/bash

# XXX Should we run airflow as root ?
echo "$POSTGRES_HOST:$POSTGRES_PORT:$POSTGRES_DB:$POSTGRES_USER:$POSTGRES_PASSWORD" > /root/.pgpass
chmod 500 /root/.pgpass

python mkvars.py
airflow variables -i vars/vars.json & 
airflow scheduler & 
airflow webserver
