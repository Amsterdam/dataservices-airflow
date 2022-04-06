#!/bin/bash

export AIRFLOW__CORE__SQL_ALCHEMY_CONN=${AIRFLOW__CORE__SQL_ALCHEMY_CONN:-`echo $AIRFLOW_CONN_POSTGRES_DEFAULT | cut -d'?' -f 1`}
export AIRFLOW_CONN_POSTGRES_VSD={$AIRFLOW_CONN_POSTGRES_VSD:-$AIRFLOW__CORE__SQL_ALCHEMY_CONN}
airflow db init  # db init is not destructive, so can be re-run at startup
airflow db upgrade  # upgrade DB if needed
python scripts/mkvars.py

# creating an admin and regular users (nessacary when using RABC=True in the airflow.cnf)
airflow users create -r Admin -u admin -e admin@example.com -f admin -l admin -p ${AIRFLOW_USER_ADMIN_PASSWD:-admin}

# NOTE: Only needed for CloudVPS on Azure each datateam has its own Airflow instance.
# create custom users
airflow users create -r User -u dataservices -e dataservices@example.com -f dataservices -l dataservices -p ${AIRFLOW_USER_DATASERVICES_PASSWD:-dataservices}
airflow users create -r User -u team_ruimte -e team_ruimte@example.com -f team_ruimte -l team_ruimte -p ${AIRFLOW_USER_TEAM_RUIMTE_PASSWD:-team_ruimte}
airflow users create -r User -u team_benk -e team_benk@example.com -f team_benk -l team_benk -p ${AIRFLOW_USER_TEAM_BENK_PASSWD:-team_benk}

# NOTE: Only needed for CloudVPS. On Azure each datateam has its own Airflow instance.
# create custom roles. These need to be created before the DAG's are loaded and add DAG
# level permissions to these roles.
airflow roles create -v dataservices
airflow roles create -v team_ruimte
airflow roles create -v team_benk

# NOTE: Only needed for CloudVPS. On Azure each datateam has its own Airflow instance.
# create role-bindings for custom users and custom roles. It is needed to remove the User role
# from the users since it will allow to see/interact with all DAGs.
airflow users remove-role -r User -u team_benk
airflow users remove-role -r User -u team_ruimte
airflow users remove-role -r User -u dataservices
airflow users add-role -r team_benk -u team_benk
airflow users add-role -r team_ruimte -u team_ruimte
airflow users add-role -r dataservices -u dataservices

# Airflow does not support slack connection config through environment var
# So we (re-)create the slack connection on startup.
#
# WARNING: DEPRECATED way of creating Connections, please use Env variables.
airflow connections delete slack
airflow connections add slack --conn-host $SLACK_WEBHOOK_HOST \
    --conn-password "/$SLACK_WEBHOOK" --conn-type http

airflow connections delete geozet_conn_id
airflow connections add geozet_conn_id --conn-host http://geozet.koop.overheid.nl \
    --conn-type http

airflow connections delete hior_conn_id
airflow connections add hior_conn_id \
    --conn-host http://131f4363709c46b89a6ba5bc764b38b9.objectstore.eu \
    --conn-type http

airflow connections delete ams_maps_conn_id
airflow connections add ams_maps_conn_id \
    --conn-host https://maps.amsterdam.nl \
    --conn-type http

airflow connections delete fietspaaltjes_conn_id
airflow connections add fietspaaltjes_conn_id \
    --conn-host https://cdn.endora.nl \
    --conn-type http

airflow connections delete api_data_amsterdam_conn_id
airflow connections add api_data_amsterdam_conn_id \
    --conn-host  https://api.data.amsterdam.nl  \
    --conn-type http

airflow connections delete schemas_data_amsterdam_conn_id
airflow connections add schemas_data_amsterdam_conn_id \
    --conn-host  https://schemas.data.amsterdam.nl \
    --conn-type http

airflow connections delete airflow_home_conn_id
airflow connections add airflow_home_conn_id \
    --conn-host  /usr/local/airflow/ \
    --conn-type http

airflow connections delete verlichting_conn_id
airflow connections add verlichting_conn_id \
    --conn-host https://asd.techtek.eu \
    --conn-type http

airflow connections delete taxi_waarnemingen_conn_id
airflow connections add taxi_waarnemingen_conn_id \
    --conn-host https://waarnemingen.amsterdam.nl \
    --conn-type http

airflow connections delete taxi_waarnemingen_acc_conn_id
airflow connections add taxi_waarnemingen_acc_conn_id \
    --conn-host https://acc.waarnemingen.amsterdam.nl \
    --conn-type http

airflow connections delete rdw_conn_id
airflow connections add rdw_conn_id \
    --conn-host https://opendata.rdw.nl/resource \
    --conn-type http

# airflow connections delete gob_graphql
# airflow connections add gob_graphql \
#     --conn_host https://acc.api.data.amsterdam.nl \
#     --conn_type http

# airflow variables -i vars/vars.json &
# airflow scheduler &
# airflow webserver
airflow variables import vars/vars.json

# Check all dags by running them as python modules.
# This is whait the Airflow dagbag is also doing.
# If one of the DAGs fails, the check script exits
# with a non-zero exit state.
# A fully configured Airflow instance is needed to be able
# to run the checkscript.
# Make sure that the the containing shell script (run.sh)
# stops on errors (set -e).
#python scripts/checkdags.py || exit


# During development it is not always desirable to run the
# Airflow webserver and scheduler. When the NO_AUTOSTART_AIRFLOW
# variable is defined (or it is zero-length),
# airflow will not start automatically.
# If needed, it can be started manually using supervisor from the container.

if [[ ! -z $NO_AUTOSTART_AIRFLOW ]]; then
    sleep infinity
else
    /usr/local/bin/supervisord --config /usr/local/airflow/etc/supervisord.conf
fi
