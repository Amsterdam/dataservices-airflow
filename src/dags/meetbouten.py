from airflow import DAG
from http_gob_operator import HttpGobOperator
from common import default_args


graphql_query = """{"query":"{  meetboutenMeetbouten($active: false, publiceerbaar: true) {    edges {      node {        identificatie         nabijNummeraanduiding {          edges {            node {              identificatie               volgnummer             }          }        }        locatie         status         vervaldatum         merk         xCoordinaatMuurvlak         yCoordinaatMuurvlak         windrichting         ligtInBouwblok {          edges {            node {              identificatie               volgnummer             }          }        }        ligtInBuurt {          edges {            node {              identificatie               volgnummer             }          }        }        ligtInStadsdeel {          edges {            node {              identificatie               volgnummer             }          }        }        geometrie         publiceerbaar       }    }  }}"}"""

dag_id = "meetbouten"
owner = "gob"

with DAG(dag_id, default_args={**default_args, **{"owner": owner}}) as dag:

    http_load_task = HttpGobOperator(
        task_id="http_load_task",
        endpoint="gob/graphql/streaming/",
        schema="meetbouten_meetbouten",
        id_fields="identificatie,volgnummer",
        geojson_field="geometrie",
        graphql_query=graphql_query,
        http_conn_id="gob_graphql",
    )
