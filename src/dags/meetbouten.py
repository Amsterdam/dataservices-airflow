import json
from airflow import DAG
from postgres_rename_operator import PostgresTableRenameOperator
from airflow.operators.postgres_operator import PostgresOperator
from http_gob_operator import HttpGobOperator
from common import default_args


DROP_TMPL = """
    {% for tablename in params.tablenames %}
    DROP TABLE IF EXISTS {{ tablename }} CASCADE;
    {% endfor %}
"""


gql = """{
    meetboutenMeetbouten($active: false, publiceerbaar: true) {
      edges {
        node {
          identificatie
          nabijNummeraanduiding {
            edges {
              node {
                identificatie
                volgnummer
              }
            }
          }
          locatie
          status
          vervaldatum
          merk
          xCoordinaatMuurvlak
          yCoordinaatMuurvlak
          windrichting
          ligtInBouwblok {
            edges {
              node {
                identificatie
                volgnummer
              }
            }
          }
          ligtInBuurt {
            edges {
              node {
                identificatie
                volgnummer
              }
            }
          }
          ligtInStadsdeel {
            edges {
              node {
                identificatie
                volgnummer
              }
            }
          }
          geometrie
          publiceerbaar
        }
      }
    }
}
"""
graphql_query = {"query": gql}
dag_id = "meetbouten"
owner = "gob"


with DAG(dag_id, default_args={"owner": owner, **default_args}) as dag:

    drop_old_tmp_tables = PostgresOperator(
        task_id="drop_old_tmp_tables",
        sql=DROP_TMPL,
        params=dict(tablenames=["meetbouten_meetbouten_new"]),
    )

    data_load_task = HttpGobOperator(
        task_id="data_load_task",
        endpoint="gob/graphql/streaming/",
        dataset="meetbouten",
        schema="meetbouten",
        id_fields="identificatie,volgnummer",
        geojson_field="geometrie",
        graphql_query=json.dumps(graphql_query),
        http_conn_id="gob_graphql",
    )

    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table",
        old_table_name="meetbouten_meetbouten_new",
        new_table_name="meetbouten_meetbouten",
    )

drop_old_tmp_tables >> data_load_task >> rename_table
