from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow import DAG
from airflow.models import Variable


from common import pg_params, default_args
from importscripts.import_hoofdroutes import import_hoofdroutes


dag_id = "hoofdroutes"
config = Variable.get("dag_config", deserialize_json=True)
generic_config = config["vsd"]["generic"]
dag_config = config["vsd"][dag_id]

with DAG(dag_id, default_args=default_args,) as dag:

    tmp_dir = f"/tmp/{dag_id}"
    tmp_file_prefix = f"{tmp_dir}/{dag_id}"
    sql_table_rename = generic_config["sql_table_rename"]

    import_routes = PythonOperator(
        task_id="import_routes",
        python_callable=import_hoofdroutes,
        op_args=[f"{tmp_file_prefix}.json"],
    )

    extract_geojson = BashOperator(
        task_id="extract_geojson",
        bash_command=f"ogr2ogr -f 'PGDump' -nlt MULTILINESTRING "
        "-t_srs EPSG:28992 -s_srs EPSG:4326 "
        f"-nln {dag_id}_new "
        f"{tmp_file_prefix}.sql {tmp_file_prefix}.json",
    )

    load_table = BashOperator(
        task_id="load_table", bash_command=f"psql {pg_params} < {tmp_file_prefix}.sql",
    )

    rename_table = PostgresOperator(
        task_id="rename_table", sql=sql_table_rename, params=dict(tablename=dag_id),
    )

import_routes >> extract_geojson >> load_table >> rename_table
