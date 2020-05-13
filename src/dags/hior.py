import pathlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from common import (
    pg_params,
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from importscripts.import_hior import import_hior

SQL_TABLE_RENAME = """
    BEGIN;
    ALTER TABLE IF EXISTS hior_items RENAME TO hior_items_old;
    ALTER TABLE IF EXISTS hior_properties RENAME TO hior_properties_old;
    ALTER TABLE IF EXISTS hior_attributes RENAME TO hior_attributes_old;
    ALTER TABLE IF EXISTS hior_faq RENAME TO hior_faq_old;
    ALTER TABLE IF EXISTS hior_metadata RENAME TO hior_metadata_old;
    ALTER TABLE hior_items_new RENAME TO hior_items;
    ALTER TABLE hior_properties_new RENAME TO hior_properties;
    ALTER TABLE hior_attributes_new RENAME TO hior_attributes;
    ALTER TABLE hior_faq_new RENAME TO hior_faq;
    ALTER TABLE hior_metadata_new RENAME TO hior_metadata;
    DROP TABLE IF EXISTS hior_properties_old CASCADE;
    DROP TABLE IF EXISTS hior_attributes_old CASCADE;
    DROP TABLE IF EXISTS hior_items_old CASCADE;
    DROP TABLE IF EXISTS hior_faq_old CASCADE;
    DROP TABLE IF EXISTS hior_metadata_old CASCADE;
    COMMIT;
"""

dag_id = "hior"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
dag_config = Variable.get(dag_id, deserialize_json=True)

with DAG(dag_id, default_args=default_args, template_searchpath=["/"]) as dag:

    import_tables = []
    import_linked_tables = []
    tmp_dir = f"/tmp/{dag_id}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # fetch_xls = HttpFetchOperator(
    #     task_id="fetch_xls",
    #     endpoint=dag_config["xls_endpoint"],
    #     http_conn_id="hior_conn_id",
    #     tmp_file=f"{tmp_dir}/HIOR Amsterdam.xlsx",
    # )

    fetch_xls = BashOperator(
        task_id="fetch_xls",
        bash_command=f'wget -O "{tmp_dir}/HIOR Amsterdam.xlsx" '
        '"http://131f4363709c46b89a6ba5bc764b38b9.objectstore.eu/hior/HIOR Amsterdam.xlsx"',
    )

    convert_data = PythonOperator(
        task_id="convert_data",
        python_callable=import_hior,
        op_args=[f"{tmp_dir}/HIOR Amsterdam.xlsx", f"{tmp_dir}"],
    )

    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {sql_path}/hior_data_create.sql",
    )

    for path in (
        f"{tmp_dir}/hior_items_new.sql",
        f"{tmp_dir}/hior_faq_new.sql",
        f"{tmp_dir}/hior_metadata_new.sql",
    ):
        name = pathlib.Path(path).stem
        import_tables.append(
            BashOperator(
                task_id=f"create_{name}", bash_command=f"psql {pg_params()} < {path}",
            )
        )
    for path in (
        f"{tmp_dir}/hior_properties_new.sql",
        f"{tmp_dir}/hior_attributes_new.sql",
    ):
        name = pathlib.Path(path).stem
        import_linked_tables.append(
            BashOperator(
                task_id=f"create_{name}", bash_command=f"psql {pg_params()} < {path}",
            )
        )

    rename_table = PostgresOperator(task_id="rename_table", sql=SQL_TABLE_RENAME)

(
    slack_at_start
    >> fetch_xls
    >> convert_data
    >> create_table
    >> import_tables[1:]
    >> rename_table
)

create_table >> import_tables[0] >> import_linked_tables >> rename_table
