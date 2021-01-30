import subprocess
from environs import Env
from airflow.models.baseoperator import BaseOperator


class PgComparatorCDCOperator(BaseOperator):
    """
    PG_comparator CDC (change data capture) is used to detect modifications (insert, updates and deletes) on a target table based on a source table.
    Detected modifications are synchronzed into the target table.
    """

    def __init__(self, source_table, target_table, db_conn_id=None, **kwargs):
        super().__init__(**kwargs)
        self.source_table = source_table
        self.target_table = target_table
        self.db_conn_id = db_conn_id if db_conn_id else 'AIRFLOW_CONN_POSTGRES_DEFAULT'

    def execute(self, context=None):
        env = Env()
        db_connection = env(self.db_conn_id).split("?")[0].split("//")[1]

        self.log.info("Start change data capture (with pg_comparator) from: %s to: %s", self.source_table,  self.target_table)
        subprocess.run(
            f'pg_comparator --do-it --synchronize --max-ratio=2.0 --prefix={self.target_table}_cmp ' \
            f'pgsql://{db_connection}/{self.source_table} ' \
            f'pgsql://{db_connection}/{self.target_table}' \
            , shell=True, check=True
            )