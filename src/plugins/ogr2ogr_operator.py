import subprocess
from airflow.models.baseoperator import BaseOperator
from common.db import DatabaseEngine


class Ogr2OgrOperator(BaseOperator):
    """" translates various file formats with spatial data to SQL output """

    def __init__(
        self,
        *,
        target_table_name,
        input_file,
        conn_id="postgres_default",
        s_srs="EPSG:4326",
        t_srs="EPSG:28992",
        fid="id",
        geometry_name="geometry",
        sql_output_file=None,
        sql_statement=None,
        input_file_sep=None,
        auto_detect_type=None,
        mode="PGDump",
        db_conn: DatabaseEngine = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.s_srs = s_srs
        self.t_srs = t_srs
        self.target_table_name = target_table_name
        self.sql_output_file = sql_output_file if sql_output_file else f"{input_file}.sql"
        self.input_file = input_file
        self.input_file_sep = input_file_sep
        self.auto_detect_type = auto_detect_type
        self.fid = fid
        self.geometry_name = geometry_name
        self.sql_statement = sql_statement
        self.mode = mode
        self.db_conn = db_conn

    def execute(self, context=None):
        """proces the input file with OGR2OGR to SQL output

        ARGS:
        mode: 'PGDump' will generate a sql file (the default)
              'PostgreSQL' will load data directly into database
        """

        # setup the cmd to execute
        ogr2ogr_cmd = []
        ogr2ogr_cmd.append(f"ogr2ogr -f '{self.mode}' ")

        # Option 1 SQL (default): create sql file
        if self.mode == "PGDump":

            # mandatory
            ogr2ogr_cmd.append(f"-nln {self.target_table_name} ")
            ogr2ogr_cmd.append(f"{self.sql_output_file} {self.input_file} ")

            # optionals
            self.input_file_sep and ogr2ogr_cmd.append(f"-lco SEPARATOR={self.input_file_sep} ")
            self.sql_statement and ogr2ogr_cmd.append(f"-sql {self.sql_statement}")

        # Option 2 DIRECT LOAD: load data directly into DB; no file created in between
        else:

            # mandatory
            ogr2ogr_cmd.append(
                f"PG:'host={self.db_conn.host} dbname={self.db_conn.db} user={self.db_conn.user} password={self.db_conn.password} port={self.db_conn.port}' "
            )
            ogr2ogr_cmd.append(f"{self.input_file} ")
            ogr2ogr_cmd.append(f"-nln {self.target_table_name} -overwrite ")

        # generic parameters for all options
        ogr2ogr_cmd.append(f"{'-s_srs ' + self.s_srs if self.s_srs else ''} -t_srs {self.t_srs} ")
        ogr2ogr_cmd.append(f"-lco FID={self.fid} ")
        ogr2ogr_cmd.append(f"-oo AUTODETECT_TYPE={self.auto_detect_type} ")
        ogr2ogr_cmd.append(f"-lco GEOMETRY_NAME={self.geometry_name} ")

        # execute cmd string
        print("".join(ogr2ogr_cmd))
        subprocess.run("".join(ogr2ogr_cmd), shell=True, check=True)
