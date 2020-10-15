import subprocess
from airflow.models.baseoperator import BaseOperator


class Ogr2OgrOperator(BaseOperator):
    """" translates various file formats with spatial data to SQL output """

    def __init__(
        self,
        *,
        target_table_name,
        sql_output_file,
        input_file,
        conn_id="postgres_default",
        s_srs="EPSG:4326",
        t_srs="EPSG:28992",
        fid="id",
        geometry_name="geometry",
        sql_statement=None,
        input_file_sep=None,
        auto_dect_type=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.s_srs = s_srs
        self.t_srs = t_srs
        self.target_table_name = target_table_name
        self.sql_output_file = sql_output_file
        self.input_file = input_file
        self.input_file_sep = input_file_sep
        self.auto_dect_type = auto_dect_type
        self.fid = fid
        self.geometry_name = geometry_name
        self.sql_statement = sql_statement

    def execute(self, context=None):
        """ proces the input file with OGR2OGR to SQL output """

        # setting up the cmd to execute
        ogr2ogr_cmd = []
        ogr2ogr_cmd.append("ogr2ogr -f 'PGDump' ")
        ogr2ogr_cmd.append(f"-s_srs {self.s_srs} -t_srs {self.t_srs} ")
        ogr2ogr_cmd.append(f"-nln {self.target_table_name} ")
        ogr2ogr_cmd.append(f"{self.sql_output_file} {self.input_file} ")
        ogr2ogr_cmd.append(f"-lco FID={self.fid} ")
        ogr2ogr_cmd.append(f"-lco GEOMETRY_NAME={self.geometry_name} ")

        # optionals
        self.input_file_sep and ogr2ogr_cmd.append(
            f"-lco SEPARATOR={self.input_file_sep} "
        )
        self.auto_dect_type and ogr2ogr_cmd.append(
            f"-oo AUTODETECT_TYPE={self.auto_dect_type} "
        )
        self.sql_statement and ogr2ogr_cmd.append(f"-sql {self.sql_statement}")

        # execute cmd string
        subprocess.run("".join(ogr2ogr_cmd), shell=True, check=True)
