import logging
import subprocess
from typing import Any, Dict, List, Optional

from airflow.models.baseoperator import BaseOperator
from common.db import DatabaseEngine

logger = logging.getLogger(__name__)


class Ogr2OgrOperator(BaseOperator):
    """ " translates various file formats with spatial data to SQL output"""

    def __init__(
        self,
        *,
        target_table_name: str,
        input_file: str,
        conn_id: str = "postgres_default",
        s_srs: str = "EPSG:4326",
        t_srs: str = "EPSG:28992",
        fid: str = "id",
        geometry_name: str = "geometry",
        sql_output_file: Optional[str] = None,
        sql_statement: Optional[str] = None,
        input_file_sep: Optional[str] = None,
        auto_detect_type: Optional[str] = None,
        mode: str = "PGDump",
        db_conn: Optional[DatabaseEngine] = None,
        encoding_schema: str = "UTF-8",
        **kwargs: Any,
    ) -> None:
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
        self.encoding_schema = encoding_schema

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:
        """proces the input file with OGR2OGR to SQL output

        ARGS:
        mode: 'PGDump' will generate a sql file (the default)
              'PostgreSQL' will load data directly into database

        Executes:
                A batch ogr2ogr cmd
        """

        # setup the cmd to execute
        ogr2ogr_cmd: List = []
        ogr2ogr_cmd.append(f"ogr2ogr -f '{self.mode}' ")

        # Option 1 SQL (default): create sql file
        if self.mode == "PGDump":

            # mandatory
            ogr2ogr_cmd.append(f"-nln {self.target_table_name} ")
            ogr2ogr_cmd.append(f"{self.sql_output_file} {self.input_file} ")

            # optionals
            if self.input_file_sep:
                ogr2ogr_cmd.append(f"-lco SEPARATOR={self.input_file_sep} ")

        # Option 2 DIRECT LOAD: load data directly into DB; no file created in between
        else:

            # mandatory
            ogr2ogr_cmd.append(
                f"PG:'host={getattr(self.db_conn, 'host')} "
                f"dbname={getattr(self.db_conn, 'db')} "
                f"user={getattr(self.db_conn, 'user')} "
                f"password={getattr(self.db_conn, 'password')} "
                f"port={getattr(self.db_conn, 'port')}' "
            )
            ogr2ogr_cmd.append(f"{self.input_file} ")
            ogr2ogr_cmd.append(f"-nln {self.target_table_name} -overwrite ")

        # generic parameters for all options
        ogr2ogr_cmd.append(f"{'-s_srs ' + self.s_srs if self.s_srs else ''} -t_srs {self.t_srs} ")
        ogr2ogr_cmd.append(f"-lco FID={self.fid} ")
        ogr2ogr_cmd.append(f"-oo AUTODETECT_TYPE={self.auto_detect_type} ")
        ogr2ogr_cmd.append(f"-lco GEOMETRY_NAME={self.geometry_name} ")
        ogr2ogr_cmd.append(f"-lco ENCODING={self.encoding_schema} ")
        ogr2ogr_cmd.append("-nlt PROMOTE_TO_MULTI ")
        ogr2ogr_cmd.append("-lco precision=NO ")
        if self.sql_statement:
            ogr2ogr_cmd.append(f"-sql {self.sql_statement}")

        # execute cmd string
        try:
            subprocess.run("".join(ogr2ogr_cmd), shell=True, check=True)
        except subprocess.CalledProcessError as err:
            logger.error(
                """Something went wrong, cannot execute subproces.
              Please check the ogr2ogr cmd %s
            """,
                err.output,
            )
