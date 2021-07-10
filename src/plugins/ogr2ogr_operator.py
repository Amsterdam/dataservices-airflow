import subprocess  # noqa: S404
from pathlib import Path
from typing import Any, List, Optional, Union

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import Context
from common.db import DatabaseEngine


class Ogr2OgrOperator(BaseOperator):
    """Translates various file formats with spatial data to SQL output."""

    def __init__(
        self,
        *,
        target_table_name: str,
        input_file: Union[Path, str],
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
        promote_to_multi: bool = False,
        **kwargs: Any,
    ) -> None:
        """Setup params.

        Args:
            target_table_name: Name of target database table to write data.
            input_file: Source file to import into database.
            conn_id: Database connection. Defaults to "postgres_default".
            s_srs: The source geo reference system. Defaults to "EPSG:4326".
            t_srs: The target geo reference system. Defaults to "EPSG:28992".
            fid: The record identifier. Defaults to "id".
            geometry_name: Column name that contains the geometry data. Defaults to "geometry".
            sql_output_file: Output SQL file path. Defaults to None.
            sql_statement: SQL subselection on data before import. Defaults to None.
            input_file_sep: The row separator. Defaults to None.
            auto_detect_type: Can be set to "YES" to auto detect datatype. Defaults to None.
            mode: If set to `PostgreSQL` data is directly imported into database.
                Defaults to "PGDump" which output a file.
            db_conn: Database engine instance. Defaults to None.
            encoding_schema: Source character schema. Defaults to "UTF-8".
            promote_to_multi: Should geometry be converted to an multigeometry object?
                This can be needed if the source has e.g. points and multipoints. With this option
                set to True all points are set to multipoints as well.
        """
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
        self.promote_to_multi = promote_to_multi

    def execute(self, context: Context) -> None:
        """Proces the input file with OGR2OGR to SQL output.

        Args:
            context: 'PGDump' will generate a sql file (the default)
                'PostgreSQL' will load data directly into database
        Executes:
                A batch ogr2ogr cmd
        """
        input_file: Path = Path(self.input_file)
        input_file.parents[0].mkdir(parents=True, exist_ok=True)

        # setup the cmd to execute
        program = "ogr2ogr"
        ogr2ogr_cmd: List[str] = [program, "-f", self.mode]

        # Option 1 SQL (default): create sql file
        if self.mode == "PGDump":

            sql_output_file: Path = Path(self.sql_output_file)
            sql_output_file.parents[0].mkdir(parents=True, exist_ok=True)

            # mandatory
            ogr2ogr_cmd.extend(["-nln", self.target_table_name])
            ogr2ogr_cmd.extend([self.sql_output_file, input_file.as_posix()])

            # optionals
            if self.input_file_sep:
                ogr2ogr_cmd.extend(["-lco", f"SEPARATOR={self.input_file_sep}"])

        # Option 2 DIRECT LOAD: load data directly into DB; no file created in between
        else:

            # mandatory
            ogr2ogr_cmd.append(
                f"PG:host={getattr(self.db_conn, 'host')} "  # noqa: B009
                f"dbname={getattr(self.db_conn, 'db')} "  # noqa: B009
                f"user={getattr(self.db_conn, 'user')} "  # noqa: B009
                f"password={getattr(self.db_conn, 'password')} "  # noqa: B009
                f"port={getattr(self.db_conn, 'port')}"  # noqa: B009
            )
            ogr2ogr_cmd.append(input_file.as_posix())
            ogr2ogr_cmd.extend(["-nln", self.target_table_name, "-overwrite"])

        # generic parameters for all options
        if self.s_srs:
            ogr2ogr_cmd.extend(["-s_srs", self.s_srs])
        ogr2ogr_cmd.extend(["-t_srs", self.t_srs])
        ogr2ogr_cmd.extend(["-lco", f"FID={self.fid}"])
        ogr2ogr_cmd.extend(["-oo", f"AUTODETECT_TYPE={self.auto_detect_type}"])
        ogr2ogr_cmd.extend(["-lco", f"GEOMETRY_NAME={self.geometry_name}"])
        ogr2ogr_cmd.extend(["-lco", f"ENCODING={self.encoding_schema}"])
        ogr2ogr_cmd.extend(["-lco", "precision=NO"])

        # generic optional parameters
        if self.sql_statement:
            ogr2ogr_cmd.extend(["-sql", self.sql_statement])
        if self.promote_to_multi:
            ogr2ogr_cmd.extend(["-nlt", "PROMOTE_TO_MULTI"])

        # execute cmd string
        try:
            result = subprocess.run(  # noqa: S603
                ogr2ogr_cmd, capture_output=True, check=True, text=True
            )
            self.log.debug(result.stdout)
        except subprocess.CalledProcessError as cpe:
            self.log.error(cpe.stderr)
            self.log.exception("Failed to run %r.", program)
