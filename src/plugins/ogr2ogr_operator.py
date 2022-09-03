import subprocess  # noqa: S404
from pathlib import Path
from typing import Any, Optional, Union

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import Context
from common.db import DatabaseEngine


class Ogr2OgrOperator(BaseOperator):
    """Translates various file formats with spatial data to SQL output."""

    def __init__(
        self,
        *,
        input_file: Union[Path, str],
        conn_id: str = "postgres_default",
        s_srs: str = "EPSG:4326",
        t_srs: str = "EPSG:28992",
        fid: str = "id",
        geometry_name: str = "geometry",
        target_table_name: Optional[str] = None,
        source_table_name: Optional[list[str]] = None,
        sql_output_file: Optional[str] = None,
        sql_statement: Optional[str] = None,
        input_file_sep: Optional[str] = None,
        auto_detect_type: Optional[str] = None,
        mode: str = "PGDump",
        dataset_name: Optional[str] = None,
        db_conn: Optional[DatabaseEngine] = None,
        db_schema: Optional[str] = None,
        encoding_schema: str = "UTF-8",
        promote_to_multi: bool = False,
        sqlite_source: bool = False,
        twodimenional: bool = False,
        nln_options: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> None:
        """Setup params.

        Args:
            target_table_name: Name of target database table to write data.
            source_table_name: List of database tables to read data from (in
                case of a sqllite file for instance)
            sqlite_source: If a sqlite then set to True. The ogr2ogr cmd
                would be different construct.
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
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            db_conn: Database engine instance. Defaults to None.
            db_schema: The active database schema to perform SQL statements. Defaults to
                None. If None, it will use the defined `temp_db_schema` from DatabaseEngine.
            encoding_schema: Source character schema. Defaults to "UTF-8".
            promote_to_multi: Should geometry be converted to an multigeometry object?
                This can be needed if the source has e.g. points and multipoints. With this option
                set to True all points are set to multipoints as well.
            twodimenional: Indicator if the data should be converted to 2D. If set to True
                an extra argument is placed to explicitly convert to 2D.
            nln_options: ogr2ogr argument -nln to add to command. This is a list, can be multiple.
        """
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.s_srs = s_srs
        self.t_srs = t_srs
        self.target_table_name = target_table_name
        self.source_table_name = source_table_name
        self.sqlite_source = sqlite_source
        self.sql_output_file = sql_output_file if sql_output_file else f"{input_file}.sql"
        self.input_file = input_file
        self.input_file_sep = input_file_sep
        self.auto_detect_type = auto_detect_type
        self.fid = fid
        self.geometry_name = geometry_name
        self.sql_statement = sql_statement
        self.mode = mode
        self.dataset_name = dataset_name
        self.db_conn = db_conn
        self.db_schema = db_schema
        self.encoding_schema = encoding_schema
        self.promote_to_multi = promote_to_multi
        self.twodimenional = twodimenional
        self.nln_options = nln_options

    def execute(self, context: Context) -> None:
        """Proces the input file with OGR2OGR to SQL output.

        Args:
            context: Based on the calling DAG, its ID will
                be used as a surrogate dataset_name as part
                of the database user name.
                Only used if dataset_name is None.
        Executes:
                A batch ogr2ogr cmd
        """
        if self.db_conn is None:
            self.db_conn = DatabaseEngine(dataset_name=self.dataset_name, context=context)

        input_file: Path = Path(self.input_file)
        input_file.parents[0].mkdir(parents=True, exist_ok=True)

        # setup the cmd to execute
        program = "ogr2ogr"
        ogr2ogr_cmd: list[str] = [program, "-overwrite", "-f", self.mode]
        self.db_schema = self.db_schema if self.db_schema else self.db_conn.temp_db_schema

        # Option 1 SQL (default): create sql file
        if self.mode == "PGDump":

            sql_output_file: Path = Path(self.sql_output_file)
            sql_output_file.parents[0].mkdir(parents=True, exist_ok=True)

            if not self.sqlite_source:
                ogr2ogr_cmd.extend([self.sql_output_file, input_file.as_posix()])
            if self.input_file_sep:
                ogr2ogr_cmd.extend(["-lco", f"SEPARATOR={self.input_file_sep}"])
            if self.source_table_name and self.sqlite_source:
                ogr2ogr_cmd.extend([input_file.as_posix(), *self.source_table_name])
            if self.target_table_name:
                ogr2ogr_cmd.extend(["-nln", f"{self.db_schema}.{self.target_table_name}"])
            if self.twodimenional:
                ogr2ogr_cmd.extend(["-dim 2"])

        # Option 2 DIRECT LOAD: load data directly into DB; no file created in between
        elif self.mode == "PostgreSQL":

            ogr2ogr_cmd.append(
                f"PG:host={self.db_conn.host} "
                f"dbname={self.db_conn.db} "
                f"user={self.db_conn.user} "
                f"password={self.db_conn.password} "
                f"port={self.db_conn.port} "
                f"active_schema={self.db_schema if self.db_schema else self.db_conn.temp_db_schema}"  # noqa: E501
            )
            if not self.sqlite_source:
                ogr2ogr_cmd.append(input_file.as_posix())

            if self.nln_options:
                for option in self.nln_options:
                    ogr2ogr_cmd.extend(["-nln", option])

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
        if self.source_table_name and self.sqlite_source:
            ogr2ogr_cmd.extend([input_file.as_posix(), *self.source_table_name])
        if self.target_table_name:
            ogr2ogr_cmd.extend(["-nln", f"{self.db_schema}.{self.target_table_name}"])
        if self.twodimenional:
            ogr2ogr_cmd.extend(["-dim 2"])

        # execute cmd string
        try:
            result = subprocess.run(  # noqa: S603
                ogr2ogr_cmd, capture_output=True, check=True, text=True
            )
            self.log.debug(result.stdout)
        except subprocess.CalledProcessError as cpe:
            self.log.error(cpe.stderr)
            self.log.exception("Failed to run %r.", program)
