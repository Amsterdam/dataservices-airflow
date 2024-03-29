import tempfile
from pathlib import Path

from airflow.models import BaseOperator
from psql_cmd_hook import PsqlCmdHook
from swift_hook import SwiftHook
from typing import Optional
from zip_hook import ZipHook


class SwiftLoadSqlOperator(BaseOperator):
    """This operator combines the use of serveral hooks.
    It downloads a zip from the objectstore,
    unzips it and feeds the unzipped sql directly
    to postgresql. Doing this in one go, avoids the use
    of temporary files that are passed on between operators,
    because this is potentially problematic in a multi-container
    Airflow environment.

    A database schema can be (optionally) specified to the PsqlCmdHook.
    In that case the database schema will be dropped if exists and (re)created if not exists.
    This can be usefull if there is a discrepancy between the source data and the Amsterdam
    schema definition. When source tables exists that are not (yet) specified in the
    Amsterdam schema definition, it causes an error from a second run.
    Because tables are removed before a new insert based on existence
    in the Amsterdam schema definition. If not, it remains and the insert
    cannot be executed (object already exists).
    """

    def __init__(
        self,
        container: str,
        object_id: str,
        dataset_name: Optional[str] = None,
        db_target_schema=None,
        swift_conn_id="swift_default",
        db_search_path: Optional[list[str]] = None,
        bash_cmd_before_psql: Optional[list[str]] = None,
        *args,
        **kwargs
    ):
        """Initialize.

        args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            db_search_path: List of one or more database schema names that needs to be
                present in the search path. I.e. for locating geometry datatypes.
                Defaults to None.
            bash_cmd_before_psql: In some very specific cases, source files can contain
                a schema reference to the geometry datatype. I.e. public.geometry. The geometry
                datatype is in refDB on Azure not installed in schema public. Therefor the
                schema name can be omitted and use the `db_search_path` argument to locate the
                geometry schema. A simple bash command can be added to be executed before
                executing SQL by psql connection.
                Default to None.

        returns:
            class instance.
        """
        self.container = container
        self.object_id = object_id
        self.dataset_name = dataset_name
        self.swift_conn_id = swift_conn_id
        self.db_target_schema = db_target_schema
        self.db_search_path = db_search_path
        self.bash_cmd_before_psql = bash_cmd_before_psql
        super().__init__(*args, **kwargs)

    def execute(self, context):
        swift_hook = SwiftHook(swift_conn_id=self.swift_conn_id)
        with tempfile.TemporaryDirectory() as tmpdirname:
            object_path = Path(self.container) / self.object_id
            download_filepath = Path(tmpdirname) / object_path
            swift_hook.download(
                self.container, self.object_id, download_filepath)
            if download_filepath.suffix == ".zip":
                zip_hook = ZipHook(download_filepath)
                filenames = zip_hook.unzip(tmpdirname)
            else:
                filenames = [object_path]
            psql_cmd_hook = PsqlCmdHook(
                db_target_schema=self.db_target_schema,
                dataset_name=self.dataset_name,
                db_search_path=self.db_search_path,
                bash_cmd_before_psql=self.bash_cmd_before_psql
            )
            psql_cmd_hook.run(Path(tmpdirname) / fn for fn in filenames)
