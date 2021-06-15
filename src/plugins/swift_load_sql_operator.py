import tempfile
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psql_cmd_hook import PsqlCmdHook
from swift_hook import SwiftHook
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

    @apply_defaults
    def __init__(
        self,
        container,
        object_id,
        db_target_schema=None,
        swift_conn_id="swift_default",
        *args,
        **kwargs
    ):
        self.container = container
        self.object_id = object_id
        self.swift_conn_id = swift_conn_id
        self.db_target_schema = db_target_schema
        super().__init__(*args, **kwargs)

    def execute(self, context):
        swift_hook = SwiftHook(swift_conn_id=self.swift_conn_id)
        with tempfile.TemporaryDirectory() as tmpdirname:
            object_path = Path(self.container) / self.object_id
            download_filepath = Path(tmpdirname) / object_path
            swift_hook.download(self.container, self.object_id, download_filepath)
            if download_filepath.suffix == ".zip":
                zip_hook = ZipHook(download_filepath)
                filenames = zip_hook.unzip(tmpdirname)
            else:
                filenames = [object_path]
            psql_cmd_hook = PsqlCmdHook(db_target_schema=self.db_target_schema)
            psql_cmd_hook.run(Path(tmpdirname) / fn for fn in filenames)
