import tempfile
from pathlib import Path
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from swift_hook import SwiftHook
from zip_hook import ZipHook
from psql_cmd_hook import PsqlCmdHook


class SwiftLoadSqlOperator(BaseOperator):
    """ This operator combines the use of serveral hooks.
        It downloads a zip from the objectstore,
        unzips it and feeds the unzipped sql directly
        to postgresql. Doing this in one go, avoids the use
        of temporary files that are passed on between operators,
        because this is potentially problematic in a multi-container
        Airflow environment.
    """

    @apply_defaults
    def __init__(
        self, container, object_id, swift_conn_id="swift_default", *args, **kwargs
    ):
        self.container = container
        self.object_id = object_id
        self.swift_conn_id = swift_conn_id
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
            psql_cmd_hook = PsqlCmdHook()
            psql_cmd_hook.run(Path(tmpdirname) / fn for fn in filenames)
