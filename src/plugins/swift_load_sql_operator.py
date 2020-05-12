import tempfile
from pathlib import Path
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from swift_hook import SwiftHook
from zip_hook import ZipHook
from psql_cmd_hook import PsqlCmdHook


class SwiftLoadSqlOperator(BaseOperator):
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
            zip_filepath = Path(tmpdirname) / "out.zip"
            swift_hook.download(self.container, self.object_id, zip_filepath)
            zip_hook = ZipHook(zip_filepath)
            filenames = zip_hook.unzip(tmpdirname)
            psql_cmd_hook = PsqlCmdHook()
            psql_cmd_hook.run(Path(tmpdirname) / fn for fn in filenames)
