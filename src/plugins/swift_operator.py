from pathlib import Path
from swiftclient.service import SwiftService, SwiftError
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class SwiftOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, container: str, object_id: str, output_path: str, *args, **kwargs
    ) -> None:
        self.container = container
        self.object_id = object_id
        self.output_path = output_path
        super().__init__(*args, **kwargs)

    def execute(self, context):
        Path(self.output_path).parents[0].mkdir(parents=True, exist_ok=True)
        download_options = {
            "out_file": self.output_path,
        }
        # SwiftService needs proper setup of env vars.
        with SwiftService() as swift:
            try:
                for down_res in swift.download(
                    container=self.container,
                    objects=[self.object_id],
                    options=download_options,
                ):
                    if down_res["success"]:
                        self.log.info("downloaded: %s", down_res["object"])
                    else:
                        self.log.error("download failed: %s", down_res["object"])

            except SwiftError as e:
                self.log.error(e.value)
