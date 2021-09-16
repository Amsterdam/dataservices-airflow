from pathlib import Path
from typing import List
from zipfile import ZipFile

from airflow.hooks.base import BaseHook


class ZipHook(BaseHook):
    """Implement unzipping as a hook, for easy re-use from operators."""

    def __init__(self, zip_file: str) -> None:
        """Initialize ZipHook.

        Args:
            zip_file: The zipfile to be extracted.
        """
        self.zip_file = zip_file

    def unzip(self, output_path: str) -> List[str]:
        """Unzips the zipfile."""
        self.log.info("Extracting: %s to %s", self.zip_file, output_path)
        Path(output_path).parents[0].mkdir(parents=True, exist_ok=True)
        with ZipFile(self.zip_file, "r") as zip_file:
            members = zip_file.namelist()
            zip_file.extractall(output_path)
            return members
