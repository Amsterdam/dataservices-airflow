import csv
from typing import Iterable, TypedDict

from common import make_hash
from more_ds.network.url import URL
from swift_hook import SwiftHook


class Data(TypedDict):
    """Datastructure processenverbaal verkiezingen"""

    verkiezingsjaar: int
    volgnummer: str
    uri: str
    documentnaam: str
    id: int


class ObjectStoreListing:
    """This class is used for traversing a given folder base on a FTP
    server (objectstore) using BFS (Breadth-First search) algorithm.
    """

    def __init__(self, connection: str) -> None:
        """Initialize

        Args:
            Connection: Hold the connection to the objectstore

        """
        self.connection = connection

    def list_files(self, start_folder: str) -> Iterable[str]:
        """Return all files within a path and its sub directories

        Args:
            start_folder: The starting path for looking for files

        Generates:
            found files of type str

        """

        try:
            swift_hook = SwiftHook(self.connection)
        except Exception as err:
            raise Exception from err
        else:
            files = []
            list_ = swift_hook.list_container(start_folder)
            for element in list_:
                if "directory" not in element.get("content_type", None):
                    files.append(element["name"])

        yield from files


def save_data(start_folder: str, base_url: str, conn_id: str, output_file: str) -> None:
    """Save listing of data files to csv

    Args:
        start_folder: The starting directory to start looking for files in main and sub directories
        base_url: The protocol, subdomain and domain part of the URI to locate files
        conn_id: Hold the connection to the objectstore
        output_file: name of .csv file to save

    Executes:
        Stores file URL's and it's metadata to .csv file

    Notes:
        The filenames are meaningful. It contains it's metadata. For example:
        `001.procesverbaaltk21.Amstel1.pdf` conceals <volgnummer>.<documentnaam>.<stemlocatie>.pdf
        Furthermore, the files are located in a folder which name
        represents it's election year i.e. 2021

    """
    data_to_save: list = []
    get_listing = ObjectStoreListing(conn_id)
    for file in get_listing.list_files(start_folder):
        if "pdf" in file:

            volgnummer = file.split(".")[0]
            documentnaam = file.split(".")[1]
            uri = URL(base_url) / start_folder / file
            verkiezingsjaar = file.split("/")[0]

            try:
                verkiezingsjaar_int = int(verkiezingsjaar)
            except ValueError as err:
                raise ValueError(
                    """Verkiezingsjaar is not a number. Check the folder
                    name where the files 'processenverbaal' are located.
                    The folder name must be set as YYYY as year of election.
                    """
                ) from err

            try:
                volgnummer_split = volgnummer.rsplit("/", 1)[1]
            except IndexError:
                # no subdirectories, just get orignal content
                volgnummer_split = volgnummer
                pass

            metadata = Data(
                verkiezingsjaar=verkiezingsjaar_int,
                volgnummer=volgnummer_split,
                uri=uri,
                documentnaam=documentnaam,
                id=make_hash([uri], 6),
            )
            data_to_save.append(metadata)

        fieldnames = Data.__annotations__.keys()

        with open(output_file, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, dialect=csv.unix_dialect)
            writer.writeheader()
            writer.writerows(data_to_save)
