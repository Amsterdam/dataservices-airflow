import csv
import ftplib
from more_ds.network.url import URL
from os import path as ospath
from typing import List, TypedDict, Tuple, Optional, Generator


class Data(TypedDict):
    """Datastructure processenverbaal verkiezingen"""

    verkiezingsjaar: int
    volgnummer: str
    uri: str
    documentnaam: str
    stemlocatie: str
    id: str


class FTPListing:
    """This class is used for traversing a given folder base on a FTP
    server (objectstore) using BFS (Breadth-First search) algorithm.
    """

    def __init__(self, connection: ftplib.FTP) -> None:
        """Initialize

        Args:
            Connection: Hold the connection to the objectstore

        """
        self.connection = connection

    def list_dirs_and_files(self, _path: str) -> Tuple[List[str], Optional[List[str]]]:
        """Return files and directories within a path
        So it can be used to identifiy if there is need for another directory scan
        to locate files. Files already found can be used to get the URI.

        Args:
            _path: The path for looking for files and directories (within the same path)

        Returns:
            list of found dictories and files

        """

        list_, dirs, files = [], [], []
        try:
            self.connection.cwd(_path)
        except Exception:
            return [], []
        else:
            self.connection.retrlines("LIST", lambda x: list_.append(x.split()))
            for info in list_:
                type, name = info[0], info[-1]
                if type.startswith("d"):
                    dirs.append(name)
                else:
                    files.append(name)
            return dirs, files

    def traverse_folder(self, path: str = "/") -> Generator:
        """Recursive walk through directory tree, based on a BFS algorithm.
        This function acts like an orchestrator for looking for files and dirs.

        Args:
            path: The path for looking for files and directories at the same level

        Yields:
            list of all files incl its path until all directories are depleted

        """
        dirs, files = self.list_dirs_and_files(path)
        yield path, dirs, files
        for name in dirs:
            path = ospath.join(path, name)
            yield from self.traverse_folder(path)
            self.connection.cwd("..")
            path = ospath.dirname(path)


def save_data(
    startfolder: str, prefix_url: str, host: str, user: str, passwd: str, output_file: str
) -> None:
    """Save listing of data files to csv

    Args:
        startfolder: The starting directory to start looking for files and directories
        prefix_url: The protocol, subdomain and domain part of the URI to locate files
        host: the hostname of the objectstore where files are located
        user: the username that can access the objectstore
        passwd: the password that is used to access the objectstore
        output_file: name of .csv file to save

    Executes:
        Stores file URL's and it's metadata to .csv file

    Notes:
        The filenames are meaningful. It contains it's metadata. For example:
        `001.procesverbaaltk21.Amstel1.pdf` conceals <volgnummer>.<documentnaam>.<stemlocatie>.pdf
        Furthermore, the files are located in a folder which name
        represents it's election year i.e. 2021

    """
    data_to_save: List = []
    connection = ftplib.FTP(host=host)
    connection.login(user=user, passwd=passwd)
    get_listing = FTPListing(connection)

    for data in get_listing.traverse_folder(startfolder):
        for file in data[2]:

            verkiezingsjaar = data[0].split("/")[1]
            volgnummer = file.split(".")[0]
            uri = URL(prefix_url) / data[0] / file
            documentnaam = file.split(".")[1]
            stemlocatie = file.split(".")[2]

            metadata = Data(
                verkiezingsjaar=verkiezingsjaar,
                volgnummer=volgnummer,
                uri=uri,
                documentnaam=documentnaam,
                stemlocatie=stemlocatie,
                id=verkiezingsjaar + volgnummer,
            )
            data_to_save.append(metadata)

    header = Data.__annotations__.keys()
    data = [row.values() for row in data_to_save]

    with open(output_file, "w") as f:
        write = csv.writer(f, dialect=csv.unix_dialect)
        write.writerow(header)
        write.writerows(data)
