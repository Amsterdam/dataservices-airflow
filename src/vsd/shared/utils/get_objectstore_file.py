import argparse
import logging
import os

import objectstore

# from various_small_datasets.generic.source import OBJECTSTORE
from common.objectstore import fetch_objectstore_credentials

log = logging.getLogger(__name__)


def get_objectstore_file(location, dir1, connection):
    path = "/".join(location.split("/")[1:])
    output_path = dir1 + "/" + path
    if os.path.isfile(output_path):
        log.warning(f"File {output_path} exists. Skip download")
        return
    else:
        log.warning(f"Get file {output_path}")
    if connection:
        credentials = fetch_objectstore_credentials(connection)
    else:
        credentials = fetch_objectstore_credentials()
    connection = objectstore.get_connection(credentials)
    container = location.split("/")[0]
    new_data = objectstore.get_object(connection, {"name": path}, container)
    output_dir = "/".join(output_path.split("/")[:-1])
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    with open(output_path, "wb") as file:
        file.write(new_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("-s", "--swift_connection")
    args = parser.parse_args()
    tmpdir = os.getenv("TMPDIR", "/tmp/")
    get_objectstore_file(args.filename, tmpdir, args.swift_connection)
