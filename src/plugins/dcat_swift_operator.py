import urllib
from urllib.parse import urlparse

import requests
from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
from swift_hook import SwiftHook


class DCATSwiftOperator(BaseOperator):
    def __init__(
        self,
        dataset_title: str,
        distribution_id: str,
        environment: str,
        input_path: str,
        swift_conn_id: str = "objectstore_datacatalogus",
        *args,
        **kwargs,
    ) -> None:
        self.dataset_title = dataset_title
        self.distribution_id = distribution_id
        self.environment = environment
        self.input_path = input_path
        self.swift_conn_id = swift_conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context):
        """
        Update a existing distribution(resource) in the datacatalogus
        identified by dataset_title and distribution id
        """
        acc = "acc." if self.environment == "acceptance" else ""
        # First determine dataset identifier for dataset title
        title = urllib.parse.quote_plus(self.dataset_title)
        url = f"https://{acc}api.data.amsterdam.nl/dcatd/datasets?/properties/dct:title=eq={title}"
        r = requests.get(url)
        if r.status_code != 200:
            raise AirflowException(f"Failed to find dataset with title: {self.dataset_title}")
        json1 = r.json()
        if "dcat:dataset" not in json1 or len(json1["dcat:dataset"]) < 1:
            raise AirflowException(f"Failed to find dataset with title: {self.dataset_title}")
        identifier = json1["dcat:dataset"][0]["dct:identifier"]
        # The use persistent URL to retrieve the objectstore ID for the resource
        purl = f"https://{acc}api.data.amsterdam.nl/dcatd/datasets/{identifier}/purls/{self.distribution_id}"  # noqa
        r = requests.head(purl)
        if r.status_code != 307:
            raise AirflowException(
                f"Failed to find distribution {self.distribution_id} for dataset {identifier}"
            )
        location = r.headers["Location"]
        o = urlparse(location)
        (_, container, object_id) = o.path.split("/", 2)
        if not (o.hostname and o.hostname.endswith("objectstore.eu")):
            f"Invalid location {location} for distribution {self.distribution_id} for dataset {identifier}"  # noqa
        self.log.info("Uploading: %s to %s-%s", self.input_path, container, object_id)
        self.hook = SwiftHook(swift_conn_id=self.swift_conn_id)
        # Upload the file to the specified object_id
        self.hook.upload(container, self.input_path, object_id)
