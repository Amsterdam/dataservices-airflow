import logging
import os
from typing import Any, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient


class AzureBlobOperator(BaseOperator):
    """Class to interact with an Azure storage account.

    `About WasbHook`
    This class contains the use of the WasbHook operator. The WasbHook uses under the hood the official Python Azure SDK.
    See: https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/_api/airflow/providers/microsoft/azure/hooks/wasb/index.html

    """

    def __init__(
        self,
        storage_account_conn: str = "DUMMY",
        type: Optional[str] = "MID",
        mid_client_id: Optional[str] = None,
        mid_sa_account_url: Optional[str] = None,
        container_name: Optional[str] = None,
        blob_name: Optional[str] = None,
        action_type: str = "download",
        file_path: Optional[str] = None,
        upload_overwrite: bool = True,
        del_blob_is_prefix: bool = False,
        del_blob_ignore_if_missing: bool = False,
        *args: Any,
        **kwargs: dict,
    ) -> None:
        """Intialize method.

        Args:
        storage_account_conn: Optional, the storage account connection string or SAS.
            This can be used for testing. Not for production.
        type: Optional, indication of not using a Managed Identity. Defaults to using
            a MID.
        mid_client_id: Optional, the Managed Identity client id that is bound to your Airflow
            instance. The MID must have the correct role to the storage account for its actions.
        mid_sa_account_url: Optional, the storage account URL to perform the action on. This
            must be present when using a MID.
        container_name: Optional, name of the container where the BLOB is located.
        blob_name: Optional, name of BLOB that apply to the `action_type` argument.
        action_type: Optional, the type of action to apply, this could
            be `download`, `upload`, `delete blob`, `delete container` or
            `create container`. Defaults to `download`.
        file_path: Optional, the file path to store the blob if action is `download` or `upload`.
        upload_overwrite: Optional, bool value to overwrite if file already exists whilst uploading.
            If set to false it will raise an `ResourceExistsError`. Defaults to `true`.
        del_blob_is_prefix: Optional, bool value when `action_type` == `deleting blob`
            then consider the `blob_name` as a prefix to delete blobs. Defaults to `false`.
        del_blob_ignore_if_missing: Optional, bool value when `action_type` == `deleting blob`
            then ignore if blob does not exists. Defaults to `false`.

        """
        self.storage_account_conn = storage_account_conn
        self.type = type
        self.managed_identity = mid_client_id
        self.storage_account_url = mid_sa_account_url
        self.container_name = container_name
        self.blob_name = blob_name
        self.action_type = action_type
        self.file_path = file_path
        self.upload_overwrite = upload_overwrite
        self.del_blob_is_prefix = del_blob_is_prefix
        self.del_blob_ignore_if_missing = del_blob_ignore_if_missing
        super().__init__(*args, **kwargs)

    def execute(self, context: Optional[dict[str, Any]] = None) -> None:
        """Dispatcher to call the WasbHook methods i.e. download, upload or delete.

        Args:
        context: When this operator is created the context parameter is used
        to refer to get_template_context for more context as part of
        inheritance of the BaseOperator. It is set to None in this case.

        NOTE: Since this operator relies on the methods of the WasbHook operator
            we need a `storage_account_conn` to be present as an environment
            variable like so `AIRFLOW_CONN_<NAME>`. Eventhough we might use
            the Managed Identity (MID) for the autorization to the Storage Account,
            and not use a connection string. In the latter the `AIRFLOW_CONN_<NAME>`
            would hold a value like so `wasb://:@?connection_string=<conn-string-value>`.

        About Managed Identities:
            Airflow is depends on using a Managed Identity (MID) if it is activated on AKS
            cluster level. Since we are dealing with Managed Identities for each Airflow instance
            (also read as: per namespace), we must specifiy the MID to be used explicity and cannot
            use the default behaviour (using one MID per AKS cluster). For using the MID, the MID
            has to have the correct role-binding to the storage account. Here you can find the roles
            supported:
            https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles

        """
        # setup logger so output can be added to the Airflow logs
        logger = logging.getLogger(__name__)
        # Use managed identity if no connection string is used.
        if self.type == "MID":
            # create dummy connection to still make profit of the WasbHook operator
            # and it's methods while using a MID for authentication and autorization.
            os.environ["AIRFLOW_CONN_DUMMY"] = "DUMMY"
            # Instantiate the WasbHook operator class
            self.hook = WasbHook(wasb_conn_id=self.storage_account_conn)
            my_mid_client_id = self.managed_identity
            my_sa_account_url = self.storage_account_url
            credential = ManagedIdentityCredential(client_id=my_mid_client_id)
            self.log.info("Using managed identity as credential")
            self.hook.connection = BlobServiceClient(
                account_url=my_sa_account_url, credential=credential
            )
        else:
            # This route will be choosen if you do not connect to the Storage Account
            # by MID but by a connection string where `AIRFLOW_CONN_<NAME>` is set to
            # `wasb://:@?connection_string=<conn-string-value>`.
            # Instantiate the WasbHook operator class to make use of the Storage Account
            # operations like download, upload or create.
            self.hook = WasbHook(wasb_conn_id=self.storage_account_conn)
        if self.action_type == "download":
            logger.info(
                "%s: %s/%s to %s",
                self.action_type,
                self.container_name,
                self.blob_name,
                self.file_path,
            )
            self.hook.get_file(
                file_path=self.file_path,
                container_name=self.container_name,
                blob_name=self.blob_name,
            )
        elif self.action_type == "upload":
            logger.info(
                "%s: %s to %s/%s",
                self.action_type,
                self.file_path,
                self.container_name,
                self.blob_name,
            )
            self.hook.load_file(
                file_path=self.file_path,
                container_name=self.container_name,
                blob_name=self.blob_name,
                overwrite=self.upload_overwrite,
            )
        elif self.action_type in ("delete blob"):
            logger.info(
                f"%s: %s/%s {'(as prefix}' if  self.del_blob_is_prefix else None}",
                self.action_type,
                self.container_name,
                self.blob_name,
            )
            self.hook.delete_file(
                container_name=self.container_name,
                blob_name=self.blob_name,
                is_prefix=self.del_blob_is_prefix,
                ignore_if_missing=self.del_blob_ignore_if_missing,
            )
        elif self.action_type in ("delete container", "create container"):
            logger.info(f"%s: %s", self.action_type, self.container_name)
            if self.action_type == "delete container":
                self.hook.delete_container(container_name=self.container_name)
            else:
                self.hook.create_container(container_name=self.container_name)
        else:
            logger.error("No action specified, do nothing!")
