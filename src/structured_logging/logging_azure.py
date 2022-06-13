import base64
import datetime
import hashlib
import hmac
import json
import os
from typing import Final

import requests


def send_airflow_execution_logs_to_azure(log_record: dict) -> None:
    """Sending Airflow logs to Azure Log Analytics Workspace (LAWS) instance.

    ::Azure data collector API::
    Sending the Airflow execution logs to Azure LAWS is executed by the Azure data collector
    API. See for more information:
    https://docs.microsoft.com/nl-nl/azure/azure-monitor/logs/data-collector-api

    ::Managed Idenities::
    The authorization is done by an Azure Managed Idenitity (MI) bound to the Airflow Instance.
    This MI must have a role-binding to the Key Vault on the related Azure subscription of the
    data team of the Airflow instance.

    ::Needed environment variables::
    Your Airflow instance must contain the environment variable `AZURE_LAWS_ID` and
    `AZURE_LAWS_KEY` that holds respectively the ID and Key of the LAWS instance.
    To activate the Airflow execution logs to be send to Azure LAWS the environment variables
    `AZURE_AIRFLOW_EXECUTION_LOGS_TO_LAWS` must be explicitly send to True.

    ::LAWS log table name::
    The Airflow execution logs are send to the LAWS table `AIRFLOW_LOGS_CL`. The `CL` postfix
    is added by Azure itself and it represents the words 'custom logs'.

    NOTE: Unforunately, we cannot use the Python SDK `opencensus-ext-azure` for Azure and
    `opencensus-ext-logging` to send custom logs to an Azure Application Insights (APPI)
    instance. The Python SDK `opencensus-ext-azure` packages usages the Python `logging`
    package and that interfers with the inner working of Airflow logging. Since the latter
    uses the Python `logging` package as well. Resulting a weird recursive call that ends
    up in errors.
    """
    if isinstance(log_record, dict):

        # get laws credentials needed to write Airflow logs to laws
        laws_id = os.environ.get("AZURE_LAWS_ID")
        laws_key = os.environ.get("AZURE_LAWS_KEY")

        # parse Airflow log record into a json string
        json_data = [{**log_record}]
        body = json.dumps(json_data)

        # The log type holds of the name of the LAWS table where the logs
        # will be placed.
        LOG_TYPE: Final = "AIRFLOW_LOGS"

        # posting logs to Azure LAWS.
        if laws_id and laws_key:
            post_data(laws_id, laws_key, body, LOG_TYPE)


def build_signature(
    LAW_ID: str,
    LAW_KEY: str,
    date: str,
    content_length: int,
    method: str,
    content_type: str,
    resource: str,
) -> str:
    """Create API signature before sending log data to Azure LAWS."""
    x_headers = "x-ms-date:" + date
    # string hash example:
    # POST \n 500 \n application/json \n x-ms-date: Sun, 20 Mar 2022 08:46:12 GMT \n /api/logs
    string_to_hash = (
        method
        + "\n"
        + str(content_length)
        + "\n"
        + content_type
        + "\n"
        + x_headers
        + "\n"
        + resource
    )
    bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
    decoded_key = base64.b64decode(LAW_KEY)
    encoded_hash = base64.b64encode(
        hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
    ).decode()
    authorization = f"SharedKey {LAW_ID}:{encoded_hash}"
    return authorization


def post_data(LAW_ID: str, LAW_KEY: str, body: str, log_type: str) -> None:
    """Create API (post) request based on signature (see build_signature).

    Before sending log data to Azure LAWS.
    """
    method = "POST"
    content_type = "application/json"
    resource = "/api/logs"
    rfc1123date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    content_length = len(body)
    signature = build_signature(
        LAW_ID, LAW_KEY, rfc1123date, content_length, method, content_type, resource
    )
    uri = "https://" + LAW_ID + ".ods.opinsights.azure.com" + resource + "?api-version=2016-04-01"
    headers = {
        "content-type": content_type,
        "Authorization": signature,
        "Log-Type": log_type,
        "x-ms-date": rfc1123date,
    }
    requests.post(uri, data=body, headers=headers)
