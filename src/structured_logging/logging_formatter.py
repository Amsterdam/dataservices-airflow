import logging
import os
from datetime import datetime

from pythonjsonlogger import jsonlogger

from structured_logging.logging_azure import send_airflow_execution_logs_to_azure


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """A custom JSON formater based on Python pythonjsonlogger."""

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        """Overwriting out of the log fields to be logged.

        Which attributes to log are defined in the logging_handler.py
        This method can overwrite the default values for those attributes if needed.
        """
        super().add_fields(log_record, record, message_dict)

        if not log_record.get("timestamp"):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now

        if log_record.get("level"):
            log_record["level"] = log_record["level"].upper()
        else:
            log_record["level"] = record.levelname


class CustomJsonFormatterAzureLogs(CustomJsonFormatter):
    """Subclass of CustomJsonFormatter above for adding sending logs to Azure LAWS.

    LAWS is the abbreviation for `Log Analytic Workspace`.

    Only applicable for DAG execution logs. Where each subprocess (task) log data will be send
    to Azure LAWS if activated.
    """

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        """Adds the extra line for sending logs to Azure LAWS."""
        super().add_fields(log_record, record, message_dict)

        # sending logs to Azure LAWS (if activated).
        # set the environment variable `AZURE_AIRFLOW_EXECUTION_LOGS_TO_LAWS` to value `true`
        # in your Airflow instance. Since we still use for now docker-compose for Airflow (on
        # cloudVPS anyways) it cannot handle boolean values. Only strings or integers.
        # Hence the string value of `true`.
        # See: https://devops.stackexchange.com/questions/537/why-is-one-not-allowed-to-use-a-boolean-in-a-docker-compose-yml # noqa: E501
        if os.environ.get("AZURE_AIRFLOW_EXECUTION_LOGS_TO_LAWS") == "true":
            send_airflow_execution_logs_to_azure(log_record)
