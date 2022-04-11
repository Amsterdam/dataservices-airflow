import logging
from datetime import datetime

from pythonjsonlogger import jsonlogger


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
