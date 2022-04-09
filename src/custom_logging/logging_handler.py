from logging.handlers import RotatingFileHandler

from airflow.utils.log.file_processor_handler import FileProcessorHandler
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import RedirectStdHandler

from custom_logging.logging_formatter import CustomJsonFormatter


class JsonStreamHandler(RedirectStdHandler):
    """Custom handler for streaming logs to sys.stdout, sys.stderr.

    Content content is in JSON format.
    """

    def __init__(self, stream: str) -> None:
        """Initialize class instance."""
        super().__init__(stream)
        # For attributes to log see:
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        json_formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(thread)s %(process)d %(filename)s %(funcName)s %(name)s %(message)s"  # noqa: E501
        )
        self.setFormatter(json_formatter)


class JsonFileTaskHandler(FileTaskHandler):
    """Custom handler for reading/saving task instances logs to disk.

    Content content is in JSON format.
    """

    def __init__(self, base_log_folder: str, filename_template: str) -> None:
        """Initialize class instance."""
        super().__init__(base_log_folder, filename_template)
        # For attributes to log see:
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        json_formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(thread)s %(process)d %(filename)s %(funcName)s %(name)s %(message)s"  # noqa: E501
        )
        self.setFormatter(json_formatter)


class JsonFileProcessorHandler(FileProcessorHandler):
    """Custom handler for handling DAG processor logs.

    Content content is in JSON format.
    """

    def __init__(self, base_log_folder: str, filename_template: str) -> None:
        """Initialize class instance."""
        super().__init__(base_log_folder, filename_template)
        # For attributes to log see:
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        json_formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(thread)s %(process)d %(filename)s %(funcName)s %(name)s %(message)s"  # noqa: E501
        )
        self.setFormatter(json_formatter)


class JsonRotatingFileHandler(RotatingFileHandler):
    """Supports rotation of disk log files.

    Content content is in JSON format.

    Rotation is an automated process which log files are  compressed, moved (archived),
    renamed or deleted once they are too old or too big (or other metrics that apply).
    New incoming log data is directed into a new fresh file (at the same location).
    """

    def __init__(self, filename: str, mode: str, maxBytes: int, backupCount: int) -> None:
        """Initialize class instance."""
        super().__init__(filename, mode, maxBytes, backupCount)
        # For attributes to log see:
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        json_formatter = CustomJsonFormatter(
            "%(timestamp)s %(level)s %(thread)s %(process)d %(filename)s %(funcName)s %(name)s %(message)s"  # noqa: E501
        )
        self.setFormatter(json_formatter)
