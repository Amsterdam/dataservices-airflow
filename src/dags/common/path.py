import logging
import shutil
from contextlib import suppress
from pathlib import Path

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task  # type: ignore[misc]
def mk_dir(path: Path, clean_if_exists: bool = False) -> Path:
    """Create and optionally clean directory.

    It is okay for the directory to be created to already exist.
    The parameter ``clean_if_exists`` can be used
    to specify that the directory should be cleaned
    of any containing directories and files.
    If this cleaning attempt results in errors,
    they will simply be ignored;
    The cleaning is simply a best effort attempt.

    Args:
        path: Path of the directory to create
        clean_if_exists: Whether to clean the directory if it already existed.

    Returns:
        The path of the directory created (or that already existed).
    """
    logger.debug("Creating directory %r.", path)
    path.mkdir(parents=True, exist_ok=True)
    if clean_if_exists:
        # This is a best effort attempt. Hence we are ignoring errors wherever we can.
        for p in path.iterdir():
            if p.is_dir():
                logger.debug("Removing directory %r.", p)
                shutil.rmtree(p, ignore_errors=True)
            else:
                logger.debug("Removing file %r.", p)
                with suppress(OSError):
                    p.unlink(missing_ok=True)
    return path
