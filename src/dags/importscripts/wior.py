import json
import logging

logger = logging.getLogger(__name__)


def _pick_branch(file: str) -> str:
    """Select next step to be executed based on existence of duplicate records.

    Args:
        file: path to .geojson source file.

    Returns:
        string containing name of desired step to execute next.
    """
    with open(file) as f:

        logger.info("reading file: %d", file)
        data = json.load(f)

        ids = []

        for row in data["features"]:
            _id = row["properties"]["_instance_id"]

            if _id in ids:
                logger.info("Duplicate found!: %d", _id)
                return "remove_duplicate_rows"
            else:
                ids.append(_id)

        logger.info("No duplicate found!: Resuming happy flow")
        return "no_duplicates_found"
