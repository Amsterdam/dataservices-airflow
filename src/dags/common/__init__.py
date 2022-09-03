import logging
from datetime import timedelta
from hashlib import blake2s
from typing import Iterable, cast

import pendulum
from airflow.models.taskinstance import Context
from airflow.settings import TIMEZONE
from environs import Env
from log_message_operator import LogMessageOperator
from psycopg2.extensions import QuotedString
from slack_message_operator import SlackMessageOperator

env: Env = Env()

# define logger for output to console
logger: logging.Logger = logging.getLogger(__name__)

# the DATAPUNT_ENVIRONMENT is set in CloudVPS (Openstack repo) for
# environment `acceptance` or `production`. These values are also
# used in some dags to refer to a folder on the objectstore as such.
# Therefor we will leave this one exists for now. When fully migrating
# to Azure, we can modify the set of `DATASTORE_TYPE` and
# `MessageOperator` see below.
DATAPUNT_ENVIRONMENT: str = env("DATAPUNT_ENVIRONMENT", "acceptance")

# For slack messages and callback function (see callbacks.py) we use
# the variable to indicate where execution takes place.
# Since we have to deal with the hybride situation of CloudVPS and Azure
# we for now still need to account for DATAPUNT_ENVIRONMENT as value.
# When fully migrated to Azure we can remove the second entry.
OTAP_ENVIRONMENT: str = env("AZURE_OTAP_ENVIRONMENT", env("DATAPUNT_ENVIRONMENT", "acceptance"))

# Defines the environments in which sending of email is enabled.
# After we are fully migrated to Azure, we can remove the list item
# `production` and just leave `prd`.
ELIGIBLE_EMAIL_ENVIRONMENTS: tuple[str, ...] = tuple(
    cast(
        Iterable[str],
        map(lambda s: s.strip(), env.list("ELIGIBLE_EMAIL_ENVIRONMENTS", ["production", "prd"])),
    )
)

# used for storing temporary files like when downloading.
SHARED_DIR: str = env("SHARED_DIR", "/tmp")  # noqa: S108

# defines the an ephermeral directory used on an AKS pod non-mounted as a share.
EPHEMERAL_DIR: str = env("EPHEMERAL_DIR", "/scratch-volume")  # noqa: S108

DATASTORE_TYPE: str = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)


MessageOperator = (
    LogMessageOperator if DATAPUNT_ENVIRONMENT == "development" else SlackMessageOperator
)


default_args: Context = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": pendulum.yesterday(TIMEZONE),
    "email": "example@airflow.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": SlackMessageOperator(task_id="error").slack_failed_task,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,  # do not backfill
}

vsd_default_args: Context = default_args.copy()
vsd_default_args["postgres_conn_id"] = "postgres_default"


def addloopvariables(iterable: Iterable) -> Iterable:
    """Pass through all values from the given iterable.

    Return items of the iterable and booleans
    signalling the first and the last item

    Args:
        iterable:
            iterable that holds values

    Yields:
        current item with indication first of last
    """
    # Get an iterator and pull the first value.
    it = iter(iterable)
    current = next(it)
    yield current, True, False
    current = next(it)
    # Run the iterator to exhaustion (starting from the third value).
    for val in it:
        # Report the *previous* value (more to come).
        yield current, False, False
        current = val
    # Report the last value.
    yield current, False, True


def quote_string(val: str) -> str:
    """Return properly quoted and escaped string value for usage in PostgreSQL SQL queries.

    Return the string enclosed in single quotes.
    Any single quote appearing in the string
    is escaped by doubling it according to SQL string constants syntax.
    Backslashes are escaped too.

    Args:
        val: string value to be quoted and escaped

    Returns:
        Escaped and quoted string value.

    """
    # This function uses extension functions from Pyscopg2. Normally Pscycopg2 has a
    # connection (to a DB) it can derive the encoding from. Here we are using the functions
    # without a connection present. Hence Psycopg2 uses the default encoding of latin-1.
    # Rather then modifying the encoding globally to utf-8 and causing unintended side-effects,
    # we accept the default encoding and encode and decode our utf-8 string accordingly.
    return cast(str, QuotedString(val.encode()).getquoted().decode())


def make_hash(composite_values: list[str], digest_size: int = 5) -> int:
    """Construct a hash value.

    The blake2s algorithm is used to generate a single hased value for source
    composite values that uniquely identify a row.
    In case the source itself doesn't provide a single solid identification as key.

    Args:
        composite_values: a list of record values based on
                        which columns uniquely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Returns:
        a hased value of the composite values in int

    Note:
        The default digest size is for now set to 5 bytes, which is equivalent
        to ~ 10 karakters long.
        Because the id column is preferably of type int, the hased value is converted
        from hex to int.
    """
    return int.from_bytes(
        blake2s("|".join(composite_values).encode(), digest_size=digest_size).digest(),
        byteorder="little",
    )
