import csv
from hashlib import blake2s
import json
import logging
from typing import List, Tuple, TypedDict, Generator, Type


# define logger for output to console
logger = logging.getLogger(__name__)


def get_dialect(file: str) -> Type:
    """determine dialect of file

    Args:
        file: the path/to/file.ext that will be used to determine it's dialect

    Returns:
        A subclass dialect of Sniffer

    """
    with open(file) as data:
        return csv.Sniffer().sniff(data.readline())


def get_header(file: str) -> List:
    """Return header row of file

    Args:
        file: the path/to/file.ext that will be used to determine it's header

    Returns:
        a list of columns in string

    """
    dialect = get_dialect(file)
    with open(file, "r") as src:
        data = csv.reader(src, dialect=dialect)
        return next(data)


def make_hash(composite_values: List, digest_size: int = None) -> int:
    """The blake2s algorithm is used to generate a single hased value for source
    composite values that uniquely identify a row.
    In case the source itself doesn't provide a single solid identification as key.

    Args:
        composite_values: a concatenated string of record values based on
                        which columns unqiuely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Returns:
        a hased value of the composite values in int

    Note:
        The default digest size is for now set to 3 bytes, which is equivant
        to ~ 10 karakters long.
        Because the id column is preferably of type int, the hased value is converted
        from hex to int.
    """
    digest_size = 3 if not digest_size else digest_size
    return int(
        blake2s("|".join(composite_values).encode(), digest_size=digest_size).hexdigest(), base=16
    )


def read_data(file: str, composite_key: Tuple) -> Generator:
    """Read the data from csv file and contain it into a data class

    Args:
        file: the path/to/file.ext that will be read
        composite_key: Tuple of column name(s) that unqiuely identifiy a record

    Returns:
        a generator containing the rows of the file plus it's header and the defined composite key

    """
    dialect = get_dialect(file)
    header = get_header(file)
    # setup the generic TypeDict data mold
    DataObject = TypedDict("DataObject", {col: str for col in header})
    with open(file, "r") as src:
        data = csv.reader(src, dialect=dialect)
        for line in data:
            # construct lines into dictionary format, to proces into the generic data mold
            row_dict = {i[0]: i[1] for i in zip(header, line)}
            row_object = dict(object_=DataObject(row_dict), composite_key=composite_key)
            yield row_object


def generate_unique_id(file: str, composite_key: Tuple, digest_size: int = None) -> Generator:
    """Generate unique hased id from composite values

    Args:
        file: the path/to/file.ext that will be read
        composite_key: Tuple of column name(s) that unqiuely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Returns:
        a generator containing the rows plus the unique ID value for the added ID column

    """
    for row in read_data(file, composite_key):
        composite_values = []
        for column in row["composite_key"]:
            composite_values.append(row["object_"].get(column, None))
        try:
            row["object_"]["id"] = make_hash(composite_values, digest_size=digest_size)
            yield row["object_"]
        except TypeError as err:
            logger.error("%s cannot be retrieved from %s: %s", column, file, err)
            continue


def add_unique_id_to_csv(file: str, composite_key: Tuple, digest_size: int = None) -> None:
    """save enriched data to csv for further processing in DAG

    Args:
        file: the path/to/file.ext that will be read
        composite_key: Tuple of column name(s) that unqiuely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Does:
        saves data rows incl the added new ID column back to the *csv* file

    """
    data = generate_unique_id(file, composite_key=composite_key)
    header = next(data)  # get the modified header including the new id column
    rows = list()
    for row in data:
        rows.append(row.values())
    with open(file, "w") as f:
        write = csv.writer(f, dialect=csv.unix_dialect)
        write.writerow(header)
        write.writerows(rows)


def add_unique_id_to_geojson(file: str, composite_key: Tuple, digest_size: int = None) -> None:
    """save enriched data to geojson for further processing in DAG

    Args:
        file: the path/to/file.ext that will be read
        composite_key: Tuple of column name(s) that unqiuely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Does:
        saves data rows incl the added new ID column back to the *geojson* file

    """
    with open(file, "r+") as f:
        data = json.load(f)
        for rows in data["features"]:
            key_values = []
            for key in composite_key:
                # openbareruimte geojson has a different structure
                if "openbaresportplek" in file:
                    key_values.append(rows[key])
                else:
                    key_values.append(rows["properties"][key])
                rows["properties"]["id"] = make_hash(["".join(str(key_values))])
        f.seek(0)  # set reader back to line 0
        f.truncate()
        f.write(json.dumps(data))
