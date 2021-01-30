import ccsv as csv
import json
import sys
import hashlib
from collections import namedtuple
from collections import defaultdict
from shapely import wkt
from shapely.geometry import (
    GeometryCollection,
    MultiLineString,
    LineString,
    MultiPolygon,
    Polygon,
)


# set max number of karakters per cell
csv.field_size_limit(sys.maxsize)

# ----------------------------#
# helper functions
# ----------------------------#


def get_dialect(file: str):
    """determine dialect of file"""
    with open(file) as data:
        return csv.Sniffer().sniff(data.readline())


def get_header(file: str, extra_fields_to_add: list = None):
    """retreive header from file"""
    with open(file) as data:
        reader = csv.reader(data, dialect=get_dialect(file))
        # reader = csv.reader(data, delimiter="|")
        header = list(map(lambda s: s.lower(), next(reader)))
        if extra_fields_to_add:
            for col in extra_fields_to_add:
                header.append(col)
        return header


def create_named_tuple(file: str, class_name: str, header: list = None):
    """create named tuple based upon header file"""
    if not header:
        header = get_header(file)
    return namedtuple(class_name, header)


def file_parser(file: str):
    """generate data from file"""
    with open(file) as data:
        yield from csv.reader(data, dialect=get_dialect(file))


def file_iter(file: str, name_named_tuple: str, header: list = None):
    """retrieve data of file into named tuple"""
    named_tuple = create_named_tuple(file, name_named_tuple, header)
    reader = file_parser(file)
    next(reader)  # skip header, just get data
    for row in reader:
        yield named_tuple(*row)


def filter_rows(row, filter: dict = None):
    """return true on rows that meet criterium"""
    FILTER_OPTIONS = {
        "LIKE": "in",
        "NOT LIKE": "not in",
        "EQUALS": "==",
        "NOT EQUALS": "!=",
        "GT": ">",
        "LT": "<",
        "GTE": ">=",
        "LTE": "<=",
        "STARTS_WITH": "startswith",
    }
    if not filter:
        return True

    filter_type = FILTER_OPTIONS[filter["filter_type"].upper()]

    if filter_type == "startswith":
        if eval(
            f"'{getattr(row, filter['filter_column'])}'.{filter_type}('{filter['filter_value']}')"
        ):
            return True
    else:
        if eval(
            f"'{filter['filter_value'].upper()}' {filter_type} '{getattr(row, filter['filter_column'])}'"
        ):
            return True


def unique_row_id(values: list):
    """returns unique value out of a list based upon sha1 hash.
    can be used to give a row an unqiue identifier (if not present or needed).
    """
    hash = hashlib.sha1()
    hash.update(bytes("".join(values), "utf-8"))
    return hash.hexdigest()


def save_file(output_file: str, header: list, rows: list):
    """saves file to csv with given header and data rows"""
    with open(output_file, "w") as f:
        write = csv.writer(f, delimiter="|", quotechar='"')
        write.writerow(header)
        write.writerows(rows)


# ----------------------------#
# Merging data logic
# ----------------------------#


def merge_files_parser(
    file_target: str,
    name_target: str,
    file_source: str,
    name_source: str,
    name_join_key: str,
    source_fields_names: list,
    target_fields_names: list,
    source_filter: dict = None,
    target_filter: dict = None,
):
    """generate all data from target file and extend it with selected data from source file"""
    target_file = file_iter(file_target, name_target)
    source_file = file_iter(file_source, name_source)
    merge_header = get_header(file_target, target_fields_names)
    merge_named_tuple = create_named_tuple(file_target, name_target, merge_header)

    # retrieve all relevant data from source file, to append to target file
    # used defaultdict to group element on one key (== name_join_key)
    source_data_to_merge = defaultdict(list)
    for row in source_file:
        if filter_rows(row, source_filter):
            for field in source_fields_names:
                # To avoid the number of values exceeds the number of columns to accomodate these values.
                # This happens when the value of name_join_key is found more then once in the same file
                # i.e. dmb_lpg_afleverzuil.csv
                if len(source_data_to_merge[getattr(row, name_join_key)]) >= len(
                    source_fields_names
                ):
                    continue

                source_data_to_merge[getattr(row, name_join_key)].append(getattr(row, field, None))

    # append data from source file to target file based upon mutual key
    for row in target_file:
        if filter_rows(row, target_filter):
            if source_data_to_merge[getattr(row, name_join_key)]:
                yield merge_named_tuple(
                    *row + tuple(source_data_to_merge[getattr(row, name_join_key)])
                )


def merge_files_iter(
    target_file_name: str,
    target_file: str,
    source_file_name: str,
    source_file: str,
    mutual_key: str,
    map_source_field_to_target: dict,
    output_file: str = None,
    source_filter: dict = None,
    target_filter: dict = None,
):
    """
    call the merge file generator and save output
    """
    source_fields_names = [value for value in json.loads(map_source_field_to_target).keys()]
    target_fields_names = [value for value in json.loads(map_source_field_to_target).values()]
    header = get_header(target_file, target_fields_names)
    rows = [
        list(i)
        for i in merge_files_parser(
            target_file,
            target_file_name,
            source_file,
            source_file_name,
            mutual_key,
            source_fields_names,
            target_fields_names,
            source_filter,
            target_filter,
        )
    ]
    output_file = output_file if output_file else target_file
    save_file(output_file, header, rows)


# ----------------------------#
# Union data logic
# ----------------------------#


def union_files_parser(
    target_file: str,
    target_name: str,
    source_file: list,
    source_file_content_type: list,
    source_file_content_column: list,
    source_file_dir_path: str,
    id_column: str,
    header: list,
    row_unique_cols: list,
):
    """generate all data from source files and concat it's content in one target file """
    if source_file_content_type:
        # add extra field to header, based on source_file_content_column, to store content type
        target = create_named_tuple(
            f"{source_file_dir_path}/{source_file[0]}",
            target_name,
            get_header(f"{source_file_dir_path}/{source_file[0]}", source_file_content_column),
        )

    else:
        target = create_named_tuple(f"{source_file_dir_path}/{source_file[0]}", target_name)

    # with a union, to avoid dubplicate key errors,
    # it is needed to create an unique value for the record identifcation field
    unique_row_value = []
    row_unique_cols = row_unique_cols if row_unique_cols else [id_column]
    for file, type_ in zip(source_file, source_file_content_type):
        reader = file_iter(f"{source_file_dir_path}/{file}", "source")
        for row in reader:
            for col in row_unique_cols:
                unique_row_value.append(getattr(row, col))
            row_id = unique_row_id(unique_row_value)
            row = eval(f"row._replace({id_column}='{row_id}')")
            yield target(*row + tuple(type_.split()))


def union_files_iter(
    target_name: str,
    target_file: str,
    source_file: tuple,
    source_file_dir_path: str,
    id_column: str = "id",
    source_file_content_type: list = None,
    source_file_content_column: list = None,
    output_file: str = None,
    row_unique_cols: list = None,
):
    """
    call the union file generator and save output
    """
    header = get_header(f"{source_file_dir_path}/{source_file[0]}", source_file_content_column)
    rows = [
        list(i)
        for i in union_files_parser(
            target_file,
            target_name,
            source_file,
            source_file_content_type,
            source_file_content_column,
            source_file_dir_path,
            id_column,
            header,
            row_unique_cols,
        )
    ]
    output_file = output_file if output_file else target_file
    save_file(output_file, header, rows)


# ----------------------------#
# Cleansing data logic
# ----------------------------#


def cleanse_misformed_data_parser(
    source_name: str,
    source_file: str,
    header: list,
    row_unique_cols: list,
    id_column: str = "id",
    output_file: str = None,
):
    """
    generate cleaned up data from file
    For instance: header in camelcase, no unique key present, etc.
    """
    data = file_iter(source_file, source_name, header)
    target = create_named_tuple(source_file, source_name, header)

    for row in data:
        unique_row_value = []
        for col in row_unique_cols:
            unique_row_value.append(getattr(row, col))
        # setup unique key by hashing values of unique cols combination
        row_id = unique_row_id(unique_row_value)
        row = eval(f"row._replace({id_column}='{row_id}')")
        yield target(*row)


def cleanse_misformed_data_iter(
    source_name: str,
    source_file: str,
    row_unique_cols: list,
    id_column: str = "id",
    output_file: str = None,
    extra_cols: list = None,
):
    """call the cleanse file generator and save output"""

    header = get_header(source_file, extra_cols)
    rows = [
        list(i)
        for i in cleanse_misformed_data_parser(
            source_name, source_file, header, row_unique_cols, id_column, output_file
        )
    ]
    output_file = output_file if output_file else source_file
    save_file(output_file, header, rows)


# ----------------------------#
# Unifying geometry data logic
# ----------------------------#


def unify_geometry_data_parser(
    source_name: str,
    source_file: str,
    geom_data_type_to_use: str,
    geom_column: str = "geometrie",
):
    """generate translated geometry data.
    It translates GeometryCollection or single geom datatype to prefered multi geom datatype i.e. LineString to MultiLineString
    """

    for row in file_iter(source_file, source_name):
        geom = wkt.loads(getattr(row, geom_column))
        result = None
        elements = []
        # geom is a collection; break it up
        if isinstance(geom, GeometryCollection):
            if geom_data_type_to_use == "MultiLineString":
                for element in geom:
                    if isinstance(element, LineString):
                        elements.append(element.coords)

                result = MultiLineString(elements)

            if geom_data_type_to_use == "MultiPolygon":
                for element in geom:
                    if isinstance(element, Polygon):
                        elements.append(element)

                result = MultiPolygon(elements)

            row = eval(f"row._replace({geom_column}='{result}')")

        # geom is not a collection
        elif not isinstance(geom, eval(geom_data_type_to_use)):
            if isinstance(geom, LineString):
                elements.append(geom.coords)
                result = MultiLineString(elements)

            if isinstance(geom, Polygon):
                elements.append(geom)
                result = MultiPolygon(elements)

            row = eval(f"row._replace({geom_column}='{result}')")

        # geom type equals the geom_data_type_to_use
        else:
            row = eval(f"row._replace({geom_column}='{row.geometrie}')")

        yield row


def unify_geometry_data_iter(
    source_name: str,
    source_file: str,
    geom_data_type_to_use: str,
    geom_column: str = "geometrie",
    output_file: str = None,
):
    """call the geometry generator and save output"""

    header = get_header(source_file)
    rows = [
        row
        for row in unify_geometry_data_parser(
            source_name, source_file, geom_data_type_to_use, geom_column
        )
    ]
    output_file = output_file if output_file else source_file
    save_file(output_file, header, rows)
