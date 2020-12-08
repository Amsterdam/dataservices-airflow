import requests
import re

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class TypeAHeadLocationOperator(BaseOperator):
    """
    If a source doesn't contains geometry data, but does have other location data, such as
    an address then this is used to find the geometry object using the typeahead service.

    The typeadhead services uses the BAG (basisregistratie adressen en gebouwen) for
    a match.
    The typeahead service is currently also used to complement user search keyword input at
    data.amsterdam.nl.

    This operator will feed the typeahead service with the non-geometry location data from a
    database table column source, and looks up the geometry in BAG 'verblijfsobject'.

    If the non-geometry location results in more then one hit, i.e. because an address without
    a (house) number is given, it will take the first hit.
    Finally, the geometry result is stored in the geometry column in the source table.

    Note:
    The operator acts on the assumption that a geometry type columm already exists in the table.
    Make sure the geometry column is created before running this operator.
    The default name of this column is 'geometry' and can be overwritten if needed.
    """

    @apply_defaults
    def __init__(
        self,
        source_table: str,
        source_location_column: str,
        source_key_column: str = "id",
        geometry_column: str = "geometry",
        postgres_conn_id: str = "postgres_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_table = source_table
        self.source_location_column = source_location_column
        self.source_key_column = source_key_column
        self.geometry_column = geometry_column
        self.postgres_conn_id = postgres_conn_id

    def get_non_geometry_records(self):
        """get location values from table (for record with no geometry)"""

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        with pg_hook.get_cursor() as cursor:

            cursor.execute(
                f"""
                    SELECT
                        {self.source_location_column}
                    ,   {self.source_key_column}
                    FROM {self.source_table}
                    WHERE 1=1
                    AND {self.geometry_column} is NULL
                """
            )

            rows = cursor.fetchall()

        return rows

    def get_typeahead_result(self, record):
        """Look up BAG verblijfsobject id from typeahead service for non-geometry location data"""

        # setup result list
        typeadhead_result = {}

        # Make dictionary out of list, so it's easier to query later.
        record_dict = {}
        record_dict[self.source_location_column] = record[0]
        record_dict[self.source_key_column] = record[1]

        # Alias entries in the record_dict for easy use
        key_value = record_dict[self.source_key_column]
        location_value = record_dict[self.source_location_column]

        # Feed typeahead with location data
        data_headers = {"content-type": "application/json"}
        data_url = f"https://api.data.amsterdam.nl/atlas/typeahead/bag/?q={location_value}"
        data_response = requests.get(data_url, headers=data_headers, verify=False)

        # TODO: find better solution
        # If no match, try matching only on the first number of
        # an address (i.e. 'street 1-100 => street 1')
        if not data_response.json():
            location_value = location_value.split("-")[0]
            data_url = f"https://api.data.amsterdam.nl/atlas/typeahead/bag/?q={location_value}"
            data_response = requests.get(data_url, headers=data_headers, verify=False)

        self.log.error(
            f"typeahead status {data_response} "
            f"gets result {data_response.json()} "
            f"for value {location_value}"
        )

        # Get the first result, if there are multiple responses
        # i.e. address without number or addition leads to more than one result
        try:
            typeadhead_result[key_value] = data_response.json()[0]["content"][0] or None

        except IndexError:
            self.log.error(f"No results for {location_value}")

        return typeadhead_result

    def execute(self, context=None):
        """look up the geometry where no geometry is present"""

        # get location data without geometry
        rows = self.get_non_geometry_records()

        # get BAG verblijfsobject ID from typeahead
        for record in rows:

            get_typeadhead_result = self.get_typeahead_result(record)
            record_key = None
            bag_url = None
            for key, value in get_typeadhead_result.items():
                record_key = key
                bag_url = value

            # extract the BAG id from the url, which is the last
            # series of numbers before the last forward-slash
            try:
                bag_id = re.search("(/)([0-9]+)(/)$", bag_url["uri"]).group(2)
                self.log.error(f"BAG id found for {record_key}: {bag_id}")

            except AttributeError:
                self.log.error(f"No BAG id found for {record_key}, empty result...")
                continue

            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            with pg_hook.get_cursor() as cursor:

                # update record with found geometry
                cursor.execute(
                    f"""
                        WITH BAG_VBO_GEOM AS (
                        SELECT geometrie
                        FROM public.baggob_verblijfsobjecten
                        WHERE 1=1
                        AND identificatie = '{bag_id}'
                        )
                        UPDATE {self.source_table}
                        SET {self.geometry_column} = BAG_VBO_GEOM.geometrie
                        FROM BAG_VBO_GEOM
                        WHERE 1=1
                        AND {self.source_key_column} = '{record_key}';
                        COMMIT;
                        """
                )
