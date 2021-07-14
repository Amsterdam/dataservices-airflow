import json
from typing import Final
from urllib.parse import ParseResult, urlparse

from airflow.hooks.http_hook import HttpHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


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
        http_conn_id: str = "api_data_amsterdam_conn_id",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_table = source_table
        self.source_location_column = source_location_column
        self.source_key_column = source_key_column
        self.geometry_column = geometry_column
        self.postgres_conn_id = postgres_conn_id
        self.http_conn_id = http_conn_id

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
                    WHERE {self.geometry_column} is NULL
                """
            )

            rows = cursor.fetchall()

        return rows

    def prepare_typeahead_call(self, record):
        """Prep typeahead service call for non-geometry location data"""

        # setup result list
        typeadhead_result = {}

        # Make dictionary out of list, so it's easier to query later.
        record_dict = {}
        record_dict[self.source_location_column] = location_value = record[0]
        record_dict[self.source_key_column] = key_value = record[1]

        # Call the typeahead service with params
        data_headers = {"content-type": "application/json"}
        http_endpoint = f"/atlas/typeahead/bag/?q={location_value}"
        http_response = self.get_typeahead_result(http_endpoint, data_headers)
        http_data = None

        try:
            http_data = json.loads(http_response.text)[0]["content"][0]["uri"]
            self.log.info(f"BAG id found for {location_value}.")

        except IndexError:
            self.log.error(f"Attempt 1: No BAG id found for {location_value}, empty result...")

        # TODO: find better solution
        # If no match, try matching only on the first number of
        # an address (i.e. 'street 1-100 (hiven is invalid input) => street 1')
        if not http_data:
            location_value = location_value.split("-")[0]
            http_endpoint = f"/atlas/typeahead/bag/?q={location_value}"
            http_response = self.get_typeahead_result(http_endpoint, data_headers)

            try:
                http_data = json.loads(http_response.text)[0]["content"][0]["uri"]
                self.log.info(f"BAG id found for {location_value}.")

            except IndexError:
                self.log.error(f"Attempt 2: No BAG id found for {location_value}, empty result...")

        typeadhead_result[key_value] = http_data

        return typeadhead_result

    def get_typeahead_result(self, http_endpoint, data_headers):
        """Look up BAG verblijfsobject id from typeahead service for non-geometry location data"""

        http = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        http_response = http.run(endpoint=http_endpoint, data=None, headers=data_headers)

        return http_response

    def execute(self, context=None):
        """look up the geometry where no geometry is present"""

        # get location data without geometry
        rows = self.get_non_geometry_records()

        # get BAG verblijfsobject ID from typeahead
        for record in rows:

            get_typeadhead_result = self.prepare_typeahead_call(record)
            record_key = None
            bag_url = None
            for key, value in get_typeadhead_result.items():
                record_key = key
                bag_url = value

            # extract the BAG id from the url, which is the last
            # series of numbers before the last forward-slash
            try:
                get_uri = urlparse(bag_url)
                if not isinstance(get_uri, ParseResult):
                    self.log.info(f"No BAG id found for {record}")
                    continue
                else:
                    bag_id = get_uri.path.rsplit("/")[-2]
                    self.log.info(f"BAG id found for {record_key}: {bag_id}")

            except AttributeError:
                self.log.error(
                    f"No BAG id found for {record_key} {bag_id} {bag_url}, empty result..."
                )
                continue

            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            with pg_hook.get_cursor() as cursor:

                # update record with found geometry
                cursor.execute(
                    f"""
                        WITH BAG_VBO_GEOM AS (
                        SELECT geometrie
                        FROM public.bag_verblijfsobjecten
                        WHERE identificatie = %s
                        )
                        UPDATE {self.source_table}
                        SET {self.geometry_column} = BAG_VBO_GEOM.geometrie
                        FROM BAG_VBO_GEOM
                        WHERE {self.source_key_column} = %s;
                        COMMIT;
                        """,
                    (
                        bag_id,
                        record_key,
                    ),
                )
