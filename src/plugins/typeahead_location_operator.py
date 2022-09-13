import json
from typing import Any, Optional
from urllib.parse import ParseResult, urlparse

from airflow.hooks.http_hook import HttpHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from postgres_on_azure_hook import PostgresOnAzureHook
from psycopg2 import sql


class TypeAHeadLocationOperator(BaseOperator):
    """TypeAHeadLocationOperator.

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

    @apply_defaults  # type: ignore
    def __init__(
        self,
        source_table: str,
        source_location_column: str,
        dataset_name: Optional[str] = None,
        source_key_column: str = "id",
        geometry_column: str = "geometry",
        postgres_conn_id: str = "postgres_default",
        http_conn_id: str = "api_data_amsterdam_conn_id",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize.

        Args:
           dataset_name: Name of the dataset as known in the Amsterdam schema.
               Since the DAG name can be different from the dataset name, the latter
               can be explicity given. Only applicable for Azure referentie db connection.
               Defaults to None. If None, it will use the execution context to get the
               DAG id as surrogate. Assuming that the DAG id equals the dataset name
               as defined in Amsterdam schema. It is used to setup the datbase user for
               the connection to the database.
            source_table: Name of the table to run the address to geometry lookup on.
            source_location_column: Name of the column to run the lookup for an address to
                geometry.
            source_key_column: Name of the key column, used to update the correct row.
            geometry_column: The target column to add the found geometry to.
            postgres_conn_id: Connection name to use to setup the database connection.
                Defaults to `postgres_default`.
            http_conn_id: Connection name to use for the API base URL.
                Defaults to `api_data_amsterdam_conn_id`.

        """
        super().__init__(*args, **kwargs)
        self.source_table = source_table
        self.source_location_column = source_location_column
        self.source_key_column = source_key_column
        self.geometry_column = geometry_column
        self.postgres_conn_id = postgres_conn_id
        self.http_conn_id = http_conn_id
        self.dataset_name = dataset_name

    def get_non_geometry_records(self, context: Context) -> Any:
        """Get location values from table (for record with no geometry)."""
        pg_hook = PostgresOnAzureHook(
            dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id
        )

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

    def prepare_typeahead_call(self, record: list) -> dict[str, Optional[Any]]:
        """Prep typeahead service call for non-geometry location data.

        Args:
            record: A single row in the source table to be apply the
                lookup from address to geometry on.

        """
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
            self.log.info("BAG id found for %s", location_value)

        except IndexError:
            self.log.error("Attempt 1: No BAG id found for %s, empty result...", location_value)

        # TODO: find better solution
        # If no match, try matching only on the first number of
        # an address (i.e. 'street 1-100 (hiven is invalid input) => street 1')
        if not http_data:
            location_value = location_value.split("-")[0]
            http_endpoint = f"/atlas/typeahead/bag/?q={location_value}"
            http_response = self.get_typeahead_result(http_endpoint, data_headers)

            try:
                http_data = json.loads(http_response.text)[0]["content"][0]["uri"]
                self.log.info("BAG id found for %s", location_value)

            except IndexError:
                self.log.error(
                    "Attempt 2: No BAG id found for %s, empty result...", location_value
                )

        typeadhead_result[key_value] = http_data

        return typeadhead_result

    def get_typeahead_result(self, http_endpoint: str, data_headers: dict[str, str]) -> Any:
        """Look up BAG verblijfsobject id from typeahead service for non-geometry location data.

        Args:
            http_endpoint: The endpoint of the API to use for the lookup. In this case
                the `typeahead` API: https://api.data.amsterdam.nl/atlas/typeahead/bag/?q
            data_headers: The API headers to add when calling it.
        """
        http = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        http_response = http.run(endpoint=http_endpoint, data=None, headers=data_headers)

        return http_response

    def execute(self, context: Context) -> None:
        """Look up the geometry based on address if no geometry is present."""
        # get location data without geometry
        rows = self.get_non_geometry_records(context=context)

        # get BAG verblijfsobject ID from typeahead
        for record in rows:

            get_typeadhead_result = self.prepare_typeahead_call(record)
            record_key = None
            bag_url = None
            bag_id = None

            for key, value in get_typeadhead_result.items():
                record_key = key
                bag_url = value

                # extract the BAG id from the url, which is the last
                # series of numbers before the last forward-slash
                try:
                    if bag_url:
                        get_uri = urlparse(bag_url)
                    if not isinstance(get_uri, ParseResult):
                        self.log.info("No BAG id found for %s", record)
                    else:
                        bag_id = get_uri.path.rsplit("/")[-2]
                        self.log.info("BAG id found for %s:%s", record_key, bag_id)

                except AttributeError:
                    self.log.error(
                        "No BAG id found for %s %s %s, empty result...",
                        record_key,
                        bag_id,
                        bag_url,
                    )
                    continue

            pg_hook = PostgresOnAzureHook(
                dataset_name=self.dataset_name,
                context=context,
                postgres_conn_id=self.postgres_conn_id,
            )

            with pg_hook.get_cursor() as cursor:

                # update record with found geometry
                if bag_url is not None:
                    cursor.execute(
                        sql.SQL(
                            """
                            WITH BAG_VBO_GEOM AS (
                            SELECT geometrie
                            FROM public.bag_verblijfsobjecten
                            WHERE identificatie = %(bag_id)s
                            )
                            UPDATE {source_table}
                            SET {geometry_column} = BAG_VBO_GEOM.geometrie
                            FROM BAG_VBO_GEOM
                            WHERE {source_key_column} = %(record_key)s;
                            COMMIT;
                            """
                        ).format(
                            source_table=sql.Identifier(self.source_table),
                            geometry_column=sql.Identifier(self.geometry_column),
                            source_key_column=sql.Identifier(self.source_key_column),
                        ),
                        {"bag_id": bag_id, "record_key": record_key},
                    )

                # TEMPORARY: Final attempt to update the source data with geometry
                # based on the BAG tables in the DB itself. Bypassing the
                # use of the typahead API. Since the API cannot cope with
                # huisnummer larger then 3 digits. See:
                # https://api.data.amsterdam.nl/atlas/typeahead/bag/?q=Gustav%20Mahlerlaan%202920
                # TODO: Adjust the typeahead API in this repo:
                # https://github.com/Amsterdam/bag_services/
                if bag_url is None:
                    self.log.info(
                        "Final attempt: No BAG id found for %s, \
                        attempt by query the BAG tables directly,\
                        bypassing the /atlas/typeahead/bag API...",
                        record,
                    )

                    record = record[0].split(" ")
                    index_last_value = len(record) - 1
                    adres = " ".join(record[:index_last_value])
                    huisnummer = "".join(record[index_last_value : index_last_value + 1])

                    self.log.info("Value for `adres` to use in DB query = %s", adres)
                    self.log.info("Value for `huisnummer` to use in DB query = %s", huisnummer)

                    cursor.execute(
                        sql.SQL(
                            """
                            WITH BAG_VBO_GEOM_FULL_SEARCH AS (
                            SELECT DISTINCT bv.geometrie
                            FROM public.bag_verblijfsobjecten bv
                            INNER JOIN
                                public.bag_nummeraanduidingen_adresseert_verblijfsobject bnav
                                ON bnav.adresseert_verblijfsobject_identificatie = bv.identificatie
                            INNER JOIN
                                public.bag_nummeraanduidingen bn
                                ON bn.id = bnav.nummeraanduidingen_id
                            INNER JOIN public.bag_openbareruimtes bo
                                ON bo.id = bn.ligt_aan_openbareruimte_id
                            WHERE bo.naam_nen = %(adres)s
                            AND bn.huisnummer =  %(huisnummer)s
                            )
                            UPDATE {source_table}
                            SET  {geometry_column} = BAG_VBO_GEOM_FULL_SEARCH.geometrie
                            FROM BAG_VBO_GEOM_FULL_SEARCH
                            WHERE  {source_key_column} = %(record_key)s;
                            COMMIT;
                            """
                        ).format(
                            source_table=sql.Identifier(self.source_table),
                            geometry_column=sql.Identifier(self.geometry_column),
                            source_key_column=sql.Identifier(self.source_key_column),
                        ),
                        {"adres": adres, "huisnummer": huisnummer, "record_key": record_key},
                    )
