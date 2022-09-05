from collections import defaultdict
from typing import Any, Final, Iterable, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import Context
from postgres_on_azure_hook import PostgresOnAzureHook
from airflow.utils.decorators import apply_defaults
from environs import Env
from more_ds.network.url import URL
from psycopg2 import sql
from schematools.utils import dataset_schema_from_url, to_snake_case, toCamelCase

env = Env()
SCHEMA_URL: Final = URL(env("SCHEMA_URL"))


class ProvenanceRenameOperator(BaseOperator):
    """Rename columns and indices according to the provenance field in the schema."""

    @apply_defaults  # type: ignore [misc]
    def __init__(  # noqa: D107
        self,
        dataset_name: str,
        pg_schema: str = "public",
        postgres_conn_id: str = "postgres_default",
        rename_indexes: bool = False,
        prefix_table_name: str = "",
        postfix_table_name: str = "",
        subset_tables: Optional[list] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dataset_name = dataset_name
        self.pg_schema = pg_schema
        self.rename_indexes = rename_indexes
        # The table to enforce the provenance translations is defined by
        # the table ID in the schema definition. If the provenance translations
        # must be applied on a temp table name i.e. 'spoorlijnen_metro_new'. Then specify
        # the prefix (i.e. spoorlijnen_) and postfix (i.e. _new) when calling this operator.
        self.prefix_table_name = prefix_table_name
        self.postfix_table_name = postfix_table_name
        # set table name for getting provenance for specific table
        self.subset_tables = subset_tables

    def _get_existing_tables(
        self, pg_hook: PostgresOnAzureHook, tables: list, pg_schema: str = "public"
    ) -> dict[str, Any]:
        """Looks up the table name in schema.

        Taking into account the provenance (it can contain the orginal (real) name) and relates
        them to existing table in database.

        Args:
            pg_hook: Postgres connection
            tables: list of table names of type string
            pg_schema: name of database schema where table are located

        Return:
            dictionary of tables as objects
        """
        if not tables:
            return {}

        if self.subset_tables:
            subset_tables = [toCamelCase(table) for table in self.subset_tables]
            tables = [table for table in tables if table["id"] in subset_tables]

        table_lookup = {}

        for table in tables:
            real_tablename = table.get(
                "provenance",
                self.prefix_table_name + table.id + self.postfix_table_name,
            )
            table_lookup[to_snake_case(real_tablename)] = table

        if not table_lookup:
            return {}

        rows = pg_hook.get_records(
            """
            SELECT tablename FROM pg_tables
             WHERE schemaname = %(schema)s
               AND tablename IN %(tables)s
        """,
            {"schema": pg_schema, "tables": tuple(table_lookup.keys())},
        )

        return {row["tablename"]: table_lookup[row["tablename"]] for row in rows}

    def _get_existing_columns(
        self, pg_hook: PostgresOnAzureHook, snaked_tablenames: Iterable[str], pg_schema: str = "public"
    ) -> dict[str, set[str]]:
        """Looks up the column name of table in database.

        Args:
            pg_hook: Postgres connection
            snaked_tablenames: list of table names in snake case of type string
            pg_schema: name of database schema where table are located

        Return:
            dictionary containg a set of table columns of type string

        """
        rows = pg_hook.get_records(
            """
            SELECT table_name, column_name
              FROM information_schema.columns
             WHERE table_schema = %(schema)s
               AND table_name IN %(tables)s
        """,
            {"schema": pg_schema, "tables": tuple(snaked_tablenames)},
        )
        table_columns = defaultdict(set)
        for row in rows:
            table_columns[row["table_name"]].add(row["column_name"])
        return table_columns

    def _get_existing_indexes(
        self, pg_hook: PostgresOnAzureHook, snaked_tablenames: Iterable[str], pg_schema: str = "public"
    ) -> dict[str, list[str]]:
        """Looks up the index name of table in database.

        Args:
            pg_hook: Postgres connection
            snaked_tablenames: list of table names in snake case of type string
            pg_schema: name of database schema where table are located

        Return:
            dictionary containing a set of table indexes of type string
        """
        indices_pattern = "|".join(f"{tn}%" for tn in snaked_tablenames)
        rows = pg_hook.get_records(
            """
              SELECT tablename, indexname FROM pg_indexes
               WHERE schemaname = %(schema)s
                 AND indexname SIMILAR TO %(indices_pattern)s
            ORDER BY indexname
        """,
            {"schema": pg_schema, "indices_pattern": indices_pattern},
        )
        idx_per_table = defaultdict(list)
        for row in rows:
            idx_per_table[row["tablename"]].append(row["indexname"])
        return idx_per_table

    def execute(self, context: Context) -> None:
        """Translates table, column and index names based on provenance specification in schema.

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.

        Executes:
            SQL alter statements to change database table names, columns and or indexes

        """
        dataset = dataset_schema_from_url(SCHEMA_URL, self.dataset_name)
        pg_hook = PostgresOnAzureHook(dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id)
        sqls = []
        existing_tables_lookup = self._get_existing_tables(
            pg_hook, dataset.tables, pg_schema=self.pg_schema
        )
        snaked_tablenames = existing_tables_lookup.keys()
        existing_columns = self._get_existing_columns(
            pg_hook, snaked_tablenames, pg_schema=self.pg_schema
        )

        if self.rename_indexes:
            for table_name, index_names in self._get_existing_indexes(
                pg_hook, snaked_tablenames, pg_schema=self.pg_schema
            ).items():
                if table_name not in existing_tables_lookup:
                    continue
                for index_name in index_names:
                    new_table_name = existing_tables_lookup[table_name].id
                    new_index_name = index_name.replace(
                        table_name, to_snake_case(f"{dataset.id}_{new_table_name}")
                    )
                    if index_name != new_index_name:
                        sqls.append(
                            sql.SQL(
                                """
                                ALTER INDEX {old_index_name}
                                    RENAME TO {new_index_name}
                            """
                            ).format(
                                old_index_name=sql.Identifier(self.pg_schema, index_name),
                                new_index_name=sql.Identifier(new_index_name),
                            )
                        )

        for snaked_tablename, table in existing_tables_lookup.items():
            for field in table.fields:
                provenance = field.get("provenance")
                if provenance is not None:
                    snaked_field_name = to_snake_case(field.name)
                    if "relation" in field:
                        snaked_field_name += "_id"
                    if provenance.lower() in existing_columns[snaked_tablename]:
                        # quotes are applied on the provenance name in case the
                        # source uses a space in the name
                        sqls.append(
                            sql.SQL(
                                """
                                ALTER TABLE {table_name}
                                    RENAME COLUMN {provenance} TO {snaked_field_name}
                            """
                            ).format(
                                table_name=sql.Identifier(self.pg_schema, snaked_tablename),
                                provenance=sql.Identifier(provenance),
                                snaked_field_name=sql.Identifier(snaked_field_name),
                            )
                        )

            provenance = table.get("provenance")
            if provenance is not None:
                sqls.append(
                    sql.SQL(
                        """
                        ALTER TABLE IF EXISTS {old_table_name}
                            RENAME TO {new_table_name}
                    """
                    ).format(
                        old_table_name=sql.Identifier(self.pg_schema, snaked_tablename),
                        new_table_name=sql.Identifier(to_snake_case(table.id)),
                    )
                )

        pg_hook.run(sqls)
