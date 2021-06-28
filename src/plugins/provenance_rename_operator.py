from collections import defaultdict
from typing import Any, Dict, Final, Iterable, List, Optional, Set

from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from environs import Env
from schematools.utils import schema_def_from_url, to_snake_case

env = Env()
SCHEMA_URL: Final = env("SCHEMA_URL")


class ProvenanceRenameOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        dataset_name: str,
        pg_schema: str = "public",
        postgres_conn_id: str = "postgres_default",
        rename_indexes: bool = False,
        prefix_table_name: str = "",
        postfix_table_name: str = "",
        subset_tables: Optional[List] = None,
        *args: Any,
        **kwargs: Dict,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id: str = postgres_conn_id
        self.dataset_name: str = dataset_name
        self.pg_schema: str = pg_schema
        self.rename_indexes: bool = rename_indexes
        # The table to enforce the provenance translations is defined by
        # the table ID in the schema definition. If the provenance translations
        # must be applied on a temp table name i.e. 'spoorlijnen_metro_new'. Then specify
        # the prefix (i.e. spoorlijnen_) and postfix (i.e. _new) when calling this operator.
        self.prefix_table_name: str = prefix_table_name
        self.postfix_table_name: str = postfix_table_name
        # set table name for getting provenance for specific table
        self.subset_tables: Optional[List] = subset_tables

    def _snake_tablenames(self, tablenames: Iterable[str]) -> str:
        """Translates tablenames to snake case

        Args:
            tablenames: list of table names of type string

        Return:
            tuple of snaked cased table names of type string

        """
        return ", ".join(f"'{to_snake_case(tn)}'" for tn in tablenames)

    def _get_existing_tables(
        self, pg_hook: PostgresHook, tables: List, pg_schema: str = "public"
    ) -> Dict[str, Any]:
        """Looks up the table name in schema (provenance can contain the orginal (real) name)
        and relates them to existing table in database

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
            tables = [table for table in tables if table["id"] in self.subset_tables]

        table_lookup = {}

        for table in tables:
            real_tablename = table.get(
                "provenance",
                self.prefix_table_name + table.id + self.postfix_table_name,
            )
            table_lookup[to_snake_case(real_tablename)] = table

        snaked_tablenames_str = self._snake_tablenames(table_lookup.keys())
        rows = pg_hook.get_records(
            f"""
                SELECT tablename FROM pg_tables
                WHERE schemaname = '{pg_schema}' AND tablename IN ({snaked_tablenames_str})
            """
        )

        return {row["tablename"]: table_lookup[row["tablename"]] for row in rows}

    def _get_existing_columns(
        self, pg_hook: PostgresHook, snaked_tablenames: Iterable[str], pg_schema: str = "public"
    ) -> Dict[str, Set[str]]:
        """Looks up the column name of table in database

        Args:
            pg_hook: Postgres connection
            snaked_tablenames: list of table names in snake case of type string
            pg_schema: name of database schema where table are located

        Return:
            dictionary containg a set of table columns of type string

        """
        snaked_tablenames_str = self._snake_tablenames(snaked_tablenames)
        rows = pg_hook.get_records(
            f"""
                SELECT table_name, column_name FROM information_schema.columns
                WHERE table_schema = '{pg_schema}' AND table_name IN ({snaked_tablenames_str})
            """
        )
        table_columns = defaultdict(set)
        for row in rows:
            table_columns[row["table_name"]].add(row["column_name"])
        return table_columns

    def _get_existing_indexes(
        self, pg_hook: PostgresHook, snaked_tablenames: Iterable[str], pg_schema: str = "public"
    ) -> Dict[str, Set[str]]:
        """Looks up the index name of table in database

        Args:
            pg_hook: Postgres connection
            snaked_tablenames: list of table names in snake case of type string
            pg_schema: name of database schema where table are located

        Return:
            dictionary containg a set of table indexes of type string

        """

        tables_query_str = "|".join(f"{tn}%" for tn in snaked_tablenames)
        rows = pg_hook.get_records(
            f"""
                SELECT tablename, indexname FROM pg_indexes
                WHERE schemaname = '{pg_schema}' AND indexname SIMILAR TO '{tables_query_str}'
                ORDER BY indexname
            """
        )
        idx_per_table = defaultdict(list)
        for row in rows:
            idx_per_table[row["tablename"]].append(row["indexname"])
        return idx_per_table

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:  # NoQA C901
        """translates table, column and index names based on provenance
         specification in schema

        Args:
            context: When this operator is created the context parameter is used
                to refer to get_template_context for more context as part of
                inheritance of the BaseOperator. It is set to None in this case.

        Executes:
            SQL alter statements to change database table names, columns and or indexes

        """
        dataset = schema_def_from_url(SCHEMA_URL, self.dataset_name)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
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
                            f"""ALTER INDEX {self.pg_schema}.{index_name}
                                RENAME TO {new_index_name}"""
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
                            f"""ALTER TABLE {self.pg_schema}.{snaked_tablename}
                                RENAME COLUMN "{provenance}" TO {snaked_field_name}"""
                        )

            provenance = table.get("provenance")
            if provenance is not None:
                sqls.append(
                    f"""ALTER TABLE IF EXISTS {self.pg_schema}.{snaked_tablename}
                            RENAME TO {to_snake_case(table.id)}"""
                )

        pg_hook.run(sqls)
