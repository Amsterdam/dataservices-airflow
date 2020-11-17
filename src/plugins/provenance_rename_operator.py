from collections import defaultdict
from os.path import realpath
from environs import Env
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from schematools.utils import schema_def_from_url, to_snake_case

env = Env()
SCHEMA_URL = env("SCHEMA_URL")


class ProvenanceRenameOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        dataset_name,
        pg_schema="public",
        postgres_conn_id="postgres_default",
        rename_indexes=False,
        prefix_table_name="",
        postfix_table_name="",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.dataset_name = dataset_name
        self.pg_schema = pg_schema
        self.rename_indexes = rename_indexes
        # The table to enforce the provenance translations is defined by the table ID in the schema definition.
        # If the provenance translations must be applied on a temp table name i.e. 'spoorlijnen_metro_new'
        # then specify the prefix (i.e. spoorlijnen_) and postfix (i.e. _new) when calling this operator.
        self.prefix_table_name = prefix_table_name
        self.postfix_table_name = postfix_table_name

    def _snake_tablenames(self, tablenames):
        return ", ".join((f"'{to_snake_case(tn)}'" for tn in tablenames))

    def _get_existing_tables(self, pg_hook, tables, pg_schema="public"):
        if not tables:
            return []
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

    def _get_existing_columns(self, pg_hook, snaked_tablenames, pg_schema="public"):
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

    def _get_existing_indexes(self, pg_hook, snaked_tablenames, pg_schema="public"):

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

    def execute(self, context=None):
        dataset = schema_def_from_url(SCHEMA_URL, self.dataset_name)
        print(dataset)
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
                        # quotes are applied on the provenance name in case the source uses a space in the name
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
