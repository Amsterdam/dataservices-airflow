#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.www import utils as wwwutils
from common.db import define_dataset_name_for_azure_dbuser
from postgres_on_azure_hook import PostgresOnAzureHook
from psycopg2.sql import SQL, Identifier

if TYPE_CHECKING:
    from airflow.utils.context import Context  # noqa: F811


class PostgresOnAzureOperator(BaseOperator):
    """Executes SQL code in a specific Azure Postgres database."""

    template_fields: Sequence[str] = ("sql",)
    # TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
    template_fields_renderers = {
        "sql": "postgresql" if "postgresql" in wwwutils.get_attr_renderer() else "sql"
    }
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        sql: Union[str, list[str]],
        dataset_name: Optional[str] = None,
        postgres_conn_id: str = "postgres_default",
        autocommit: bool = False,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        runtime_parameters: Optional[Mapping] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize.

        params:
            sql: the SQL code to be executed as a single string, or
                a list of str (sql statements), or a reference to a template file.
                Template references are recognized by str ending in '.sql'
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            db_schema_name: Name of the schema where to execute the SQL statement.
                It defaults to `public`.
            postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
                reference to a specific postgres database.
                `DefaultAzureCredential` is used which will pick up the managed identity
                using the `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` environment variables.
                Then set the connection using an env var like this:
                AIRFLOW_CONN_POSTGRES_DEFAULT:
                postgresql://EM4W-DATA-dataset-ot-covid_19-rw@<host-name>:<token>@<host-url>:5432/<db-name>?\
                    cursor=dictcursor&iam=true
                Note: The `EM4W-DATA-dataset-ot-covid_19-rw` to be registered in the
                    Postgres database as an AAD related database user.
                See: https://docs.microsoft.com/en-us/azure/postgresql/single-server/\
                how-to-configure-sign-in-azure-ad-authentication#authenticate-with-azure-ad-as-a-group-member
                for reference
            autocommit: if True, each command is automatically committed. Defaults to False.
            parameters: The parameters to render the SQL query with. Defaults to None.
            database: Name of database which overwrite defined one in connection. Defaults to None.
            runtime_parameters: Connection parameters. Defaults to None.

        """
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.runtime_parameters = runtime_parameters
        self.hook: Optional[PostgresOnAzureHook] = None

    def execute(self, context: Context) -> None:
        """Default execution method after intialization.

        params:
            context: The context of the calling DAG.

        Executes:
            SQL statement with defined database connection.
        """
        # If Azure.
        # To cope with a different logic for defining the Azure referentie db user.
        # If CloudVPS is not used anymore, then this extra route can be removed.
        if os.environ.get("AZURE_TENANT_ID") is not None:
            self.dataset_name = define_dataset_name_for_azure_dbuser(
                dataset_name=self.dataset_name, context=context
            )

        self.hook = PostgresOnAzureHook(
            postgres_conn_id=self.postgres_conn_id,
            context=context,
            schema=self.database,
            dataset_name=self.dataset_name,
        )
        if self.runtime_parameters:
            final_sql = []
            sql_param = {}
            for param in self.runtime_parameters:
                set_param_sql = f"SET {{}} TO %({param})s;"  # noqa: P103
                dynamic_sql = SQL(set_param_sql).format(Identifier(f"{param}"))
                final_sql.append(dynamic_sql)
            for param, val in self.runtime_parameters.items():
                sql_param.update({f"{param}": f"{val}"})
            if self.parameters:
                sql_param.update(self.parameters)
            if isinstance(self.sql, str):
                final_sql.append(SQL(self.sql))
            else:
                final_sql.extend(list(map(SQL, self.sql)))
            self.hook.run(final_sql, self.autocommit, parameters=sql_param)
        else:
            self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
