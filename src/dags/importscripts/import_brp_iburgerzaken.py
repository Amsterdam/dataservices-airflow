import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Final, Optional, Any


def get_tables() -> list[str]:
    """Get source tables and each of their row count.

    : return : Returns a list of tables names and row count.
    """
    # Setup the needed variables for execution.
    # NOTE_1: The environment variables need to be set in the
    # Airflow DAG (and given to this container), need to be present
    # in Azure Key Vault as a secret so it can be collected by HELM
    # and be present as an environment variable.
    SRC_DB_SERVER: Optional[str] = os.getenv("DB_IBURGERZAKEN_SERVER")
    SRC_DB_NAME: Optional[str] = os.getenv("DB_IBURGERZAKEN_DB_NAME")
    SRC_DB_UID: Optional[str] = os.getenv("DB_IBURGERZAKEN_DB_UID")
    SRC_DB_UID_PWD: Optional[str] = os.getenv("DB_IBURGERZAKEN_DB_UID_PWD")
    SRC_CONNECTION_DRIVER: Optional[str] = "IBM i Access ODBC Driver"
    SRC_CONNECTION_STRING: Optional[str] = f"""DRIVER={SRC_CONNECTION_DRIVER};\
                                                SYSTEM={SRC_DB_SERVER};\
                                                DATABASE={SRC_DB_NAME};\
                                                UID={SRC_DB_UID};\
                                                PWD={SRC_DB_UID_PWD}"""
    # indicates large table, if beyond limit
    # then proces by seperate container else
    # bulk execute in one rest container
    MAX_ROW_NUM: int = 50000
    # connection open
    define_engine = create_engine("ibm_db_sa+pyodbc:///?odbc_connect={odbc_connect}".format(odbc_connect=SRC_CONNECTION_STRING))
    SQL_GET_TABLES: str = """SELECT DISTINCT
                            NUMBER_ROWS,
                            TABLE_NAME
                            FROM QSYS2.SYSTABLESTAT
                            WHERE TABLE_SCHEMA in (?)
                            AND NUMBER_ROWS > ?
                            """
    # start connection to source
    tables: list = []
    with define_engine.connect() as conn:
        result = conn.execute(SQL_GET_TABLES, SRC_DB_NAME, MAX_ROW_NUM)
        for row_count, table_name in result:
            tables.append([row_count,table_name])
    return tables


def get_tables_row_batch() -> list[Any]:
    """Get the chunk of rows per source table.

    :param tables: Hold a list of table names and row count.
    :return: Returns a list of tables names and row count.
    """
    # define the limit of number of rows for each table to proces in one container.
    # If the table has more rows that the limit, then it will process it in chunks.
    # Each chunk will have it's own container. The chunk division (what container will
    # proces which row ranges) is set in the Airflow DAG and given to this container.
    TABLE_ROW_CHOP_LIMIT: Final = 500_000
    # list of tables row batches (row chunks) to be processed
    tables_batches: list = []
    for row_count, table_name in get_tables():
        # calculate the number of row batches based to process
        row_batches =  row_count // TABLE_ROW_CHOP_LIMIT
        start_batch_counter = 0
        # for each `row batch` create a container
        for _ in range(row_batches + 1):
            # check if batch row is still within total rows, then continue creating
            # a next container for it including its row ranges (start-end)
            if TABLE_ROW_CHOP_LIMIT < (row_count - start_batch_counter):
                end_batch_counter = start_batch_counter + TABLE_ROW_CHOP_LIMIT
                tables_batches.append([table_name, str(start_batch_counter), str(end_batch_counter)])
                start_batch_counter = end_batch_counter
            # last batch row detected
            # if batch row size is bigger then rows to proces left,
            # then set batch row size to last row counter and total rows
            # like 100 (start row number) till 101 (total rows) for instance.
            else:
                tables_batches.append([table_name, str(start_batch_counter), str(row_count)])
    return tables_batches


def setup_containers() -> dict[str, list]:
    """Defines the source tables to process for each container.

    To speed up processing paralell containers are executed. The BRP
    data contains some big volume tables (some over 16 mil. rows per table).
    To allocate a specific containter for processint a big volume table,
    the total duration can be minimized.

    NOTE_1: Source table over 5 million records will have a dedicated
    container to run. Other container can have more then one source table
    to process, where the total records are somewhere arround 5 million.

    NOTE_2: The `GENERIC_VARS` are environment variabels that contains values that are collected
    from KeyVault in Azure by the Airflow instance on AKS. See `values.yaml` and `secrets.yaml`
    in the Airflow HELM configuration repository (airflow-dave). These vars are used by the docker
    container to use during dataprocessing logic.
    """
    # the environment variables names that need to be included into the container
    # "DB_IBURGERZAKEN_DB_UID_PWD"
    GENERIC_VARS_NAMES: list = [
        "ST_IBURGERZAKEN_CONTAINER_NAME",
        "ST_IBURGERZAKEN_CONNECTION_STRING",
        "DB_IBURGERZAKEN_SERVER",
        "DB_IBURGERZAKEN_DB_NAME",
        "DB_IBURGERZAKEN_DB_UID",
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
    ]

    # collect the environment variables that need to be included into the container based on `GENERIC_VARS_NAMES` above
    GENERIC_VARS_DICT: dict[str, str] = {
        variable: os.environ[variable] for variable in GENERIC_VARS_NAMES
    }

    containers: dict[str,str] = {}
    collect_all_table_names: list[str] = []

    # create for each table a container
    for index, table_name_and_row_range in enumerate(get_tables_row_batch()):
        if table_name_and_row_range[0] not in collect_all_table_names:
            collect_all_table_names.append(table_name_and_row_range[0])
        tables_to_proces_container = {"TABLES_TO_PROCESS": ','.join(table_name_and_row_range)}
        tables_to_proces_container.update(GENERIC_VARS_DICT)
        containers[f'container_{index}'] = tables_to_proces_container

    # the rest container will process all tables except the ones
    # that are already processed and defined in the `collect_table_names` list.
    # therefor the `collect_table_names` list is part of its arguments
    # so it can do an invert in the container (and process the tables
    # that are not in that list)
    TABLES_TO_PROCESS_REST: dict[str, str] = {"TABLES_TO_PROCESS": ','.join(collect_all_table_names)}
    CONTAINER_TYPE: dict[str, str] = {"CONTAINER_TYPE": "REST"}
    CONTAINER_COLLECTED_REST: dict[str, str] = (
        GENERIC_VARS_DICT | TABLES_TO_PROCESS_REST | CONTAINER_TYPE
    )
    containers['container_rest'] = CONTAINER_COLLECTED_REST

    # # TEST #
    # containers2 = { k:v for k,v in containers.items() if k == 'container_0'}
    # return containers2
    # # TEST #

    return containers