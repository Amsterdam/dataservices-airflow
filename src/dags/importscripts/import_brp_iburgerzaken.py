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
    TABLE_ROW_CHOP_LIMIT: Final = 1_000_000
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

    # REST_CONTAINER_TABLES: list = []

    # # source tables far over 10 million records
    # TABLES_TO_PROCESS: dict[str, str] = {"TABLES_TO_PROCESS": "BZSBURM00"}  # 161 mil records
    # CONTAINER_OVER10MIL_1: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {"TABLES_TO_PROCESS": "BZSBURL00"}  # 161 mil records
    # CONTAINER_OVER10MIL_2: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {"TABLES_TO_PROCESS": "BZSVBRW00,BZSVBRL00"}
    # CONTAINER_OVER10MIL_3: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {
    #     "TABLES_TO_PROCESS": "BZSPRML00,BZSPRMM00,BZSPRRL00,BZSPRRM00"
    # }
    # CONTAINER_OVER10MIL_4: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # # collected source tables arround 5 million records total
    # TABLES_TO_PROCESS: dict[str, str] = {
    #     "TABLES_TO_PROCESS": "BZSAHIL00,BZSAHIL01,BZSAHIM00,BZSAHIL03,BZSDHIL00,BZSDHIM00,BZSBOAW00,BZSSYNL00,BZSSYNL01,BZSSYNM00"
    # }
    # CONTAINER_COLLECTED_5MIL_1: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {
    #     "TABLES_TO_PROCESS": "BZSRLPL00,BZSRLPL01,BZSRLPM00,BZSOBAL01,BZSOBAM00,BZSRSDL00,BZSRSDM00,BZSNATL00,BZSNATM00,BZSPRSL00,BZSPRSL01,BZSPRSL02,BZSPRSL03"
    # }
    # CONTAINER_COLLECTED_5MIL_2: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {
    #     "TABLES_TO_PROCESS": "BZSPRSL04,BZSPRSL05,BZSPRSL06,BZSPRSL07,BZSPRSL08,BZSPRSL10,BZSPRSL11,BZSPRSL12,BZSPRSL13,BZSPRSL14,BZSPRSL16,BZSPRSL21,BZSPRSL22,BZSPRSM00,BZSKPGL00,BZSKPGL01,BZSKPGL02,BZSKPGL03,BZSKPGM00,BZSANNM00,BZSBRSL00,BZSBRSM00,BZSAHIL02,BZSBIZL00,BZSBIZL01"
    # }
    # CONTAINER_COLLECTED_5MIL_3: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {
    #     "TABLES_TO_PROCESS": "BZSBIZL02,BZSBIZL03,BZSBIZM00,BZSC60L00,BZSC60M00,BZSKINL00,BZSKINL01,BZSKINL02,BZSKINM00,BZSPRSL17,BZSRELL00,BZSRELL01,BZSRELM00,BZSVWSL00,BZSVWSM00,BZST0JM00,BZST0JML0,BZST1JM00,BZST1JML0,BZST1JML1,BZSANTM00,BZSGOVL00,BZSGOVM00,BZSBUIL00,BZSBUIL01,BZSBUIL02,BZSBUIL03,BZSBUIL04,BZSBUIL05,BZSBUIL06,BZSBUIL07,BZSBUIL08,BZSBUIM00,BZSHUWL00,BZSHUWL01"
    # }
    # CONTAINER_COLLECTED_5MIL_4: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # TABLES_TO_PROCESS: dict[str, str] = {
    #     "TABLES_TO_PROCESS": "BZSHUWL02,BZSHUWM00,BZSPRSL18,BZSAFRL00,BZSAFRL01,BZSAFRM00,BZSPRAL00,BZSPRAM00,BZSC51L00,BZSC51L01,BZSC51M00,BZST0FM00,BZST0FML0,BZST1FM00,BZST1FML0,BZST1FML1,BZST1FML2,BZST1FML3,BZST1FML4,BZST1FML5,BZST1FML6,BZST1FML7,BZST2FM00,BZST2FML0,BZST2FML1,BZST2FML2,BZSVGDL00,BZSVGDL01,BZSVGDL02,BZSVGDL03,BZSVGDL04,BZSVGDL05,BZSVGDL06,BZSVGDL07,BZSVGDL08,BZSVGDL09,BZSVGDL10,BZSVGDL11,BZSVGDM00,BZSOBNM00,BZSOBNML0,BZSVBTL00,BZSVBTM00,BZSKCSL00,BZSKCSM00,BZSC55L00,BZSC55M00,BZSHBWM00,BZSHBWML0,BZSGDIL00,BZSGDIM00"
    # }
    # CONTAINER_COLLECTED_5MIL_5: dict[str, str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()

    # # TEST CONTAINER
    # # BZSTABT00,BZSTBXT00,BZSTBXM00 (only BZSTBXM00 is present at the moment)
    # TABLES_TO_PROCESS:dict[str,str] = {"TABLES_TO_PROCESS": "BZSTBXM00"}
    # CONTAINER_COLLECTED_TEST:dict[str,str] = GENERIC_VARS_DICT | TABLES_TO_PROCESS
    # REST_CONTAINER_TABLES += TABLES_TO_PROCESS.values()
    # # TEST CONTAINER

    # # concat all tables above that are handled by a dedicated container
    # # this tables are used in the rest container to NOT handle but all others
    # # tables that do not appear in `REST_CONTAINER_TABLES`.
    # REST_CONTAINER_TABLES_CONCAT = ",".join(REST_CONTAINER_TABLES)

    # # collected source all other tables (a.k.a the `REST` container)
    # TABLES_TO_PROCESS: dict[str, str] = {"TABLES_TO_PROCESS": REST_CONTAINER_TABLES_CONCAT}
    # CONTAINER_TYPE: dict[str, str] = {"CONTAINER_TYPE": "REST"}
    # CONTAINER_COLLECTED_REST: dict[str, str] = (
    #     GENERIC_VARS_DICT | TABLES_TO_PROCESS | CONTAINER_TYPE
    # )

    # return {
    #     "container_test": CONTAINER_COLLECTED_TEST,
    #     "container_over10mil_1": CONTAINER_OVER10MIL_1,
    #     "container_over10mil_2": CONTAINER_OVER10MIL_2,
    #     "container_over10mil_3": CONTAINER_OVER10MIL_3,
    #     "container_over10mil_4": CONTAINER_OVER10MIL_4,
    #     "container_arround5mil_1": CONTAINER_COLLECTED_5MIL_1,
    #     "container_arround5mil_2": CONTAINER_COLLECTED_5MIL_2,
    #     "container_arround5mil_3": CONTAINER_COLLECTED_5MIL_3,
    #     "container_arround5mil_4": CONTAINER_COLLECTED_5MIL_4,
    #     "container_arround5mil_5": CONTAINER_COLLECTED_5MIL_5,
    #     "container_rest": CONTAINER_COLLECTED_REST
    # }

    # tables: list = [
    #     "BZSBURL00",
    #     "BZSBURM00",
    #     "BZSVBRL00",
    #     "BZSVBRW00",
    #     "BZSPRML00",
    #     "BZSPRMM00",
    #     "BZSPRRL00",
    #     "BZSPRRM00",
    #     "BZSAHIL00",
    #     "BZSAHIL01",
    #     "BZSAHIM00",
    #     "BZSAHIL03",
    #     "BZSDHIL00",
    #     "BZSDHIM00",
    #     "BZSBOAW00",
    #     "BZSSYNL00",
    #     "BZSSYNL01",
    #     "BZSSYNM00",
    #     "BZSRLPL00",
    #     "BZSRLPL01",
    #     "BZSRLPM00",
    #     "BZSOBAL01",
    #     "BZSOBAM00",
    #     "BZSRSDL00",
    #     "BZSRSDM00",
    #     "BZSNATL00",
    #     "BZSNATM00",
    #     "BZSPRSL00",
    #     "BZSPRSL01",
    #     "BZSPRSL02",
    #     "BZSPRSL03",
    #     "BZSPRSL04",
    #     "BZSPRSL05",
    #     "BZSPRSL06",
    #     "BZSPRSL07",
    #     "BZSPRSL08",
    #     "BZSPRSL10",
    #     "BZSPRSL11",
    #     "BZSPRSL12",
    #     "BZSPRSL13",
    #     "BZSPRSL14",
    #     "BZSPRSL16",
    #     "BZSPRSL21",
    #     "BZSPRSL22",
    #     "BZSPRSM00",
    #     "BZSKPGL00",
    #     "BZSKPGL01",
    #     "BZSKPGL02",
    #     "BZSKPGL03",
    #     "BZSKPGM00",
    #     "BZSANNM00",
    #     "BZSBRSL00",
    #     "BZSBRSM00",
    #     "BZSAHIL02",
    #     "BZSBIZL00",
    #     "BZSBIZL01",
    #     "BZSBIZL02",
    #     "BZSBIZL03",
    #     "BZSBIZM00",
    #     "BZSC60L00",
    #     "BZSC60M00",
    #     "BZSKINL00",
    #     "BZSKINL01",
    #     "BZSKINL02",
    #     "BZSKINM00",
    #     "BZSPRSL17",
    #     "BZSRELL00",
    #     "BZSRELL01",
    #     "BZSRELM00",
    #     "BZSVWSL00",
    #     "BZSVWSM00",
    #     "BZST0JM00",
    #     "BZST0JML0",
    #     "BZST1JM00",
    #     "BZST1JML0",
    #     "BZST1JML1",
    #     "BZSANTM00",
    #     "BZSGOVL00",
    #     "BZSGOVM00",
    #     "BZSBUIL00",
    #     "BZSBUIL01",
    #     "BZSBUIL02",
    #     "BZSBUIL03",
    #     "BZSBUIL04",
    #     "BZSBUIL05",
    #     "BZSBUIL06",
    #     "BZSBUIL07",
    #     "BZSBUIL08",
    #     "BZSBUIM00",
    #     "BZSHUWL00",
    #     "BZSHUWL01",
    #     "BZSHUWL02",
    #     "BZSHUWM00",
    #     "BZSPRSL18",
    #     "BZSAFRL00",
    #     "BZSAFRL01",
    #     "BZSAFRM00",
    #     "BZSPRAL00",
    #     "BZSPRAM00",
    #     "BZSC51L00",
    #     "BZSC51L01",
    #     "BZSC51M00",
    #     "BZST0FM00",
    #     "BZST0FML0",
    #     "BZST1FM00",
    #     "BZST1FML0",
    #     "BZST1FML1",
    #     "BZST1FML2",
    #     "BZST1FML3",
    #     "BZST1FML4",
    #     "BZST1FML5",
    #     "BZST1FML6",
    #     "BZST1FML7",
    #     "BZST2FM00",
    #     "BZST2FML0",
    #     "BZST2FML1",
    #     "BZST2FML2",
    #     "BZSVGDL00",
    #     "BZSVGDL01",
    #     "BZSVGDL02",
    #     "BZSVGDL03",
    #     "BZSVGDL04",
    #     "BZSVGDL05",
    #     "BZSVGDL06",
    #     "BZSVGDL07",
    #     "BZSVGDL08",
    #     "BZSVGDL09",
    #     "BZSVGDL10",
    #     "BZSVGDL11",
    #     "BZSVGDM00",
    #     "BZSOBNM00",
    #     "BZSOBNML0",
    #     "BZSVBTL00",
    #     "BZSVBTM00",
    #     "BZSKCSL00",
    #     "BZSKCSM00",
    #     "BZSC55L00",
    #     "BZSC55M00",
    #     "BZSHBWM00",
    #     "BZSHBWML0",
    #     "BZSGDIL00",
    #     "BZSGDIM00"]

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
    TABLES_TO_PROCESS_REST: dict[str, str] = {"TABLES_TO_PROCESS": collect_all_table_names}
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


# Below an overview of all source table that have over 0.5 million records each (about 100+ out of 1_000+ total (10%)).
# the total number of records of these tables is arround 670 million.
# Combined with all other tables, that would be arround *800* million.
# 161114433	BZSBURL00
# 161114433	BZSBURM00
# 27800398	BZSVBRL00
# 27800398	BZSVBRW00
# 17428935	BZSPRML00
# 17428935	BZSPRMM00
# 17428935	BZSPRRL00
# 17428935	BZSPRRM00
# 6662160	BZSAHIL00
# 6662160	BZSAHIL01
# 6662160	BZSAHIM00
# 4841558	BZSAHIL03
# 4729344	BZSDHIL00
# 4729344	BZSDHIM00
# 4660819	BZSBOAW00
# 4472863	BZSSYNL00
# 4472863	BZSSYNL01
# 4472863	BZSSYNM00
# 4386641	BZSRLPL00
# 4386641	BZSRLPL01
# 4386641	BZSRLPM00
# 2795119	BZSOBAL01
# 2795119	BZSOBAM00
# 2619730	BZSRSDL00
# 2619730	BZSRSDM00
# 2376884	BZSNATL00
# 2376884	BZSNATM00
# 2090614	BZSPRSL00
# 2090614	BZSPRSL01
# 2090614	BZSPRSL02
# 2090614	BZSPRSL03
# 2090614	BZSPRSL04
# 2090614	BZSPRSL05
# 2090614	BZSPRSL06
# 2090614	BZSPRSL07
# 2090614	BZSPRSL08
# 2090614	BZSPRSL10
# 2090614	BZSPRSL11
# 2090614	BZSPRSL12
# 2090614	BZSPRSL13
# 2090614	BZSPRSL14
# 2090614	BZSPRSL16
# 2090614	BZSPRSL21
# 2090614	BZSPRSL22
# 2090614	BZSPRSM00
# 2090459	BZSKPGL00
# 2090459	BZSKPGL01
# 2090459	BZSKPGL02
# 2090459	BZSKPGL03
# 2090459	BZSKPGM00
# 2042667	BZSANNM00
# 2042667	BZSBRSL00
# 2042667	BZSBRSM00
# 1820602	BZSAHIL02
# 1745838	BZSBIZL00
# 1745838	BZSBIZL01
# 1745838	BZSBIZL02
# 1745838	BZSBIZL03
# 1745838	BZSBIZM00
# 1691238	BZSC60L00
# 1691238	BZSC60M00
# 1432745	BZSKINL00
# 1432745	BZSKINL01
# 1432745	BZSKINL02
# 1432745	BZSKINM00
# 1231327	BZSPRSL17
# 1192215	BZSRELL00
# 1192215	BZSRELL01
# 1192215	BZSRELM00
# 1059440	BZSVWSL00
# 1059440	BZSVWSM00
# 1034794	BZST0JM00
# 1034794	BZST0JML0
# 1034793	BZST1JM00
# 1034793	BZST1JML0
# 1034793	BZST1JML1
# 1032295	BZSANTM00
# 948800	BZSGOVL00
# 948800	BZSGOVM00
# 872900	BZSBUIL00
# 872900	BZSBUIL01
# 872900	BZSBUIL02
# 872900	BZSBUIL03
# 872900	BZSBUIL04
# 872900	BZSBUIL05
# 872900	BZSBUIL06
# 872900	BZSBUIL07
# 872900	BZSBUIL08
# 872900	BZSBUIM00
# 862812	BZSHUWL00
# 862812	BZSHUWL01
# 862812	BZSHUWL02
# 862812	BZSHUWM00
# 859287	BZSPRSL18
# 855130	BZSAFRL00
# 855130	BZSAFRL01
# 855130	BZSAFRM00
# 850916	BZSPRAL00
# 850916	BZSPRAM00
# 763580	BZSC51L00
# 763580	BZSC51L01
# 763580	BZSC51M00
# 736781	BZST0FM00
# 736781	BZST0FML0
# 736775	BZST1FM00
# 736775	BZST1FML0
# 736775	BZST1FML1
# 736775	BZST1FML2
# 736775	BZST1FML3
# 736775	BZST1FML4
# 736775	BZST1FML5
# 736775	BZST1FML6
# 736775	BZST1FML7
# 663885	BZST2FM00
# 663885	BZST2FML0
# 663885	BZST2FML1
# 663885	BZST2FML2
# 663490	BZSVGDL00
# 663490	BZSVGDL01
# 663490	BZSVGDL02
# 663490	BZSVGDL03
# 663490	BZSVGDL04
# 663490	BZSVGDL05
# 663490	BZSVGDL06
# 663490	BZSVGDL07
# 663490	BZSVGDL08
# 663490	BZSVGDL09
# 663490	BZSVGDL10
# 663490	BZSVGDL11
# 663490	BZSVGDM00
# 659653	BZSOBNM00
# 659653	BZSOBNML0
# 624002	BZSVBTL00
# 624002	BZSVBTM00
# 615516	BZSKCSL00
# 615516	BZSKCSM00
# 552592	BZSC55L00
# 552592	BZSC55M00
# 530433	BZSHBWM00
# 530433	BZSHBWML0
# 500891	BZSGDIL00
# 500891	BZSGDIM00



