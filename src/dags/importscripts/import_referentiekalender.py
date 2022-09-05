from typing import Optional

import pandas as pd
from common.db import DatabaseEngine, get_ora_engine
from psycopg2 import sql
from sqlalchemy.types import Date, Integer, Text


def load_from_dwh(table_name: str, dataset_name: Optional[str] = None, **context: dict) -> None:
    """Loads data from an Oracle database source into a Postgres database.

    Args:
        table_name: The target table where the source data will be stored
        dataset_name: Name of dataset that will be used as the database user
            only applicable on Azure.

    Executes:
        SQL INSERT statements for the data and post-processing
        an ALTER statement to a contraint.
        Note: The SQL processing is done with SQLAlchemy
    """
    postgreshook_instance = DatabaseEngine(
        dataset_name=dataset_name, context=context
    ).get_postgreshook_instance()
    db_engine = DatabaseEngine(dataset_name=dataset_name, context=context).get_engine()
    dwh_ora_engine = get_ora_engine("oracle_dwh_stadsdelen")
    with dwh_ora_engine.get_conn() as connection:
        df = pd.read_sql(
            """
            select "ID"
            ,"DATUM"
            ,"DAG_VAN_WEEK_NUMMER"
            ,"DAG_VAN_WEEK_NAAM"
            ,"DAG_VAN_WEEK_KNAAM"
            ,"VORIGE_DAG_VAN_WEEK_NAAM"
            ,"VORIGE_DAG_VAN_WEEK_KNAAM"
            ,"WEEKEND_IND"
            ,"FEESTDAG_IND"
            ,"FEESTDAG_ADAM_IND"
            ,"AANTAL_WERKDAGEN"
            ,"AANTAL_WERKDAGEN_ADAM"
            ,"SEIZOEN"
            ,"WEEK_IN_MAAND_NUMMER"
            ,"WEEK_IN_MAAND_START_DATUM"
            ,"WEEK_IN_MAAND_EINDE_DATUM"
            ,"WEEK_IN_JAAR_NUMMER"
            ,"ISO_WEEK_NUMMER"
            ,"JAAR_ISO_WEEKNR"
            ,"ISO_WEEK_START_DATUM"
            ,"ISO_WEEK_EINDE_DATUM"
            ,"DAG_VAN_MAAND_NUMMER"
            ,"MAAND_WAARDE"
            ,"MAAND_NAAM"
            ,"MAAND_KNAAM"
            ,"JAARMAAND"
            ,"MAAND_START_DATUM"
            ,"MAAND_EINDE_DATUM"
            ,"DAGEN_IN_MAAND"
            ,"LAATSTE_DAG_VAN_MAAND_IND"
            ,"DAG_VAN_KWARTAAL_NUMMER"
            ,"KWARTAAL_WAARDE"
            ,"KWARTAAL_NAAM"
            ,"JAARKWARTAAL"
            ,"KWARTAAL_START_DATUM"
            ,"KWARTAAL_EINDE_DATUM"
            ,"DAGEN_IN_KWARTAAL"
            ,"LAATSTE_DAG_VAN_KWARTAAL_IND"
            ,"TERTAAL"
            ,"JAAR_TERTAAL"
            ,"DAG_IN_TERTAAL_NUMMER"
            ,"DAGEN_IN_TERTAAL"
            ,"LAATSTE_DAG_VAN_TERTAAL_IND"
            ,"DAG_VAN_JAAR_NUMMER"
            ,"JAAR_WAARDE"
            ,"JAAR_NAAM"
            ,"JAAR_KNAAM"
            ,"JAAR_START_DATUM"
            ,"JAAR_EINDE_DATUM"
            ,"DAGEN_IN_JAAR"
            ,"LAATSTE_DAG_VAN_JAAR_IND"
            ,"JAARNAAM_KORT"
            ,"JULIAANSE_DATUM"
            ,CAST(NULL AS NUMBER) AS "SCHOOLVAKANTIE_NL_NOORD_IND"
            ,CAST(NULL AS NUMBER) AS "SCHOOLVAKANTIE_NL_MIDDEN_IND"
            ,CAST(NULL AS NUMBER) AS "SCHOOLVAKANTIE_NL_ZUID_IND"
            ,"NIVEAUCODE"
            from DMDATA.ALG_DIM_DATUM_V2
        """,
            connection,
            coerce_float=True,
            params=None,
            parse_dates=[
                "DATUM",
                "ISO_WEEK_START_DATUM",
                "ISO_WEEK_EIND_DATUM",
                "WEEK_IN_MAAND_START_DATUM",
                "WEEK_IN_MAAND_EINDE_DATUM",
                "MAAND_START_DATUM",
                "MAAND_EINDE_DATUM",
                "KWARTAAL_START_DATUM",
                "KWARTAAL_EINDE_DATUM",
                "JAAR_START_DATUM",
                "JAAR_EINDE_DATUM",
            ],
            columns=None,
            chunksize=None,
        )
        dtype = {
            "ID": Integer(),
            "DATUM": Date(),
            "DAG_VAN_WEEK_NUMMER": Integer(),
            "DAG_VAN_WEEK_NAAM": Text(),
            "DAG_VAN_WEEK_KNAAM": Text(),
            "VORIGE_DAG_VAN_WEEK_NAAM": Text(),
            "VORIGE_DAG_VAN_WEEK_KNAAM": Text(),
            "WEEKEND_IND": Integer(),
            "FEESTDAG_IND": Integer(),
            "FEESTDAG_ADAM_IND": Integer(),
            "AANTAL_WERKDAGEN": Integer(),
            "AANTAL_WERKDAGEN_ADAM": Integer(),
            "SEIZOEN": Text(),
            "WEEK_IN_MAAND_NUMMER": Integer(),
            "WEEK_IN_MAAND_START_DATUM": Date(),
            "WEEK_IN_MAAND_EINDE_DATUM": Date(),
            "WEEK_IN_JAAR_NUMMER": Integer(),
            "ISO_WEEK_NUMMER": Integer(),
            "JAAR_ISO_WEEKNR": Text(),
            "ISO_WEEK_START_DATUM": Date(),
            "ISO_WEEK_EINDE_DATUM": Date(),
            "DAG_VAN_MAAND_NUMMER": Integer(),
            "MAAND_WAARDE": Integer(),
            "MAAND_NAAM": Text(),
            "MAAND_KNAAM": Text(),
            "JAARMAAND": Text(),
            "MAAND_START_DATUM": Text(),
            "MAAND_EINDE_DATUM": Text(),
            "DAGEN_IN_MAAND": Integer(),
            "LAATSTE_DAG_VAN_MAAND_IND": Integer(),
            "DAG_VAN_KWARTAAL_NUMMER": Integer(),
            "KWARTAAL_WAARDE": Integer(),
            "KWARTAAL_NAAM": Text(),
            "JAARKWARTAAL": Text(),
            "KWARTAAL_START_DATUM": Date(),
            "KWARTAAL_EINDE_DATUM": Date(),
            "DAGEN_IN_KWARTAAL": Integer(),
            "LAATSTE_DAG_VAN_KWARTAAL_IND": Integer(),
            "TERTAAL": Integer(),
            "JAAR_TERTAAL": Text(),
            "DAG_IN_TERTAAL_NUMMER": Integer(),
            "DAGEN_IN_TERTAAL": Integer(),
            "LAATSTE_DAG_VAN_TERTAAL_IND": Integer(),
            "DAG_VAN_JAAR_NUMMER": Integer(),
            "JAAR_WAARDE": Integer(),
            "JAAR_NAAM": Text(),
            "JAAR_KNAAM": Text(),
            "JAAR_START_DATUM": Date(),
            "JAAR_EINDE_DATUM": Date(),
            "DAGEN_IN_JAAR": Integer(),
            "LAATSTE_DAG_VAN_JAAR_IND": Integer(),
            "JAARNAAM_KORT": Text(),
            "JULIAANSE_DATUM": Integer(),
            "SCHOOLVAKANTIE_NL_NOORD_IND": Integer(),
            "SCHOOLVAKANTIE_NL_MIDDEN_IND": Integer(),
            "SCHOOLVAKANTIE_NL_ZUID_IND": Integer(),
            "NIVEAUCODE": Text(),
        }
        # it seems that get_conn() makes the columns case sensitive
        # lowercase all columns so the database will handle them as case insensitive
        df.columns = map(str.lower, df.columns)
        df.to_sql(table_name, db_engine, if_exists="replace", dtype=dtype, index=False)

        # Since Oracle DB does not differentiate between a DATE and DATETIME type
        # an explicit cast is needed. The DATE type of SQLAlchemy is ignored assumingly
        # due to Oracle override. As for now only applicable for field DATUM.
        with postgreshook_instance.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("ALTER TABLE {table_name} ADD PRIMARY KEY (ID);").format(
                        table_name=sql.Identifier(table_name)
                    )
                )
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {table_name} ALTER COLUMN\
                            DATUM TYPE DATE USING DATUM::DATE;"
                    ).format(table_name=sql.Identifier(table_name))
                )
            conn.commit()