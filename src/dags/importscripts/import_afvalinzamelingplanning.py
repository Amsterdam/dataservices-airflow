from contextlib import closing
from typing import Optional

import pandas as pd
from common.db import DatabaseEngine, get_ora_engine
from psycopg2 import sql
from sqlalchemy.types import Date, DateTime, Integer, Numeric, Text


def load_from_dwh(table_name: str, dataset_name:Optional[str]=None, **context) -> None:
    """Loads data from an Oracle database source into a Postgres database.

    Args:
        table_name: The target table where the source data will be stored
        dataset_name: Name of the dataset as known in the Amsterdam schema.
            Since the DAG name can be different from the dataset name, the latter
            can be explicity given. Only applicable for Azure referentie db connection.
            Defaults to None. If None, it will use the execution context to get the
            DAG id as surrogate. Assuming that the DAG id equals the dataset name
            as defined in Amsterdam schema.

    Executes:
        SQL INSERT statements for the data and post-processing
        an ALTER statement to a contraint.
        Note: The SQL processing is done with SQLAlchemy

    """
    postgreshook_instance = DatabaseEngine(dataset_name=dataset_name, context=context).get_postgreshook_instance()
    db_engine = DatabaseEngine(dataset_name=dataset_name, context=context).get_engine()
    dwh_ora_engine = get_ora_engine("oracle_dwh_stadsdelen")
    with dwh_ora_engine.get_conn() as connection:
        df = pd.read_sql(
            """
        select ADWH_VERSIE_ID
            , SOORT_WERKZAAMHEDEN
            , KENTEKEN
            , ADWH_KENTEKEN
            , CATEGORIE
            , ADWH_ACTIVITEIT
            , WERKZAAMHEDEN_CODE
            , WERKZAAMHEDEN_OMSCHRIJVING
            , WERKZAAMHEDEN_DATUM
            , WERKZAAMHEDEN_DATUM_REF_ID
            , WERKZAAMHEDEN_STARTTIJD
            , WERKZAAMHEDEN_EINDTIJD
            , WERKZAAMHEDEN_UREN_GEPLAND
            , PAUZE_STARTTIJD
            , PAUZE_EINDTIJD
            , PAUZE_UREN_GEPLAND
            , INHUUR
            , FASE
            , MEMO
            , TEAM
            , AANTAL_MEDEWERKERS
            , UREN_INZET_MEDEWERKER_INTERN
            , UREN_INZET_MEDEWERKER_INHUUR
            , UREN_INZET_VOERTUIG
            , AANTAL_MEDEWERKERS_INTERN
            , AANTAL_MEDEWERKERS_INHUUR
            , ADWH_LAATST_GEZIEN
            , ADWH_LAATST_GEZIEN_BRON
            from DMDATA.AFVAL_INZML_VOERTUIGPLAN_V2
        """,
            connection,
            coerce_float=True,
            params=None,
            parse_dates=[
                "WERKZAAMHEDEN_DATUM",
                "ADWH_LAATST_GEZIEN",
                "ADWH_LAATST_GEZIEN_BRON",
            ],
            columns=None,
            chunksize=None,
        )
        dtype = {
            "ADWH_VERSIE_ID": Numeric(),
            "SOORT_WERKZAAMHEDEN": Text(),
            "KENTEKEN": Text(),
            "ADWH_KENTEKEN": Text(),
            "CATEGORIE": Text(),
            "ADWH_ACTIVITEIT": Text(),
            "WERKZAAMHEDEN_CODE": Text(),
            "WERKZAAMHEDEN_OMSCHRIJVING": Text(),
            "WERKZAAMHEDEN_DATUM": Date(),
            "WERKZAAMHEDEN_DATUM_REF_ID": Integer(),
            "WERKZAAMHEDEN_STARTTIJD": Text(),
            "WERKZAAMHEDEN_EINDTIJD": Text(),
            "WERKZAAMHEDEN_UREN_GEPLAND": Numeric(),
            "PAUZE_STARTTIJD": Text(),
            "PAUZE_EINDTIJD": Text(),
            "PAUZE_UREN_GEPLAND": Text(),
            "INHUUR": Text(),
            "FASE": Text(),
            "MEMO": Text(),
            "TEAM": Text(),
            "AANTAL_MEDEWERKERS": Text(),
            "UREN_INZET_MEDEWERKER_INTERN": Numeric(),
            "UREN_INZET_MEDEWERKER_INHUUR": Numeric(),
            "UREN_INZET_VOERTUIG": Numeric(),
            "AANTAL_MEDEWERKERS_INTERN": Numeric(),
            "AANTAL_MEDEWERKERS_INHUUR": Numeric(),
            "ADWH_LAATST_GEZIEN": DateTime(),
            "ADWH_LAATST_GEZIEN_BRON": DateTime(),
        }
        # it seems that get_conn() makes the columns case sensitive
        # lowercase all columns so the database will handle them as case insensitive
        df.columns = map(str.lower, df.columns)
        df.to_sql(table_name, db_engine, if_exists="replace", dtype=dtype, index=False)

        with closing(postgreshook_instance.get_conn().cursor()) as cur:
            cur.execute(
                sql.SQL("ALTER TABLE {table_name} ADD PRIMARY KEY (ADWH_VERSIE_ID)").format(
                    table_name=sql.Identifier(table_name)
                )
            )
