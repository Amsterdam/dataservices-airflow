import pandas as pd
from contextlib import closing
from common.db import get_engine, get_ora_engine, get_postgreshook_instance
from psycopg2 import sql
from sqlalchemy.types import Boolean, Date, Integer, Numeric, Text


def load_from_dwh(table_name: str) -> None:
    """Loads data from an Oracle database source into a Postgres database

    Args:
        table_name: The target table where the source data will be stored

    Executes:
        SQL INSERT statements for the data and post-processing
        an ALTER statement to a contraint.

    Note: The SQL processing is done with SQLAlchemy
    Note2: Data is filterd on unit OIS 'Onderzoek, Informatie en Statistiek' by code 380000

    """
    postgreshook_instance = get_postgreshook_instance()
    db_engine = get_engine()
    dwh_ora_engine = get_ora_engine("oracle_dwh_ami")
    date_fmt = "%Y%d%m"
    with dwh_ora_engine.connect() as connection:
        df = pd.read_sql(
            """
            SELECT
             gb.DATUM_BOEKING as "datum_boeking"
            ,gb.BEDRAG_BOEKING as "bedrag_boeking"
            ,gb.OMSCHRIJVING as "boeking_omschrijving"
            ,gb.OPMERKING as "boeking_opmerking"
            ,gbr.HOOFDREKENING_CODE as "hoofdrekening_code"
            ,gbr.REKENING_OMSCHRIJVING as "rekening_omschrijving"
            ,gbr.BEDRIJFSEENHEID_CODE as "bedrijfseenheid_code"
            ,gbr.BEDRIJFSEENHEID_OMSCHRIJVING as "bedrijfseenheid_omschrijving"
            ,gb.SUBGROOTBOEK as "subgrootboek"
            ,gb.SUBGROOTBOEKTYPE as "subgrootboektype"
            ,vpr.VPL_VERPLICHTINGNUMMER as "vpl_verplichtingnummer"
            ,vpr.VPL_OMSCHRIJVING as "vpl_omschrijving"
            ,vpr.VPL_JAAR as "vpl_jaar"
            ,vpr.VPL_INKOOPCONTRACTCODE as "vpl_inkoopcontractcode"
            ,vpr.VPL_STATUS as "vpl_status"
            ,vpr.VPR_DTM_LSTE_AFBOEKING as "vpr_datum_laatste_afboeking"
            ,vpr.VPR_BEDRAG_OORSPRONKELIJK as "vpr_bedrag_oorspronkelijk"
            ,vpr.VPR_BEDRAG_AFGEBOEKT as "vpr_bedrag_afgeboekt"
            ,vpr.VPR_BEDRAG_RESTANT as "vpr_bedrag_restant"
            ,vpr.VPR_REGELNUMMER as "vpr_regelnummer"
            ,vpr.VPR_OMSCHRIJVING as "vpr_omschrijving"
            ,vpr.VPR_OPMERKING as "vpr_opmerking"
            ,vpr.VPR_DATUM_GROOTBOEK as "vpr_datum_grootboek"
            ,vpr.VPR_DATUMVERPLICHTING as "vpr_datumverplichting"
            ,wor.CODE as "werkorder_code"
            ,wor.OMSCHRIJVING  as "werkorder_omschrijving"
            ,wor.STATUS as "werkorder_status"
            ,wor.STATUS_OPMERKING as "werkorder_status_opmerking"
            ,rel.code as "relatie_code"
            ,rel.naam as "relatie_naam"
            ,rel.IND_CREDITEUR as "indicatie_crediteur"
            ,rel.IND_DEBITEUR as "indicatie_debiteur"
            ,org.SUBTEAM_CODE as "subteam_code"
            ,org.SUBTEAM_NAAM as "subteam_naam"
            ,org.TEAM_CODE as "team_code"
            ,org.TEAM_NAAM as "team_naam"
            ,org.AFDELING_CODE as "afdeling_code"
            ,org.AFDELING_NAAM as "afdeling_naam"
            ,org.RVE_CODE as "rve_code"
            ,org.RVE_NAAM as "rve_naam"
            ,org.CLUSTER_CODE as "cluster_code"
            ,org.CLUSTER_NAAM as "cluster_naam"
            FROM DATAHUB.FI2_DHB_FCT_GROOTBOEK GB
            INNER JOIN DATAHUB.FI2_DHB_DIM_RELATIE REL ON REL.ID = GB.REL_ID
            INNER JOIN DATAHUB.FI2_DHB_DIM_ORGANISATIE org ON ORG.ID = GB.ORG_ID
            INNER JOIN DATAHUB.FI2_DHB_DIM_GROOTBOEKREKENING gbr ON gbr.ID = GB.gbr_ID
            INNER JOIN DATAHUB.FI2_DHB_DIM_VERPLICHTINGREGEL vpr ON vpr.ID = GB.VPR_ID
            INNER JOIN DATAHUB.FI2_DHB_DIM_WERKORDER wor ON wor.ID = GB.WOR_ID
            WHERE RVE_CODE = '380000'
            """,
            connection,
            coerce_float=True,
            params=None,
            parse_dates={
                "datum_boeking": date_fmt,
                "vpr_datum_grootboek": date_fmt,
                "vpr_datumverplichting": date_fmt,
                "vpr_dtm_lste_afboeking": date_fmt,
            },
            columns=None,
            chunksize=None,
        )
        dtype = {
            "datum_boeking": Date(),
            "bedrag_boeking": Numeric(),
            "boeking_omschrijving": Text(),
            "boeking_opmerking": Text(),
            "hoofdrekening_code": Text(),
            "rekening_omschrijving": Text(),
            "bedrijfseenheid_code": Text(),
            "bedrijfseenheid_omschrijving": Text(),
            "subgrootboek": Text(),
            "subgrootboektype": Text(),
            "vpl_verplichtingnummer": Text(),
            "vpl_omschrijving": Text(),
            "vpl_jaar": Integer(),
            "vpl_inkoopcontractcode": Text(),
            "vpl_status": Text(),
            "vpr_datum_laatste_afboeking": Date(),
            "vpr_bedrag_oorspronkelijk": Numeric(),
            "vpr_bedrag_afgeboekt": Numeric(),
            "vpr_bedrag_restant": Numeric(),
            "vpr_regelnummer": Integer(),
            "vpr_omschrijving": Text(),
            "vpr_opmerking": Text(),
            "vpr_datum_grootboek": Date(),
            "vpr_datumverplichting": Date(),
            "werkorder_code": Text(),
            "werkorder_omschrijving": Text(),
            "werkorder_status": Text(),
            "werkorder_status_opmerking": Text(),
            "relatie_code": Text(),
            "relatie_naam": Text(),
            "indicatie_crediteur": Boolean(),
            "indicatie_debiteur": Boolean(),
            "subteam_code": Text(),
            "subteam_naam": Text(),
            "team_code": Text(),
            "team_naam": Text(),
            "afdeling_code": Text(),
            "afdeling_naam": Text(),
            "rve_code": Text(),
            "rve_naam": Text(),
            "cluster_code": Text(),
            "cluster_naam": Text(),
        }
        df.to_sql(
            table_name, db_engine, if_exists="replace", index_label="id", index=True, dtype=dtype
        )

        with closing(postgreshook_instance.get_conn().cursor()) as cur:
            cur.execute(
                sql.SQL("ALTER TABLE {table_name} ADD PRIMARY KEY (ID)").format(
                    table_name=sql.Identifier(table_name)
                )
            )
