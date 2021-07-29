from typing import Final

# SQL: contains select en create queries
#
# Note1:
# From the GOB objectstore the uploaded BGT and KBK data is processed in the basiskaart database
# (by Jenkins). The vector based tile generator (
# https://github.com/Amsterdam/vector_tiles_t_rex) uses the data from the basiskaart database,
# which is a collection of materialized views based upon the BGT (basiskaart grootschalige
# topologie) en KBK (kleinschalig basiskaart level 10 and 50).
#
# Note2:
# For the DSO-API and logical replication the data from the basiskaart database is processed
# into tables in the master database (a.k.a. referentie database). These tables can also be used
# to create the necessary materialized views (see note 1) for the vector based tile generator.
# i.e. CREATE MATERIALIZED VIEW <name_view> AS SELECT * FROM <table_name>
#
# --------------------------------------------------------------------------
# CREATE MVIEWS (on master DB) for the T-rex tile generator
# --------------------------------------------------------------------------

CREATE_MVIEWS: Final = """
DROP MATERIALIZED VIEW IF EXISTS bgt_vw_{{ params.base_table }};

CREATE MATERIALIZED VIEW IF NOT EXISTS bgt_vw_{{ params.base_table }} AS
SELECT * FROM PUBLIC.{{ params.dag_id }}_{{ params.base_table }}
WITH DATA;

/* creating index on geometrie */
CREATE INDEX IF NOT EXISTS bgt_vw_{{ params.base_table }}_geom_idx ON bgt_vw_{{ params.base_table }} USING gist (geometrie);
/* renaming the ID to identificatie_lokaalid as used in the T-REX tile server */
ALTER MATERIALIZED VIEW IF EXISTS bgt_vw_{{ params.base_table }} RENAME COLUMN id TO identificatie_lokaalid;
"""

# --------------------------------------------------------------------------
# SELECT statements (to get data out of basiskaart DB)
# --------------------------------------------------------------------------

SELECT_GEBOUWVLAK_SQL: Final = """
SELECT "BGTPLUS_GISE_bordes".identificatie_lokaalid || 'BGTPLUS_GISE_bordes' AS id,
       "BGTPLUS_GISE_bordes".plus_type AS type,
       ST_makeValid("BGTPLUS_GISE_bordes".geometrie) AS geometrie,
       "BGTPLUS_GISE_bordes".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_GISE_bordes"
UNION
SELECT "BGTPLUS_GISE_luifel".identificatie_lokaalid || 'BGTPLUS_GISE_luifel' AS id,
       "BGTPLUS_GISE_luifel".plus_type AS type,
       ST_makeValid("BGTPLUS_GISE_luifel".geometrie) AS geometrie,
       "BGTPLUS_GISE_luifel".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_GISE_luifel"
UNION
SELECT "BGTPLUS_GISE_onbekend".identificatie_lokaalid || 'BGTPLUS_GISE_onbekend' AS id,
       "BGTPLUS_GISE_onbekend".plus_type AS type,
       ST_makeValid("BGTPLUS_GISE_onbekend".geometrie) AS geometrie,
       "BGTPLUS_GISE_onbekend".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_GISE_onbekend"
UNION
SELECT "BGTPLUS_GISE_toegangstrap".identificatie_lokaalid || 'BGTPLUS_GISE_toegangstrap' AS id,
       "BGTPLUS_GISE_toegangstrap".plus_type AS type,
       ST_makeValid("BGTPLUS_GISE_toegangstrap".geometrie) AS geometrie,
       "BGTPLUS_GISE_toegangstrap".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_GISE_toegangstrap"
UNION
SELECT "BGTPLUS_OBW_bunker".identificatie_lokaalid || 'BGTPLUS_OBW_bunker' AS id,
       "BGTPLUS_OBW_bunker".plus_type AS type,
       ST_makeValid("BGTPLUS_OBW_bunker".geometrie) AS geometrie,
       "BGTPLUS_OBW_bunker".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_OBW_bunker"
UNION
SELECT "BGTPLUS_OBW_onbekend".identificatie_lokaalid || 'BGTPLUS_OBW_onbekend' AS id,
       "BGTPLUS_OBW_onbekend".plus_type AS type,
       ST_makeValid("BGTPLUS_OBW_onbekend".geometrie) AS geometrie,
       "BGTPLUS_OBW_onbekend".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_OBW_onbekend"
UNION
SELECT "BGTPLUS_OBW_schuur".identificatie_lokaalid || 'BGTPLUS_OBW_schuur' AS id,
       "BGTPLUS_OBW_schuur".plus_type AS type,
       ST_makeValid("BGTPLUS_OBW_schuur".geometrie) AS geometrie,
       "BGTPLUS_OBW_schuur".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_OBW_schuur"
UNION
SELECT "BGTPLUS_OBW_voedersilo".identificatie_lokaalid || 'BGTPLUS_OBW_voedersilo' AS id,
       "BGTPLUS_OBW_voedersilo".plus_type AS type,
       ST_makeValid("BGTPLUS_OBW_voedersilo".geometrie) AS geometrie,
       "BGTPLUS_OBW_voedersilo".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_OBW_voedersilo"
UNION
SELECT "BGT_OBW_bassin".identificatie_lokaalid || 'BGT_OBW_bassin' AS id,
       "BGT_OBW_bassin".bgt_type AS type,
       ST_makeValid("BGT_OBW_bassin".geometrie) AS geometrie,
       "BGT_OBW_bassin".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_bassin"
UNION
SELECT "BGT_OBW_bezinkbak".identificatie_lokaalid || 'BGT_OBW_bezinkbak' AS id,
       "BGT_OBW_bezinkbak".bgt_type AS type,
       ST_makeValid("BGT_OBW_bezinkbak".geometrie) AS geometrie,
       "BGT_OBW_bezinkbak".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_bezinkbak"
UNION
SELECT "BGT_OBW_lage_trafo".identificatie_lokaalid || 'BGT_OBW_lage_trafo' AS id,
       "BGT_OBW_lage_trafo".bgt_type AS type,
       ST_makeValid("BGT_OBW_lage_trafo".geometrie) AS geometrie,
       "BGT_OBW_lage_trafo".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_lage_trafo"
UNION
SELECT "BGT_OBW_open_loods".identificatie_lokaalid || 'BGT_OBW_open_loods' AS id,
       "BGT_OBW_open_loods".bgt_type AS type,
       ST_makeValid("BGT_OBW_open_loods".geometrie) AS geometrie,
       "BGT_OBW_open_loods".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_open_loods"
UNION
SELECT "BGT_OBW_opslagtank".identificatie_lokaalid || 'BGT_OBW_opslagtank' AS id,
       "BGT_OBW_opslagtank".bgt_type AS type,
       ST_makeValid("BGT_OBW_opslagtank".geometrie) AS geometrie,
       "BGT_OBW_opslagtank".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_opslagtank"
UNION
SELECT "BGT_OBW_overkapping".identificatie_lokaalid || 'BGT_OBW_overkapping' AS id,
       "BGT_OBW_overkapping".bgt_type AS type,
       ST_makeValid("BGT_OBW_overkapping".geometrie) AS geometrie,
       "BGT_OBW_overkapping".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_overkapping"
UNION
SELECT "BGT_OBW_transitie".identificatie_lokaalid || 'BGT_OBW_transitie' AS id,
       "BGT_OBW_transitie".bgt_type AS type,
       ST_makeValid("BGT_OBW_transitie".geometrie) AS geometrie,
       "BGT_OBW_transitie".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_transitie"
UNION
SELECT "BGT_OBW_windturbine".identificatie_lokaalid || 'BGT_OBW_windturbine' AS id,
       "BGT_OBW_windturbine".bgt_type AS type,
       ST_makeValid("BGT_OBW_windturbine".geometrie) AS geometrie,
       "BGT_OBW_windturbine".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OBW_windturbine"
UNION
SELECT "BGT_PND_pand".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                       OVER (PARTITION BY "BGT_PND_pand".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_PND_pand' AS id,
       "BGT_PND_pand".bgt_status AS type,
       ST_makeValid("BGT_PND_pand".geometrie) AS geometrie,
       "BGT_PND_pand".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_PND_pand"
UNION
SELECT "CFT_Onderbouw".guid || 'CFT_Onderbouw' AS identificatie_lokaalid,
       'onderbouw' AS type,
       ST_makeValid("CFT_Onderbouw".geometrie) AS geometrie,
       "CFT_Onderbouw".relatievehoogteligging,
       'cft' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."CFT_Onderbouw"
UNION
SELECT "CFT_Overbouw".guid || 'CFT_Overbouw' AS identificatie_lokaalid,
       'overbouw' AS type,
       ST_makeValid("CFT_Overbouw".geometrie) AS geometrie,
       "CFT_Overbouw".relatievehoogteligging,
       'cft' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."CFT_Overbouw"

/* BAG (uit BGT dataset) */
UNION
SELECT "BAG_Standplaats"."BAG_identificatie" || 'BAG_Standplaats' AS identificatie_lokaalid,
       'standplaats' AS type,
       ST_makeValid("BAG_Standplaats".geometrie) AS geometrie,
       0 AS relatievehoogteligging,
       'bag' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BAG_Standplaats"
UNION
SELECT "BAG_Ligplaats"."BAG_identificatie" || 'BAG_Ligplaats' AS identificatie_lokaalid,
       'ligplaats' AS type,
       ST_makeValid("BAG_Ligplaats".geometrie) AS geometrie,
       0 AS relatievehoogteligging,
       'bag' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BAG_Ligplaats"
UNION
SELECT "GBW_overdekt".ogc_fid::TEXT || 'GBW_overdekt_kbk10' AS identificatie_lokaal_id,
       'overdekt' AS type,
       ST_makeValid("GBW_overdekt".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."GBW_overdekt"
UNION
SELECT "GBW_gebouw".ogc_fid::TEXT || 'GBW_gebouw_kbk10' AS identificatie_lokaal_id,
       'gebouw' AS type,
       ST_makeValid("GBW_gebouw".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."GBW_gebouw"
UNION
SELECT "GBW_hoogbouw".ogc_fid::TEXT || 'GBW_hoogbouw_kbk10' AS identificatie_lokaal_id,
       'hoogbouw' AS type,
       ST_makeValid("GBW_hoogbouw".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."GBW_hoogbouw"
UNION
SELECT "GBW_kas_warenhuis".ogc_fid::TEXT || 'GBW_kas_warenhuis_kbk10' AS identificatie_lokaal_id,
       'kas_warenhuis' AS type,
       ST_makeValid("GBW_kas_warenhuis".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."GBW_kas_warenhuis"
UNION
SELECT "GBW_bebouwing".ogc_fid::TEXT || 'GBW_bebouwing_kbk50' AS identificatie_lokaal_id,
       'bebouwing' AS type,
       ST_makeValid("GBW_bebouwing".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."GBW_bebouwing"
UNION
SELECT "GBW_kassen".ogc_fid::TEXT || 'GBW_kassen_kbk50' AS identificatie_lokaal_id,
       'kassen' AS type,
       ST_makeValid("GBW_kassen".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."GBW_kassen"
"""

SELECT_INRICHTINGSELEMENTLIJN_SQL: Final = """
/*
NOTE: since 23 dec not present any more in the source. Possibly part of BGT instead of BGTPLUS.
SELECT "BGTPLUS_OSDG_damwand".identificatie_lokaalid || 'BGTPLUS_OSDG_damwand' AS id,
       "BGTPLUS_OSDG_damwand".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_damwand".geometrie) AS geometrie,
       "BGTPLUS_OSDG_damwand".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_OSDG_damwand"
UNION
SELECT "BGTPLUS_OSDG_geluidsscherm".identificatie_lokaalid || 'BGTPLUS_OSDG_geluidsscherm' AS id,
       "BGTPLUS_OSDG_geluidsscherm".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_geluidsscherm".geometrie) AS geometrie,
       "BGTPLUS_OSDG_geluidsscherm".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_OSDG_geluidsscherm"
UNION
*/
SELECT "BGTPLUS_OSDG_hek".identificatie_lokaalid || 'BGTPLUS_OSDG_hek' AS id,
       "BGTPLUS_OSDG_hek".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_hek".geometrie) AS geometrie,
       "BGTPLUS_OSDG_hek".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_OSDG_hek"
UNION
SELECT "BGTPLUS_OSDG_kademuur_L".identificatie_lokaalid || 'BGTPLUS_OSDG_kademuur_L' AS id,
       "BGTPLUS_OSDG_kademuur_L".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_kademuur_L".geometrie) AS geometrie,
       "BGTPLUS_OSDG_kademuur_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_OSDG_kademuur_L"
UNION
SELECT "BGTPLUS_OSDG_muur_L".identificatie_lokaalid || 'BGTPLUS_OSDG_muur_L' AS id,
       "BGTPLUS_OSDG_muur_L".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_muur_L".geometrie) AS geometrie,
       "BGTPLUS_OSDG_muur_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_OSDG_muur_L"
UNION
SELECT "BGTPLUS_OSDG_walbescherming".identificatie_lokaalid || 'BGTPLUS_OSDG_walbescherming' AS id,
       "BGTPLUS_OSDG_walbescherming".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_walbescherming".geometrie) AS geometrie,
       "BGTPLUS_OSDG_walbescherming".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_OSDG_walbescherming"
UNION
SELECT "BGTPLUS_SDG_draadraster".identificatie_lokaalid || 'BGTPLUS_SDG_draadraster' AS id,
       "BGTPLUS_SDG_draadraster".plus_type AS type,
       ST_makeValid("BGTPLUS_SDG_draadraster".geometrie) AS geometrie,
       "BGTPLUS_SDG_draadraster".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_SDG_draadraster"
UNION
SELECT "BGTPLUS_SDG_faunaraster".identificatie_lokaalid || 'BGTPLUS_SDG_faunaraster' AS id,
       "BGTPLUS_SDG_faunaraster".plus_type AS type,
       ST_makeValid("BGTPLUS_SDG_faunaraster".geometrie) AS geometrie,
       "BGTPLUS_SDG_faunaraster".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_SDG_faunaraster"
UNION
SELECT "BGTPLUS_VGT_haag_L".identificatie_lokaalid || 'BGTPLUS_VGT_haag_L' AS id,
       "BGTPLUS_VGT_haag_L".plus_type AS type,
       ST_makeValid("BGTPLUS_VGT_haag_L".geometrie) AS geometrie,
       "BGTPLUS_VGT_haag_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_VGT_haag_L"
UNION
SELECT "BGTPLUS_WDI_geleidewerk".identificatie_lokaalid || 'BGTPLUS_WDI_geleidewerk' AS id,
       "BGTPLUS_WDI_geleidewerk".plus_type AS type,
       ST_makeValid("BGTPLUS_WDI_geleidewerk".geometrie) AS geometrie,
       "BGTPLUS_WDI_geleidewerk".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WDI_geleidewerk"
UNION
SELECT "BGTPLUS_WDI_remmingswerk".identificatie_lokaalid || 'BGTPLUS_WDI_remmingswerk' AS id,
       "BGTPLUS_WDI_remmingswerk".plus_type AS type,
       ST_makeValid("BGTPLUS_WDI_remmingswerk".geometrie) AS geometrie,
       "BGTPLUS_WDI_remmingswerk".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WDI_remmingswerk"
UNION
SELECT "BGTPLUS_WGI_balustrade".identificatie_lokaalid || 'BGTPLUS_WGI_balustrade' AS id,
       "BGTPLUS_WGI_balustrade".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_balustrade".geometrie) AS geometrie,
       "BGTPLUS_WGI_balustrade".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_balustrade"
UNION
SELECT "BGTPLUS_WGI_geleideconstructie_L".identificatie_lokaalid ||
       'BGTPLUS_WGI_geleideconstructie_L' AS id,
       "BGTPLUS_WGI_geleideconstructie_L".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_geleideconstructie_L".geometrie) AS geometrie,
       "BGTPLUS_WGI_geleideconstructie_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_geleideconstructie_L"
UNION
SELECT "BGTPLUS_WGI_rooster_L".identificatie_lokaalid || 'BGTPLUS_WGI_rooster_L' AS id,
       "BGTPLUS_WGI_rooster_L".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_rooster_L".geometrie) AS geometrie,
       "BGTPLUS_WGI_rooster_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_rooster_L"
UNION
SELECT "BGTPLUS_WGI_wildrooster_L".identificatie_lokaalid || 'BGTPLUS_WGI_wildrooster_L' AS id,
       "BGTPLUS_WGI_wildrooster_L".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_wildrooster_L".geometrie) AS geometrie,
       "BGTPLUS_WGI_wildrooster_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_wildrooster_L"
UNION
SELECT "BGT_KDL_stuw_L".identificatie_lokaalid || 'BGT_KDL_stuw_L' AS id,
       "BGT_KDL_stuw_L".bgt_type AS type,
       ST_makeValid("BGT_KDL_stuw_L".geometrie) AS geometrie,
       "BGT_KDL_stuw_L".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_KDL_stuw_L"
UNION
SELECT "BGT_SDG_damwand".identificatie_lokaalid || 'BGT_SDG_damwand' AS id,
       "BGT_SDG_damwand".bgt_type AS type,
       ST_makeValid("BGT_SDG_damwand".geometrie) AS geometrie,
       "BGT_SDG_damwand".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SDG_damwand"
UNION
SELECT "BGT_SDG_geluidsscherm".identificatie_lokaalid || 'BGT_SDG_geluidsscherm' AS id,
       "BGT_SDG_geluidsscherm".bgt_type AS type,
       ST_makeValid("BGT_SDG_geluidsscherm".geometrie) AS geometrie,
       "BGT_SDG_geluidsscherm".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SDG_geluidsscherm"
UNION
SELECT "BGT_SDG_hek".identificatie_lokaalid || 'BGT_SDG_hek' AS id,
       "BGT_SDG_hek".bgt_type AS type,
       ST_makeValid("BGT_SDG_hek".geometrie) AS geometrie,
       "BGT_SDG_hek".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SDG_hek"
UNION
SELECT "BGT_SDG_kademuur_L".identificatie_lokaalid || 'BGT_SDG_kademuur_L' AS id,
       "BGT_SDG_kademuur_L".bgt_type AS type,
       ST_makeValid("BGT_SDG_kademuur_L".geometrie) AS geometrie,
       "BGT_SDG_kademuur_L".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SDG_kademuur_L"
UNION
SELECT "BGT_SDG_muur_L".identificatie_lokaalid || 'BGT_SDG_muur_L' AS id,
       "BGT_SDG_muur_L".bgt_type AS type,
       ST_makeValid("BGT_SDG_muur_L".geometrie) AS geometrie,
       "BGT_SDG_muur_L".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SDG_muur_L"
UNION
SELECT "BGT_SDG_walbescherming".identificatie_lokaalid || 'BGT_SDG_walbescherming' AS id,
       "BGT_SDG_walbescherming".bgt_type AS type,
       ST_makeValid("BGT_SDG_walbescherming".geometrie) AS geometrie,
       "BGT_SDG_walbescherming".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SDG_walbescherming"
UNION
SELECT "BGTPLUS_KDL_onbekend_L".identificatie_lokaalid || 'BGTPLUS_KDL_onbekend_L' AS id,
       "BGTPLUS_KDL_onbekend_L".plus_type AS type,
       ST_makeValid("BGTPLUS_KDL_onbekend_L".geometrie) AS geometrie,
       "BGTPLUS_KDL_onbekend_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_KDL_onbekend_L"
UNION
SELECT "BGTPLUS_SDG_onbekend_L".identificatie_lokaalid || 'BGTPLUS_SDG_onbekend_L' AS id,
       "BGTPLUS_SDG_onbekend_L".plus_type AS type,
       ST_makeValid("BGTPLUS_SDG_onbekend_L".geometrie) AS geometrie,
       "BGTPLUS_SDG_onbekend_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_SDG_onbekend_L"
UNION
SELECT "BGTPLUS_VGT_onbekend_L".identificatie_lokaalid || 'BGTPLUS_VGT_onbekend_L' AS id,
       "BGTPLUS_VGT_onbekend_L".plus_type AS type,
       ST_makeValid("BGTPLUS_VGT_onbekend_L".geometrie) AS geometrie,
       "BGTPLUS_VGT_onbekend_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_VGT_onbekend_L"
UNION
SELECT "BGTPLUS_WGI_lijnafwatering".identificatie_lokaalid || 'BGTPLUS_WGI_lijnafwatering' AS id,
       "BGTPLUS_WGI_lijnafwatering".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_lijnafwatering".geometrie) AS geometrie,
       "BGTPLUS_WGI_lijnafwatering".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_lijnafwatering"
UNION
SELECT "BGTPLUS_WGI_molgoot".identificatie_lokaalid || 'BGTPLUS_WGI_molgoot' AS id,
       "BGTPLUS_WGI_molgoot".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_molgoot".geometrie) AS geometrie,
       "BGTPLUS_WGI_molgoot".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_molgoot"

/* ---- KBK10 ---- */
UNION
SELECT "WDL_smal_water_3_tot_6m".ogc_fid::TEXT ||
       'WDL_smal_water_3_tot_6m' AS identificatie_lokaal_id,
       'smal_water_3_tot_6m' AS type,
       "WDL_smal_water_3_tot_6m".geom,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WDL_smal_water_3_tot_6m"
UNION
SELECT "WDL_smal_water_tot_3m".ogc_fid::TEXT || 'WDL_smal_water_tot_3m' AS identificatie_lokaal_id,
       'smal_water_tot_3m' AS type,
       "WDL_smal_water_tot_3m".geom,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WDL_smal_water_tot_3m"
UNION
SELECT "KRT_tunnelcontour".ogc_fid::TEXT || 'KRT_tunnelcontour' AS identificatie_lokaal_id,
       'tunnelcontour' AS type,
       "KRT_tunnelcontour".geom,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."KRT_tunnelcontour"
UNION
SELECT "IRT_aanlegsteiger_smal".ogc_fid::TEXT ||
       'IRT_aanlegsteiger_smal' AS identificatie_lokaal_id,
       'aanlegsteiger_smal' AS type,
       "IRT_aanlegsteiger_smal".geom,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."IRT_aanlegsteiger_smal"
"""


SELECT_INRICHTINGSELEMENTPUNT_SQL: Final = """
SELECT "BGTPLUS_BAK_afvalbak".identificatie_lokaalid || 'BGTPLUS_BAK_afvalbak' AS id,
       "BGTPLUS_BAK_afvalbak".plus_type AS type,
       ST_makeValid("BGTPLUS_BAK_afvalbak".geometrie) AS geometrie,
       "BGTPLUS_BAK_afvalbak".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BAK_afvalbak"
UNION
SELECT "BGTPLUS_BAK_afval_apart_plaats".identificatie_lokaalid ||
       'BGTPLUS_BAK_afval_apart_plaats' AS id,
       "BGTPLUS_BAK_afval_apart_plaats".plus_type AS type,
       ST_makeValid("BGTPLUS_BAK_afval_apart_plaats".geometrie) AS geometrie,
       "BGTPLUS_BAK_afval_apart_plaats".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BAK_afval_apart_plaats"
UNION
SELECT "BGTPLUS_ISE_onbekend".identificatie_lokaalid || 'BGTPLUS_ISE_onbekend' AS id,
       "BGTPLUS_ISE_onbekend".plus_type AS type,
       ST_makeValid("BGTPLUS_ISE_onbekend".geometrie) AS geometrie,
       "BGTPLUS_ISE_onbekend".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_ISE_onbekend"
UNION
SELECT "BGTPLUS_ISE_pomp".identificatie_lokaalid || 'BGTPLUS_ISE_pomp' AS id,
       "BGTPLUS_ISE_pomp".plus_type AS type,
       ST_makeValid("BGTPLUS_ISE_pomp".geometrie) AS geometrie,
       "BGTPLUS_ISE_pomp".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_ISE_pomp"
UNION
SELECT "BGTPLUS_KST_cai-kast".identificatie_lokaalid || 'BGTPLUS_KST_cai-kast' AS id,
       "BGTPLUS_KST_cai-kast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_cai-kast".geometrie) AS geometrie,
       "BGTPLUS_KST_cai-kast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_cai-kast"
UNION
SELECT "BGTPLUS_KST_elektrakast".identificatie_lokaalid || 'BGTPLUS_KST_elektrakast' AS id,
       "BGTPLUS_KST_elektrakast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_elektrakast".geometrie) AS geometrie,
       "BGTPLUS_KST_elektrakast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_elektrakast"
UNION
SELECT "BGTPLUS_KST_onbekend".identificatie_lokaalid || 'BGTPLUS_KST_onbekend' AS id,
       "BGTPLUS_KST_onbekend".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_onbekend".geometrie) AS geometrie,
       "BGTPLUS_KST_onbekend".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_onbekend"
UNION
SELECT "BGTPLUS_PAL_lichtmast".identificatie_lokaalid || 'BGTPLUS_PAL_lichtmast' AS id,
       "BGTPLUS_PAL_lichtmast".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_lichtmast".geometrie) AS geometrie,
       "BGTPLUS_PAL_lichtmast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_lichtmast"
UNION
SELECT "BGTPLUS_PUT_brandkraan_-put".identificatie_lokaalid || 'BGTPLUS_PUT_brandkraan_-put' AS id,
       "BGTPLUS_PUT_brandkraan_-put".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_brandkraan_-put".geometrie) AS geometrie,
       "BGTPLUS_PUT_brandkraan_-put".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_brandkraan_-put"
UNION
SELECT "BGTPLUS_PUT_inspectie-_rioolput".identificatie_lokaalid ||
       'BGTPLUS_PUT_inspectie-_rioolput' AS id,
       "BGTPLUS_PUT_inspectie-_rioolput".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_inspectie-_rioolput".geometrie) AS geometrie,
       "BGTPLUS_PUT_inspectie-_rioolput".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_inspectie-_rioolput"
UNION
SELECT "BGTPLUS_PUT_kolk".identificatie_lokaalid || 'BGTPLUS_PUT_kolk' AS id,
       "BGTPLUS_PUT_kolk".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_kolk".geometrie) AS geometrie,
       "BGTPLUS_PUT_kolk".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_kolk"
UNION
SELECT "BGTPLUS_SMR_abri".identificatie_lokaalid || 'BGTPLUS_SMR_abri' AS id,
       "BGTPLUS_SMR_abri".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_abri".geometrie) AS geometrie,
       "BGTPLUS_SMR_abri".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_abri"
UNION
SELECT "BGTPLUS_SMR_betaalautomaat".identificatie_lokaalid || 'BGTPLUS_SMR_betaalautomaat' AS id,
       "BGTPLUS_SMR_betaalautomaat".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_betaalautomaat".geometrie) AS geometrie,
       "BGTPLUS_SMR_betaalautomaat".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_betaalautomaat"
UNION
SELECT "BGTPLUS_SMR_fietsenrek".identificatie_lokaalid || 'BGTPLUS_SMR_fietsenrek' AS id,
       "BGTPLUS_SMR_fietsenrek".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_fietsenrek".geometrie) AS geometrie,
       "BGTPLUS_SMR_fietsenrek".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_fietsenrek"
UNION
SELECT "BGTPLUS_SMR_herdenkingsmonument".identificatie_lokaalid ||
       'BGTPLUS_SMR_herdenkingsmonument' AS id,
       "BGTPLUS_SMR_herdenkingsmonument".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_herdenkingsmonument".geometrie) AS geometrie,
       "BGTPLUS_SMR_herdenkingsmonument".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_herdenkingsmonument"
UNION
SELECT "BGTPLUS_SMR_kunstobject".identificatie_lokaalid || 'BGTPLUS_SMR_kunstobject' AS id,
       "BGTPLUS_SMR_kunstobject".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_kunstobject".geometrie) AS geometrie,
       "BGTPLUS_SMR_kunstobject".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_kunstobject"
UNION
SELECT "BGTPLUS_SMR_openbaar_toilet".identificatie_lokaalid || 'BGTPLUS_SMR_openbaar_toilet' AS id,
       "BGTPLUS_SMR_openbaar_toilet".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_openbaar_toilet".geometrie) AS geometrie,
       "BGTPLUS_SMR_openbaar_toilet".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_openbaar_toilet"
UNION
SELECT "BGTPLUS_SMR_reclamezuil".identificatie_lokaalid || 'BGTPLUS_SMR_reclamezuil' AS id,
       "BGTPLUS_SMR_reclamezuil".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_reclamezuil".geometrie) AS geometrie,
       "BGTPLUS_SMR_reclamezuil".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_reclamezuil"
UNION
SELECT "BGTPLUS_SMR_telefooncel".identificatie_lokaalid || 'BGTPLUS_SMR_telefooncel' AS id,
       "BGTPLUS_SMR_telefooncel".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_telefooncel".geometrie) AS geometrie,
       "BGTPLUS_SMR_telefooncel".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_telefooncel"
UNION
SELECT "BGTPLUS_VGT_boom".identificatie_lokaalid || 'BGTPLUS_VGT_boom' AS id,
       "BGTPLUS_VGT_boom".plus_type AS type,
       ST_makeValid("BGTPLUS_VGT_boom".geometrie) AS geometrie,
       "BGTPLUS_VGT_boom".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_VGT_boom"
UNION
SELECT "BGT_KDL_hoogspanningsmast_P".identificatie_lokaalid || 'BGT_KDL_hoogspanningsmast_P' AS id,
       "BGT_KDL_hoogspanningsmast_P".bgt_type AS type,
       ST_makeValid("BGT_KDL_hoogspanningsmast_P".geometrie) AS geometrie,
       "BGT_KDL_hoogspanningsmast_P".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGT_KDL_hoogspanningsmast_P"
UNION
SELECT "BGTPLUS_BRD_informatiebord".identificatie_lokaalid || 'BGTPLUS_BRD_informatiebord' AS id,
       "BGTPLUS_BRD_informatiebord".plus_type AS type,
       ST_makeValid("BGTPLUS_BRD_informatiebord".geometrie) AS geometrie,
       "BGTPLUS_BRD_informatiebord".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BRD_informatiebord"
UNION
SELECT "BGTPLUS_BRD_reclamebord".identificatie_lokaalid || 'BGTPLUS_BRD_reclamebord' AS id,
       "BGTPLUS_BRD_reclamebord".plus_type AS type,
       ST_makeValid("BGTPLUS_BRD_reclamebord".geometrie) AS geometrie,
       "BGTPLUS_BRD_reclamebord".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BRD_reclamebord"
UNION
SELECT "BGTPLUS_BRD_straatnaambord".identificatie_lokaalid || 'BGTPLUS_BRD_straatnaambord' AS id,
       "BGTPLUS_BRD_straatnaambord".plus_type AS type,
       ST_makeValid("BGTPLUS_BRD_straatnaambord".geometrie) AS geometrie,
       "BGTPLUS_BRD_straatnaambord".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BRD_straatnaambord"
UNION
SELECT "BGTPLUS_BRD_verkeersbord".identificatie_lokaalid || 'BGTPLUS_BRD_verkeersbord' AS id,
       "BGTPLUS_BRD_verkeersbord".plus_type AS type,
       ST_makeValid("BGTPLUS_BRD_verkeersbord".geometrie) AS geometrie,
       "BGTPLUS_BRD_verkeersbord".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BRD_verkeersbord"
UNION
SELECT "BGTPLUS_BRD_verklikker_transportleiding".identificatie_lokaalid ||
       'BGTPLUS_BRD_verklikker_transportleiding' AS id,
       "BGTPLUS_BRD_verklikker_transportleiding".plus_type AS type,
       ST_makeValid("BGTPLUS_BRD_verklikker_transportleiding".geometrie) AS geometrie,
       "BGTPLUS_BRD_verklikker_transportleiding".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BRD_verklikker_transportleiding"
UNION
SELECT "BGTPLUS_BRD_wegwijzer".identificatie_lokaalid || 'BGTPLUS_BRD_wegwijzer' AS id,
       "BGTPLUS_BRD_wegwijzer".plus_type AS type,
       ST_makeValid("BGTPLUS_BRD_wegwijzer".geometrie) AS geometrie,
       "BGTPLUS_BRD_wegwijzer".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_BRD_wegwijzer"
UNION
SELECT "BGTPLUS_KST_gaskast".identificatie_lokaalid || 'BGTPLUS_KST_gaskast' AS id,
       "BGTPLUS_KST_gaskast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_gaskast".geometrie) AS geometrie,
       "BGTPLUS_KST_gaskast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_gaskast"
UNION
SELECT "BGTPLUS_KST_gms_kast".identificatie_lokaalid || 'BGTPLUS_KST_gms_kast' AS id,
       "BGTPLUS_KST_gms_kast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_gms_kast".geometrie) AS geometrie,
       "BGTPLUS_KST_gms_kast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_gms_kast"
UNION
SELECT "BGTPLUS_KST_openbare_verlichtingkast".identificatie_lokaalid ||
       'BGTPLUS_KST_openbare_verlichtingkast' AS id,
       "BGTPLUS_KST_openbare_verlichtingkast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_openbare_verlichtingkast".geometrie) AS geometrie,
       "BGTPLUS_KST_openbare_verlichtingkast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_openbare_verlichtingkast"
UNION
SELECT "BGTPLUS_KST_rioolkast".identificatie_lokaalid || 'BGTPLUS_KST_rioolkast' AS id,
       "BGTPLUS_KST_rioolkast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_rioolkast".geometrie) AS geometrie,
       "BGTPLUS_KST_rioolkast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_rioolkast"
UNION
SELECT "BGTPLUS_KST_telecom_kast".identificatie_lokaalid || 'BGTPLUS_KST_telecom_kast' AS id,
       "BGTPLUS_KST_telecom_kast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_telecom_kast".geometrie) AS geometrie,
       "BGTPLUS_KST_telecom_kast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_telecom_kast"
UNION
SELECT "BGTPLUS_KST_telkast".identificatie_lokaalid || 'BGTPLUS_KST_telkast' AS id,
       "BGTPLUS_KST_telkast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_telkast".geometrie) AS geometrie,
       "BGTPLUS_KST_telkast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_telkast"
UNION
SELECT "BGTPLUS_KST_verkeersregelinstallatiekast".identificatie_lokaalid ||
       'BGTPLUS_KST_verkeersregelinstallatiekast' AS id,
       "BGTPLUS_KST_verkeersregelinstallatiekast".plus_type AS type,
       ST_makeValid("BGTPLUS_KST_verkeersregelinstallatiekast".geometrie) AS geometrie,
       "BGTPLUS_KST_verkeersregelinstallatiekast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_KST_verkeersregelinstallatiekast"
UNION
SELECT "BGTPLUS_MST_onbekend".identificatie_lokaalid || 'BGTPLUS_MST_onbekend' AS id,
       "BGTPLUS_MST_onbekend".plus_type AS type,
       ST_makeValid("BGTPLUS_MST_onbekend".geometrie) AS geometrie,
       "BGTPLUS_MST_onbekend".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_MST_onbekend"
UNION
SELECT "BGTPLUS_MST_zendmast".identificatie_lokaalid || 'BGTPLUS_MST_zendmast' AS id,
       "BGTPLUS_MST_zendmast".plus_type AS type,
       ST_makeValid("BGTPLUS_MST_zendmast".geometrie) AS geometrie,
       "BGTPLUS_MST_zendmast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_MST_zendmast"
UNION
SELECT "BGTPLUS_PAL_afsluitpaal".identificatie_lokaalid || 'BGTPLUS_PAL_afsluitpaal' AS id,
       "BGTPLUS_PAL_afsluitpaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_afsluitpaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_afsluitpaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_afsluitpaal"
UNION
SELECT "BGTPLUS_PAL_drukknoppaal".identificatie_lokaalid || 'BGTPLUS_PAL_drukknoppaal' AS id,
       "BGTPLUS_PAL_drukknoppaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_drukknoppaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_drukknoppaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_drukknoppaal"
UNION
SELECT "BGTPLUS_PAL_haltepaal".identificatie_lokaalid || 'BGTPLUS_PAL_haltepaal' AS id,
       "BGTPLUS_PAL_haltepaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_haltepaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_haltepaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_haltepaal"
UNION
SELECT "BGTPLUS_PAL_hectometerpaal".identificatie_lokaalid || 'BGTPLUS_PAL_hectometerpaal' AS id,
       "BGTPLUS_PAL_hectometerpaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_hectometerpaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_hectometerpaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_hectometerpaal"
UNION
SELECT "BGTPLUS_PAL_poller".identificatie_lokaalid || 'BGTPLUS_PAL_poller' AS id,
       "BGTPLUS_PAL_poller".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_poller".geometrie) AS geometrie,
       "BGTPLUS_PAL_poller".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_poller"
UNION
SELECT "BGTPLUS_PAL_telpaal".identificatie_lokaalid || 'BGTPLUS_PAL_telpaal' AS id,
       "BGTPLUS_PAL_telpaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_telpaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_telpaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_telpaal"
UNION
SELECT "BGTPLUS_PAL_verkeersbordpaal".identificatie_lokaalid ||
       'BGTPLUS_PAL_verkeersbordpaal' AS id,
       "BGTPLUS_PAL_verkeersbordpaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_verkeersbordpaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_verkeersbordpaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_verkeersbordpaal"
UNION
SELECT "BGTPLUS_PAL_verkeersregelinstallatiepaal".identificatie_lokaalid ||
       'BGTPLUS_PAL_verkeersregelinstallatiepaal' AS id,
       "BGTPLUS_PAL_verkeersregelinstallatiepaal".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_verkeersregelinstallatiepaal".geometrie) AS geometrie,
       "BGTPLUS_PAL_verkeersregelinstallatiepaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_verkeersregelinstallatiepaal"
UNION
SELECT "BGTPLUS_PAL_vlaggenmast".identificatie_lokaalid || 'BGTPLUS_PAL_vlaggenmast' AS id,
       "BGTPLUS_PAL_vlaggenmast".plus_type AS type,
       ST_makeValid("BGTPLUS_PAL_vlaggenmast".geometrie) AS geometrie,
       "BGTPLUS_PAL_vlaggenmast".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PAL_vlaggenmast"
UNION
SELECT "BGTPLUS_PUT_benzine-_olieput".identificatie_lokaalid ||
       'BGTPLUS_PUT_benzine-_olieput' AS id,
       "BGTPLUS_PUT_benzine-_olieput".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_benzine-_olieput".geometrie) AS geometrie,
       "BGTPLUS_PUT_benzine-_olieput".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_benzine-_olieput"
UNION
SELECT "BGTPLUS_PUT_drainageput".identificatie_lokaalid || 'BGTPLUS_PUT_drainageput' AS id,
       "BGTPLUS_PUT_drainageput".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_drainageput".geometrie) AS geometrie,
       "BGTPLUS_PUT_drainageput".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_drainageput"
UNION
SELECT "BGTPLUS_PUT_gasput".identificatie_lokaalid || 'BGTPLUS_PUT_gasput' AS id,
       "BGTPLUS_PUT_gasput".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_gasput".geometrie) AS geometrie,
       "BGTPLUS_PUT_gasput".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_gasput"
UNION
SELECT "BGTPLUS_PUT_onbekend".identificatie_lokaalid || 'BGTPLUS_PUT_onbekend' AS id,
       "BGTPLUS_PUT_onbekend".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_onbekend".geometrie) AS geometrie,
       "BGTPLUS_PUT_onbekend".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_onbekend"
UNION
SELECT "BGTPLUS_PUT_waterleidingput".identificatie_lokaalid || 'BGTPLUS_PUT_waterleidingput' AS id,
       "BGTPLUS_PUT_waterleidingput".plus_type AS type,
       ST_makeValid("BGTPLUS_PUT_waterleidingput".geometrie) AS geometrie,
       "BGTPLUS_PUT_waterleidingput".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_PUT_waterleidingput"
UNION
SELECT "BGTPLUS_SMR_bank".identificatie_lokaalid || 'BGTPLUS_SMR_bank' AS id,
       "BGTPLUS_SMR_bank".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_bank".geometrie) AS geometrie,
       "BGTPLUS_SMR_bank".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_bank"
UNION
SELECT "BGTPLUS_SMR_brievenbus".identificatie_lokaalid || 'BGTPLUS_SMR_brievenbus' AS id,
       "BGTPLUS_SMR_brievenbus".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_brievenbus".geometrie) AS geometrie,
       "BGTPLUS_SMR_brievenbus".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_brievenbus"
UNION
SELECT "BGTPLUS_SMR_fietsenkluis".identificatie_lokaalid || 'BGTPLUS_SMR_fietsenkluis' AS id,
       "BGTPLUS_SMR_fietsenkluis".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_fietsenkluis".geometrie) AS geometrie,
       "BGTPLUS_SMR_fietsenkluis".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_fietsenkluis"
UNION
SELECT "BGTPLUS_SMR_lichtpunt".identificatie_lokaalid || 'BGTPLUS_SMR_lichtpunt' AS id,
       "BGTPLUS_SMR_lichtpunt".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_lichtpunt".geometrie) AS geometrie,
       "BGTPLUS_SMR_lichtpunt".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_lichtpunt"
UNION
SELECT "BGTPLUS_SMR_slagboom".identificatie_lokaalid || 'BGTPLUS_SMR_slagboom' AS id,
       "BGTPLUS_SMR_slagboom".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_slagboom".geometrie) AS geometrie,
       "BGTPLUS_SMR_slagboom".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_slagboom"
UNION
SELECT "BGTPLUS_SMR_speelvoorziening".identificatie_lokaalid ||
       'BGTPLUS_SMR_speelvoorziening' AS id,
       "BGTPLUS_SMR_speelvoorziening".plus_type AS type,
       ST_makeValid("BGTPLUS_SMR_speelvoorziening".geometrie) AS geometrie,
       "BGTPLUS_SMR_speelvoorziening".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SMR_speelvoorziening"
UNION
SELECT "BGTPLUS_SSR_camera".identificatie_lokaalid || 'BGTPLUS_SSR_camera' AS id,
       "BGTPLUS_SSR_camera".plus_type AS type,
       ST_makeValid("BGTPLUS_SSR_camera".geometrie) AS geometrie,
       "BGTPLUS_SSR_camera".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SSR_camera"
UNION
SELECT "BGTPLUS_SSR_flitser".identificatie_lokaalid || 'BGTPLUS_SSR_flitser' AS id,
       "BGTPLUS_SSR_flitser".plus_type AS type,
       ST_makeValid("BGTPLUS_SSR_flitser".geometrie) AS geometrie,
       "BGTPLUS_SSR_flitser".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SSR_flitser"
UNION
SELECT "BGTPLUS_SSR_waterstandmeter".identificatie_lokaalid || 'BGTPLUS_SSR_waterstandmeter' AS id,
       "BGTPLUS_SSR_waterstandmeter".plus_type AS type,
       ST_makeValid("BGTPLUS_SSR_waterstandmeter".geometrie) AS geometrie,
       "BGTPLUS_SSR_waterstandmeter".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_SSR_waterstandmeter"
UNION
SELECT "BGTPLUS_WDI_meerpaal".identificatie_lokaalid || 'BGTPLUS_WDI_meerpaal' AS id,
       "BGTPLUS_WDI_meerpaal".plus_type AS type,
       ST_makeValid("BGTPLUS_WDI_meerpaal".geometrie) AS geometrie,
       "BGTPLUS_WDI_meerpaal".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL AS maxzoom
FROM bgt."BGTPLUS_WDI_meerpaal"
"""

SELECT_INRICHTINGSELEMENTVLAK_SQL: Final = """
SELECT "BGTPLUS_KDL_keermuur".identificatie_lokaalid || 'BGTPLUS_KDL_keermuur' AS id,
       "BGTPLUS_KDL_keermuur".plus_type AS type,
       ST_makeValid("BGTPLUS_KDL_keermuur".geometrie) AS geometrie,
       "BGTPLUS_KDL_keermuur".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_KDL_keermuur"
UNION
SELECT "BGTPLUS_OSDG_muur_V".identificatie_lokaalid || 'BGTPLUS_OSDG_muur_V' AS id,
       "BGTPLUS_OSDG_muur_V".plus_type AS type,
       ST_makeValid("BGTPLUS_OSDG_muur_V".geometrie) AS geometrie,
       "BGTPLUS_OSDG_muur_V".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_OSDG_muur_V"
UNION
SELECT "BGTPLUS_VGT_haag_V".identificatie_lokaalid || 'BGTPLUS_VGT_haag_V' AS id,
       "BGTPLUS_VGT_haag_V".plus_type AS type,
       ST_makeValid("BGTPLUS_VGT_haag_V".geometrie) AS geometrie,
       "BGTPLUS_VGT_haag_V".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_VGT_haag_V"
UNION
SELECT "BGTPLUS_WGI_boomspiegel_V".identificatie_lokaalid || 'BGTPLUS_WGI_boomspiegel_V' AS id,
       "BGTPLUS_WGI_boomspiegel_V".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_boomspiegel_V".geometrie) AS geometrie,
       "BGTPLUS_WGI_boomspiegel_V".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_WGI_boomspiegel_V"
UNION
SELECT "BGTPLUS_WGI_rooster_V".identificatie_lokaalid || 'BGTPLUS_WGI_rooster_V' AS id,
       "BGTPLUS_WGI_rooster_V".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_rooster_V".geometrie) AS geometrie,
       "BGTPLUS_WGI_rooster_V".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_WGI_rooster_V"
UNION
SELECT "BGTPLUS_WGI_wildrooster_V".identificatie_lokaalid || 'BGTPLUS_WGI_wildrooster_V' AS id,
       "BGTPLUS_WGI_wildrooster_V".plus_type AS type,
       ST_makeValid("BGTPLUS_WGI_wildrooster_V".geometrie) AS geometrie,
       "BGTPLUS_WGI_wildrooster_V".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_WGI_wildrooster_V"
UNION
SELECT "BGT_KDL_gemaal".identificatie_lokaalid || 'BGT_KDL_gemaal' AS id,
       "BGT_KDL_gemaal".bgt_type AS type,
       ST_makeValid("BGT_KDL_gemaal".geometrie) AS geometrie,
       "BGT_KDL_gemaal".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_gemaal"
UNION
SELECT "BGT_KDL_hoogspanningsmast_V".identificatie_lokaalid || 'BGT_KDL_hoogspanningsmast_V' AS id,
       "BGT_KDL_hoogspanningsmast_V".bgt_type AS type,
       ST_makeValid("BGT_KDL_hoogspanningsmast_V".geometrie) AS geometrie,
       "BGT_KDL_hoogspanningsmast_V".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_hoogspanningsmast_V"
UNION
SELECT "BGT_KDL_sluis".identificatie_lokaalid || 'BGT_KDL_sluis' AS id,
       "BGT_KDL_sluis".bgt_type AS type,
       ST_makeValid("BGT_KDL_sluis".geometrie) AS geometrie,
       "BGT_KDL_sluis".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_sluis"
UNION
SELECT "BGT_KDL_steiger".identificatie_lokaalid || 'BGT_KDL_steiger' AS id,
       "BGT_KDL_steiger".bgt_type AS type,
       ST_makeValid("BGT_KDL_steiger".geometrie) AS geometrie,
       "BGT_KDL_steiger".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_steiger"
UNION
SELECT "BGT_KDL_stuw_V".identificatie_lokaalid || 'BGT_KDL_stuw_V' AS id,
       "BGT_KDL_stuw_V".bgt_type AS type,
       ST_makeValid("BGT_KDL_stuw_V".geometrie) AS geometrie,
       "BGT_KDL_stuw_V".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_stuw_V"
UNION
SELECT "BGT_SDG_kademuur_V".identificatie_lokaalid || 'BGT_SDG_kademuur_V' AS id,
       "BGT_SDG_kademuur_V".bgt_type AS type,
       ST_makeValid("BGT_SDG_kademuur_V".geometrie) AS geometrie,
       "BGT_SDG_kademuur_V".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_SDG_kademuur_V"
UNION
SELECT "BGT_SDG_muur_V".identificatie_lokaalid || 'BGT_SDG_muur_V' AS id,
       "BGT_SDG_muur_V".bgt_type AS type,
       ST_makeValid("BGT_SDG_muur_V".geometrie) AS geometrie,
       "BGT_SDG_muur_V".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_SDG_muur_V"

    /* KBK10 */
UNION
SELECT "WDL_waterbassin".ogc_fid::TEXT || 'WDL_waterbassin' AS identificatie_lokaal_id,
       'waterbassin' AS type,
       ST_makeValid("WDL_waterbassin".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WDL_waterbassin"
UNION
SELECT "TRN_aanlegsteiger".ogc_fid::TEXT || 'TRN_aanlegsteiger_kbk10' AS identificatie_lokaal_id,
       'aanlegsteiger' AS type,
       ST_makeValid("TRN_aanlegsteiger".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_aanlegsteiger"
"""

SELECT_SPOORLIJN_SQL: Final = """
SELECT "BGT_SPR_sneltram".identificatie_lokaalid || 'BGT_SPR_sneltram' AS id,
       "BGT_SPR_sneltram".bgt_functie AS type,
       ST_makeValid("BGT_SPR_sneltram".geometrie) AS geometrie,
       "BGT_SPR_sneltram".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SPR_sneltram"
WHERE 1 = 1
UNION
SELECT "BGT_SPR_tram".identificatie_lokaalid || 'BGT_SPR_tram' AS id,
       "BGT_SPR_tram".bgt_functie AS type,
       ST_makeValid("BGT_SPR_tram".geometrie) AS geometrie,
       "BGT_SPR_tram".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SPR_tram"
UNION
SELECT "BGT_SPR_trein".identificatie_lokaalid || 'BGT_SPR_trein' AS id,
       "BGT_SPR_trein".bgt_functie AS type,
       ST_makeValid("BGT_SPR_trein".geometrie) AS geometrie,
       "BGT_SPR_trein".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_SPR_trein"

UNION

/* KBK10 */
SELECT "SBL_metro_overdekt".ogc_fid::TEXT || 'SBL_metro_overdekt' AS identificatie_lokaal_id,
       'SBL_metro_overdekt' AS type,
       ST_makeValid("SBL_metro_overdekt".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_metro_overdekt"
UNION
SELECT "SBL_trein_overdekt_1sp".ogc_fid::TEXT ||
       'SBL_trein_overdekt_1sp' AS identificatie_lokaal_id,
       'trein_overdekt_1sp' AS type,
       ST_makeValid("SBL_trein_overdekt_1sp".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_trein_overdekt_1sp"
UNION
SELECT "SBL_trein_overdekt_nsp".ogc_fid::TEXT ||
       'SBL_trein_overdekt_nsp' AS identificatie_lokaal_id,
       'SBL_trein_overdekt_nsp' AS type,
       ST_makeValid("SBL_trein_overdekt_nsp".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_trein_overdekt_nsp"
UNION
SELECT "SBL_metro_nietoverdekt_1sp".ogc_fid::TEXT ||
       'SBL_metro_nietoverdekt_1sp' AS identificatie_lokaal_id,
       'SBL_metro_nietoverdekt_1sp' AS type,
       ST_makeValid("SBL_metro_nietoverdekt_1sp".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_metro_nietoverdekt_1sp"
UNION
SELECT "SBL_metro_nietoverdekt_nsp".ogc_fid::TEXT ||
       'SBL_metro_nietoverdekt_nsp' AS identificatie_lokaal_id,
       'SBL_metro_nietoverdekt_nsp' AS type,
       ST_makeValid("SBL_metro_nietoverdekt_nsp".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_metro_nietoverdekt_nsp"
UNION
SELECT "SBL_trein_ongeelektrificeerd".ogc_fid::TEXT ||
       'SBL_trein_ongeelektrificeerd_kbk10' AS identificatie_lokaal_id,
       'SBL_trein_ongeelektrificeerd' AS type,
       ST_makeValid("SBL_trein_ongeelektrificeerd".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_trein_ongeelektrificeerd"
UNION
SELECT "SBL_trein_nietoverdekt_1sp".ogc_fid::TEXT ||
       'SBL_trein_nietoverdekt_1sp' AS identificatie_lokaal_id,
       'SBL_trein_nietoverdekt_1sp' AS type,
       ST_makeValid("SBL_trein_nietoverdekt_1sp".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_trein_nietoverdekt_1sp"
UNION
SELECT "SBL_trein_nietoverdekt_nsp".ogc_fid::TEXT ||
       'SBL_trein_nietoverdekt_nsp' AS identificatie_lokaal_id,
       'SBL_trein_nietoverdekt_nsp' AS type,
       ST_makeValid("SBL_trein_nietoverdekt_nsp".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."SBL_trein_nietoverdekt_nsp"

/* KBK50 */
UNION
SELECT "SBL_metro_sneltram_in_tunnel".ogc_fid::TEXT ||
       'SBL_metro_sneltram_in_tunnel' AS identificatie_lokaal_id,
       'metro_sneltram_in_tunnel' AS type,
       ST_makeValid("SBL_metro_sneltram_in_tunnel".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       NULL::INT AS minzoom,
       12 AS maxzoom
FROM kbk50."SBL_metro_sneltram_in_tunnel"
UNION
SELECT "SBL_trein_in_tunnel".ogc_fid::TEXT || 'SBL_trein_in_tunnel' AS identificatie_lokaal_id,
       'trein_in_tunnel' AS type,
       ST_makeValid("SBL_trein_in_tunnel".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       NULL::INT AS minzoom,
       12 AS maxzoom
FROM kbk50."SBL_trein_in_tunnel"
UNION
SELECT "SBL_metro_sneltram".ogc_fid::TEXT || 'SBL_metro_sneltram' AS identificatie_lokaal_id,
       'metro_sneltram' AS type,
       ST_makeValid("SBL_metro_sneltram".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       NULL::INT AS minzoom,
       12 AS maxzoom
FROM kbk50."SBL_metro_sneltram"
UNION
SELECT "SBL_trein".ogc_fid::TEXT || 'SBL_trein' AS identificatie_lokaal_id,
       'trein' AS type,
       ST_makeValid("SBL_trein".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       NULL::INT AS minzoom,
       12 AS maxzoom
FROM kbk50."SBL_trein"
"""

SELECT_TERREINDEELVLAK_SQL: Final = """
SELECT "BGT_BTRN_boomteelt".identificatie_lokaalid || 'BGT_BTRN_boomteelt' AS id,
       "BGT_BTRN_boomteelt".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_boomteelt".geometrie) AS geometrie,
       "BGT_BTRN_boomteelt".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_boomteelt"
UNION
SELECT "BGT_BTRN_bouwland".identificatie_lokaalid || 'BGT_BTRN_bouwland' AS id,
       "BGT_BTRN_bouwland".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_bouwland".geometrie) AS geometrie,
       "BGT_BTRN_bouwland".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_bouwland"
UNION
SELECT "BGT_BTRN_fruitteelt".identificatie_lokaalid || 'BGT_BTRN_fruitteelt' AS id,
       "BGT_BTRN_fruitteelt".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_fruitteelt".geometrie) AS geometrie,
       "BGT_BTRN_fruitteelt".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_fruitteelt"
UNION
SELECT "BGT_BTRN_gemengd_bos".identificatie_lokaalid || 'BGT_BTRN_gemengd_bos' AS id,
       "BGT_BTRN_gemengd_bos".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_gemengd_bos".geometrie) AS geometrie,
       "BGT_BTRN_gemengd_bos".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_gemengd_bos"
UNION
SELECT "BGT_BTRN_grasland_agrarisch".identificatie_lokaalid || '-' ||
       "BGT_BTRN_grasland_agrarisch".tijdstipregistratie || '-' ||
       'BGT_BTRN_grasland_agrarisch' AS id,
       "BGT_BTRN_grasland_agrarisch".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_grasland_agrarisch".geometrie) AS geometrie,
       "BGT_BTRN_grasland_agrarisch".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_grasland_agrarisch"
UNION
SELECT "BGT_BTRN_grasland_overig".identificatie_lokaalid || '-' ||
       "BGT_BTRN_grasland_overig".tijdstipregistratie || '-' || 'BGT_BTRN_grasland_overig' AS id,
       "BGT_BTRN_grasland_overig".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_grasland_overig".geometrie) AS geometrie,
       "BGT_BTRN_grasland_overig".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_grasland_overig"
UNION
SELECT "BGT_BTRN_groenvoorziening".identificatie_lokaalid || 'BGT_BTRN_groenvoorziening' AS id,
       "BGT_BTRN_groenvoorziening".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_groenvoorziening".geometrie) AS geometrie,
       "BGT_BTRN_groenvoorziening".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_groenvoorziening"
UNION
SELECT "BGT_BTRN_houtwal".identificatie_lokaalid || 'BGT_BTRN_houtwal' AS id,
       "BGT_BTRN_houtwal".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_houtwal".geometrie) AS geometrie,
       "BGT_BTRN_houtwal".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_houtwal"
UNION
SELECT "BGT_BTRN_loofbos".identificatie_lokaalid || 'BGT_BTRN_loofbos' AS id,
       "BGT_BTRN_loofbos".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_loofbos".geometrie) AS geometrie,
       "BGT_BTRN_loofbos".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_loofbos"
UNION
SELECT "BGT_BTRN_moeras".identificatie_lokaalid || 'BGT_BTRN_moeras' AS id,
       "BGT_BTRN_moeras".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_moeras".geometrie) AS geometrie,
       "BGT_BTRN_moeras".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_moeras"
UNION
SELECT "BGT_BTRN_naaldbos".identificatie_lokaalid || 'BGT_BTRN_naaldbos' AS id,
       "BGT_BTRN_naaldbos".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_naaldbos".geometrie) AS geometrie,
       "BGT_BTRN_naaldbos".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_naaldbos"
UNION
SELECT "BGT_BTRN_rietland".identificatie_lokaalid || 'BGT_BTRN_rietland' AS id,
       "BGT_BTRN_rietland".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_rietland".geometrie) AS geometrie,
       "BGT_BTRN_rietland".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_rietland"
UNION
SELECT "BGT_BTRN_struiken".identificatie_lokaalid || 'BGT_BTRN_struiken' AS id,
       "BGT_BTRN_struiken".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_struiken".geometrie) AS geometrie,
       "BGT_BTRN_struiken".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_struiken"
UNION
SELECT "BGT_KDL_perron".identificatie_lokaalid || 'BGT_KDL_perron' AS id,
       "BGT_KDL_perron".bgt_type AS type,
       ST_makeValid("BGT_KDL_perron".geometrie) AS geometrie,
       "BGT_KDL_perron".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_perron"
UNION
SELECT "BGT_KDL_strekdam".identificatie_lokaalid || 'BGT_KDL_strekdam' AS id,
       "BGT_KDL_strekdam".bgt_type AS type,
       ST_makeValid("BGT_KDL_strekdam".geometrie) AS geometrie,
       "BGT_KDL_strekdam".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_KDL_strekdam"
UNION
SELECT "BGT_OTRN_erf".identificatie_lokaalid || '-' || "BGT_OTRN_erf".tijdstipregistratie || '-' ||
       'BGT_OTRN_erf' AS id,
       "BGT_OTRN_erf".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_OTRN_erf".geometrie) AS geometrie,
       "BGT_OTRN_erf".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OTRN_erf"
UNION
SELECT "BGT_OTRN_gesloten_verharding".identificatie_lokaalid ||
       'BGT_OTRN_gesloten_verharding' AS id,
       "BGT_OTRN_gesloten_verharding".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_OTRN_gesloten_verharding".geometrie) AS geometrie,
       "BGT_OTRN_gesloten_verharding".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OTRN_gesloten_verharding"
UNION
SELECT "BGT_OTRN_half_verhard".identificatie_lokaalid || 'BGT_OTRN_half_verhard' AS id,
       "BGT_OTRN_half_verhard".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_OTRN_half_verhard".geometrie) AS geometrie,
       "BGT_OTRN_half_verhard".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OTRN_half_verhard"
UNION
SELECT "BGT_OTRN_onverhard".identificatie_lokaalid || 'BGT_OTRN_onverhard' AS id,
       "BGT_OTRN_onverhard".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_OTRN_onverhard".geometrie) AS geometrie,
       "BGT_OTRN_onverhard".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OTRN_onverhard"
UNION
SELECT "BGT_OTRN_open_verharding".identificatie_lokaalid || 'BGT_OTRN_open_verharding' AS id,
       "BGT_OTRN_open_verharding".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_OTRN_open_verharding".geometrie) AS geometrie,
       "BGT_OTRN_open_verharding".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OTRN_open_verharding"
UNION
SELECT "BGT_OTRN_zand".identificatie_lokaalid || 'BGT_OTRN_zand' AS id,
       "BGT_OTRN_zand".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_OTRN_zand".geometrie) AS geometrie,
       "BGT_OTRN_zand".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OTRN_zand"
UNION
SELECT "BGT_OWDL_oever_slootkant".identificatie_lokaalid || '-' ||
       "BGT_OWDL_oever_slootkant".tijdstipregistratie || '-' || 'BGT_OWDL_oever_slootkant' AS id,
       "BGT_OWDL_oever_slootkant".bgt_type AS type,
       ST_makeValid("BGT_OWDL_oever_slootkant".geometrie) AS geometrie,
       "BGT_OWDL_oever_slootkant".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_OWDL_oever_slootkant"
UNION
SELECT "BGT_WGL_spoorbaan".identificatie_lokaalid || 'BGT_WGL_spoorbaan' AS id,
       "BGT_WGL_spoorbaan".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_WGL_spoorbaan".geometrie) AS geometrie,
       "BGT_WGL_spoorbaan".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_WGL_spoorbaan"
UNION
SELECT "BGT_BTRN_heide".identificatie_lokaalid || 'BGT_BTRN_heide' AS id,
       "BGT_BTRN_heide".bgt_fysiekvoorkomen AS type,
       ST_makeValid("BGT_BTRN_heide".geometrie) AS geometrie,
       "BGT_BTRN_heide".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_BTRN_heide"

    /* KBK10 */

UNION
SELECT "TRN_basaltblokken_steenglooiing".ogc_fid::TEXT ||
       'TRN_basaltblokken_steenglooiing_kbk10' AS identificatie_lokaal_id,
       'basaltblokken_steenglooiing' AS type,
       ST_makeValid("TRN_basaltblokken_steenglooiing".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_basaltblokken_steenglooiing"
UNION
SELECT "TRN_grasland".ogc_fid::TEXT || 'TRN_grasland_kbk10' AS identificatie_lokaal_id,
       'grasland overig' AS type,
       ST_makeValid("TRN_grasland".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_grasland"
UNION
SELECT "TRN_akkerland".ogc_fid::TEXT || 'TRN_akkerland_kbk10' AS identificatie_lokaal_id,
       'bouwland' AS type,
       ST_makeValid("TRN_akkerland".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_akkerland"
UNION
SELECT "TRN_overig".ogc_fid::TEXT || 'TRN_overig_kbk10' AS identificatie_lokaal_id,
       'overig' AS type,
       ST_makeValid("TRN_overig".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_overig"
UNION
SELECT "TRN_bedrijfsterrein".ogc_fid::TEXT ||
       'TRN_bedrijfsterrein_kbk10' AS identificatie_lokaal_id,
       'bedrijfsterrein' AS type,
       ST_makeValid("TRN_bedrijfsterrein".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_bedrijfsterrein"
UNION
SELECT "TRN_openbaar_groen".ogc_fid::TEXT || 'TRN_openbaar_groen_kbk10' AS identificatie_lokaal_id,
       'groenvoorziening' AS type,
       ST_makeValid("TRN_openbaar_groen".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_openbaar_groen"
UNION
SELECT "TRN_zand".ogc_fid::TEXT || 'TRN_zand_kbk10' AS identificatie_lokaal_id,
       'zand' AS type,
       ST_makeValid("TRN_zand".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_zand"
UNION
SELECT "TRN_bos-loofbos".ogc_fid::TEXT || 'TRN_bos-loofbos_kbk10' AS identificatie_lokaal_id,
       'loofbos' AS type,
       ST_makeValid("TRN_bos-loofbos".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_bos-loofbos"
UNION
SELECT "TRN_bos-naaldbos".ogc_fid::TEXT || 'TRN_bos-naaldbos_kbk10' AS identificatie_lokaal_id,
       'naaldbos' AS type,
       ST_makeValid("TRN_bos-naaldbos".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_bos-naaldbos"
UNION
SELECT "TRN_bos-gemengd_bos".ogc_fid::TEXT ||
       'TRN_bos-gemengd_bos_kbk10' AS identificatie_lokaal_id,
       'gemengd bos' AS type,
       ST_makeValid("TRN_bos-gemengd_bos".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_bos-gemengd_bos"
UNION
SELECT "TRN_bos-griend".ogc_fid::TEXT || 'TRN_bos-griend_kbk10' AS identificatie_lokaal_id,
       'griend' AS type,
       ST_makeValid("TRN_bos-griend".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_bos-griend"
UNION
SELECT "TRN_boomgaard".ogc_fid::TEXT || 'TRN_boomgaard_kbk10' AS identificatie_lokaal_id,
       'boomgaard' AS type,
       ST_makeValid("TRN_boomgaard".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_boomgaard"
UNION
SELECT "TRN_boomkwekerij".ogc_fid::TEXT || 'TRN_boomkwekerij_kbk10' AS identificatie_lokaal_id,
       'boomteelt' AS type,
       ST_makeValid("TRN_boomkwekerij".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_boomkwekerij"
UNION
SELECT "TRN_dodenakker".ogc_fid::TEXT || 'TRN_dodenakker_kbk10' AS identificatie_lokaal_id,
       'dodenakker' AS type,
       ST_makeValid("TRN_dodenakker".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_dodenakker"
UNION
SELECT "TRN_dodenakker_met_bos".ogc_fid::TEXT ||
       'TRN_dodenakker_met_bos_kbk10' AS identificatie_lokaal_id,
       'dodenakker_met_bos' AS type,
       ST_makeValid("TRN_dodenakker_met_bos".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_dodenakker_met_bos"
UNION
SELECT "TRN_fruitkwekerij".ogc_fid::TEXT || 'TRN_fruitkwekerij_kbk10' AS identificatie_lokaal_id,
       'fruitteelt' AS type,
       ST_makeValid("TRN_fruitkwekerij".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_fruitkwekerij"
UNION
SELECT "TRN_binnentuin".ogc_fid::TEXT || 'TRN_binnentuin_kbk10' AS identificatie_lokaal_id,
       'binnentuin' AS type,
       ST_makeValid("TRN_binnentuin".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       15 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_binnentuin"
UNION
SELECT "TRN_agrarisch".ogc_fid::TEXT || 'TRN_agrarisch_kbk50' AS identificatie_lokaal_id,
       'bouwland' AS type,
       ST_makeValid("TRN_agrarisch".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS minzoom
FROM kbk50."TRN_agrarisch"
UNION
SELECT "TRN_overig".ogc_fid::TEXT || 'TRN_overig_kbk50' AS identificatie_lokaal_id,
       'overig' AS type,
       ST_makeValid("TRN_overig".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS minzoom
FROM kbk50."TRN_overig"
UNION
SELECT "TRN_bedrijfsterrein_dienstverlening".ogc_fid::TEXT ||
       'TRN_bedrijfsterrein_dienstverlening_kbk50' AS identificatie_lokaal_id,
       'bedrijfsterrein' AS type,
       ST_makeValid("TRN_bedrijfsterrein_dienstverlening".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS minzoom
FROM kbk50."TRN_bedrijfsterrein_dienstverlening"
UNION
SELECT "TRN_bos_groen_sport".ogc_fid::TEXT ||
       'TRN_bos_groen_sport_kbk50' AS identificatie_lokaal_id,
       'bos_groen_sport' AS type,
       ST_makeValid("TRN_bos_groen_sport".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS minzoom
FROM kbk50."TRN_bos_groen_sport"
UNION
SELECT "TRN_zand".ogc_fid::TEXT || 'TRN_zand_kbk50' AS identificatie_lokaal_id,
       'zand' AS type,
       ST_makeValid("TRN_zand".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS minzoom
FROM kbk50."TRN_zand"
"""


SELECT_WATERDEELLIJN_SQL: Final = """
SELECT "BGTPLUS_KDL_duiker_L".identificatie_lokaalid || 'BGTPLUS_KDL_duiker_L' AS id,
       "BGTPLUS_KDL_duiker_L".plus_type AS type,
       ST_makeValid("BGTPLUS_KDL_duiker_L".geometrie) AS geometrie,
       "BGTPLUS_KDL_duiker_L".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGTPLUS_KDL_duiker_L"
WHERE 1 = 1

/* --- KBK50 ---- */

UNION
SELECT "WDL_brede_waterloop".ogc_fid::TEXT || 'WDL_brede_waterloop' AS identificatie_lokaal_id,
       'brede_waterloop' AS type,
       ST_makeValid("WDL_brede_waterloop".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       NULL::INT AS minzoom,
       12 AS maxzoom
FROM kbk50."WDL_brede_waterloop"

UNION
SELECT "WDL_smalle_waterloop".ogc_fid::TEXT || 'WDL_smalle_waterloop' AS identificatie_lokaal_id,
       'smalle_waterloop' AS type,
       ST_makeValid("WDL_smalle_waterloop".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       NULL::INT AS minzoom,
       12 AS maxzoom
FROM kbk50."WDL_smalle_waterloop"
"""

SELECT_WATERDEELVLAK_SQL: Final = """
SELECT "BGTPLUS_KDL_duiker_V".identificatie_lokaalid || 'BGTPLUS_KDL_duiker_V' AS id,
       "BGTPLUS_KDL_duiker_V".plus_type AS type,
       ST_makeValid("BGTPLUS_KDL_duiker_V".geometrie) AS geometrie,
       "BGTPLUS_KDL_duiker_V".relatievehoogteligging,
       'bgtplus' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGTPLUS_KDL_duiker_V"
UNION
SELECT "BGT_WDL_greppel_droge_sloot".identificatie_lokaalid || 'BGT_WDL_greppel_droge_sloot' AS id,
       "BGT_WDL_greppel_droge_sloot".bgt_type AS type,
       ST_makeValid("BGT_WDL_greppel_droge_sloot".geometrie) AS geometrie,
       "BGT_WDL_greppel_droge_sloot".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_WDL_greppel_droge_sloot"
UNION
SELECT "BGT_WDL_waterloop".identificatie_lokaalid || '-' ||
       "BGT_WDL_waterloop".tijdstipregistratie || '-' || 'BGT_WDL_waterloop' AS id,
       "BGT_WDL_waterloop".bgt_type AS type,
       ST_makeValid("BGT_WDL_waterloop".geometrie) AS geometrie,
       "BGT_WDL_waterloop".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_WDL_waterloop"
UNION
SELECT "BGT_WDL_watervlakte".identificatie_lokaalid || 'BGT_WDL_watervlakte' AS id,
       "BGT_WDL_watervlakte".bgt_type AS type,
       ST_makeValid("BGT_WDL_watervlakte".geometrie) AS geometrie,
       "BGT_WDL_watervlakte".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_WDL_watervlakte"

    /* KBK10 */

UNION
SELECT "WDL_breed_water".ogc_fid::TEXT || 'WDL_breed_water' AS identificatie_lokaal_id,
       'breed_water' AS type,
       ST_makeValid("WDL_breed_water".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WDL_breed_water"
UNION
SELECT "WDL_haven".ogc_fid::TEXT || 'WDL_haven_kbk10' AS identificatie_lokaal_id,
       'haven' AS type,
       ST_makeValid("WDL_haven".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WDL_haven"

/* --- KBK50 ---- */

UNION
SELECT "WDL_wateroppervlak".ogc_fid::TEXT || 'WDL_wateroppervlak' AS identificatie_lokaal_id,
       'breed_water' AS type,
       ST_makeValid("WDL_wateroppervlak".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WDL_wateroppervlak"
"""


SELECT_WEGDEELLIJN_SQL: Final = """
/* --- KBK10 ---- */
SELECT "WGL_smalle_weg".ogc_fid::TEXT || '-' || 'WGL_smalle_weg' AS id,
       'smalle_weg' AS type,
       ST_makeValid("WGL_smalle_weg".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_smalle_weg"

UNION ALL
SELECT "WGL_autoveer".ogc_fid::TEXT || '-' || 'WGL_autoveer' AS id,
       'autoveer' AS type,
       ST_makeValid("WGL_autoveer".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_autoveer"

UNION ALL
SELECT "WGL_hartlijn".ogc_fid::TEXT || '-' || 'WGL_hartlijn' AS id,
       'hartlijn' AS type,
       ST_makeValid("WGL_hartlijn".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_hartlijn"

UNION
SELECT "WGL_voetveer".ogc_fid::TEXT || '-' || 'WGL_voetveer' AS id,
       'voetveer' AS type,
       ST_makeValid("WGL_voetveer".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_voetveer"

    /* --- KBK50 ---- */
UNION
SELECT "WGL_straat_in_tunnel".ogc_fid::TEXT || '-' || 'WGL_straat_in_tunnel' AS id,
       'straat_in_tunnel' AS type,
       ST_makeValid("WGL_straat_in_tunnel".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_straat_in_tunnel"
UNION
SELECT "WGL_hoofdweg_in_tunnel".ogc_fid::TEXT || '-' || 'WGL_hoofdweg_in_tunnel' AS id,
       'hoofdweg_in_tunnel' AS type,
       ST_makeValid("WGL_hoofdweg_in_tunnel".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_hoofdweg_in_tunnel"
UNION
SELECT "WGL_regionale_weg".ogc_fid::TEXT || '-' || 'WGL_regionale_weg' AS id,
       'regionale_weg' AS type,
       ST_makeValid("WGL_regionale_weg".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_regionale_weg"
UNION
SELECT "WGL_regionale_weg_in_tunnel".ogc_fid::TEXT || '-' || 'WGL_regionale_weg_in_tunnel' AS id,
       'regionale_weg_in_tunnel' AS type,
       ST_makeValid("WGL_regionale_weg_in_tunnel".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_regionale_weg_in_tunnel"
UNION
SELECT "WGL_autosnelweg_in_tunnel".ogc_fid::TEXT || '-' || 'WGL_autosnelweg_in_tunnel' AS id,
       'autosnelweg_in_tunnel' AS type,
       ST_makeValid("WGL_autosnelweg_in_tunnel".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_autosnelweg_in_tunnel"
UNION
SELECT "WGL_straat".ogc_fid::TEXT || '-' || 'WGL_straat' AS id,
       'straat' AS type,
       ST_makeValid("WGL_straat".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_straat"
UNION
SELECT "WGL_hoofdweg".ogc_fid::TEXT || '-' || 'WGL_hoofdweg' AS id,
       'hoofdweg' AS type,
       ST_makeValid("WGL_hoofdweg".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_hoofdweg"
UNION
SELECT "WGL_autosnelweg".ogc_fid::TEXT || '-' || 'WGL_autosnelweg' AS id,
       'autosnelweg' AS type,
       ST_makeValid("WGL_autosnelweg".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_autosnelweg"
UNION
SELECT "WGL_veerverbinding".ogc_fid::TEXT || '-' || 'WGL_veerverbinding' AS id,
       'veerverbinding' AS type,
       ST_makeValid("WGL_veerverbinding".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk50' AS bron,
       8 AS minzoom,
       12 AS maxzoom
FROM kbk50."WGL_veerverbinding"
"""

SELECT_WEGDEELVLAK_SQL: Final = """
SELECT "BGT_OWGL_berm".identificatie_lokaalid || '-' || "BGT_OWGL_berm".tijdstipregistratie ||
       '-' || 'BGT_OWGL_berm' AS id,
       "BGT_OWGL_berm".bgt_functie AS type,
       ST_makeValid("BGT_OWGL_berm".geometrie) AS geometrie,
       "BGT_OWGL_berm".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_OWGL_berm"
UNION
SELECT "BGT_OWGL_verkeerseiland".identificatie_lokaalid || 'BGT_OWGL_verkeerseiland' AS id,
       "BGT_OWGL_verkeerseiland".bgt_functie AS type,
       ST_makeValid("BGT_OWGL_verkeerseiland".geometrie) AS geometrie,
       "BGT_OWGL_verkeerseiland".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_OWGL_verkeerseiland"
UNION
SELECT "BGT_WGL_baan_voor_vliegverkeer".identificatie_lokaalid ||
       'BGT_WGL_baan_voor_vliegverkeer' AS id,
       "BGT_WGL_baan_voor_vliegverkeer".bgt_functie AS type,
       ST_makeValid("BGT_WGL_baan_voor_vliegverkeer".geometrie) AS geometrie,
       "BGT_WGL_baan_voor_vliegverkeer".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_baan_voor_vliegverkeer"
UNION
SELECT "BGT_WGL_fietspad".identificatie_lokaalid || '-' ||
       "BGT_WGL_fietspad".tijdstipregistratie || '-' || 'BGT_WGL_fietspad' AS id,
       "BGT_WGL_fietspad".bgt_functie AS type,
       ST_makeValid("BGT_WGL_fietspad".geometrie) AS geometrie,
       "BGT_WGL_fietspad".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_fietspad"
UNION
SELECT "BGT_WGL_inrit".identificatie_lokaalid || 'BGT_WGL_inrit' AS id,
       "BGT_WGL_inrit".bgt_functie AS type,
       ST_makeValid("BGT_WGL_inrit".geometrie) AS geometrie,
       "BGT_WGL_inrit".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_inrit"
UNION
SELECT "BGT_WGL_ov-baan".identificatie_lokaalid || 'BGT_WGL_ov-baan' AS id,
       "BGT_WGL_ov-baan".bgt_functie AS type,
       ST_makeValid("BGT_WGL_ov-baan".geometrie) AS geometrie,
       "BGT_WGL_ov-baan".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_ov-baan"
UNION
SELECT "BGT_WGL_overweg".identificatie_lokaalid || 'BGT_WGL_overweg' AS id,
       "BGT_WGL_overweg".bgt_functie AS type,
       ST_makeValid("BGT_WGL_overweg".geometrie) AS geometrie,
       "BGT_WGL_overweg".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_overweg"
UNION
SELECT "BGT_WGL_parkeervlak".identificatie_lokaalid || 'BGT_WGL_parkeervlak' AS id,
       "BGT_WGL_parkeervlak".bgt_functie AS type,
       ST_makeValid("BGT_WGL_parkeervlak".geometrie) AS geometrie,
       "BGT_WGL_parkeervlak".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_parkeervlak"
UNION
SELECT "BGT_WGL_rijbaan_autosnelweg".identificatie_lokaalid || '-' ||
       "BGT_WGL_rijbaan_autosnelweg".tijdstipregistratie || '-' ||
       'BGT_WGL_rijbaan_autosnelweg' AS id,
       "BGT_WGL_rijbaan_autosnelweg".bgt_functie AS type,
       ST_makeValid("BGT_WGL_rijbaan_autosnelweg".geometrie) AS geometrie,
       "BGT_WGL_rijbaan_autosnelweg".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_rijbaan_autosnelweg"
UNION
SELECT "BGT_WGL_rijbaan_autoweg".identificatie_lokaalid || '-' ||
       "BGT_WGL_rijbaan_autoweg".tijdstipregistratie || '-' || 'BGT_WGL_rijbaan_autoweg' AS id,
       "BGT_WGL_rijbaan_autoweg".bgt_functie AS type,
       ST_makeValid("BGT_WGL_rijbaan_autoweg".geometrie) AS geometrie,
       "BGT_WGL_rijbaan_autoweg".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_rijbaan_autoweg"
UNION
SELECT "BGT_WGL_rijbaan_lokale_weg".identificatie_lokaalid || '-' ||
       "BGT_WGL_rijbaan_lokale_weg".tijdstipregistratie || '-' ||
       'BGT_WGL_rijbaan_lokale_weg' AS id,
       "BGT_WGL_rijbaan_lokale_weg".bgt_functie AS type,
       ST_makeValid("BGT_WGL_rijbaan_lokale_weg".geometrie) AS geometrie,
       "BGT_WGL_rijbaan_lokale_weg".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_rijbaan_lokale_weg"
UNION
SELECT "BGT_WGL_rijbaan_regionale_weg".identificatie_lokaalid || '-' ||
       "BGT_WGL_rijbaan_regionale_weg".tijdstipregistratie || '-' ||
       'BGT_WGL_rijbaan_regionale_weg' AS id,
       "BGT_WGL_rijbaan_regionale_weg".bgt_functie AS type,
       ST_makeValid("BGT_WGL_rijbaan_regionale_weg".geometrie) AS geometrie,
       "BGT_WGL_rijbaan_regionale_weg".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_rijbaan_regionale_weg"
UNION
SELECT "BGT_WGL_ruiterpad".identificatie_lokaalid || '-' ||
       "BGT_WGL_ruiterpad".tijdstipregistratie || '-' || 'BGT_WGL_ruiterpad' AS id,
       "BGT_WGL_ruiterpad".bgt_functie AS type,
       ST_makeValid("BGT_WGL_ruiterpad".geometrie) AS geometrie,
       "BGT_WGL_ruiterpad".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_ruiterpad"
UNION
SELECT "BGT_WGL_voetgangersgebied".identificatie_lokaalid || '-' ||
       "BGT_WGL_voetgangersgebied".tijdstipregistratie || '-' || 'BGT_WGL_voetgangersgebied' AS id,
       "BGT_WGL_voetgangersgebied".bgt_functie AS type,
       ST_makeValid("BGT_WGL_voetgangersgebied".geometrie) AS geometrie,
       "BGT_WGL_voetgangersgebied".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_voetgangersgebied"
UNION
SELECT "BGT_WGL_voetpad".identificatie_lokaalid || '-' || "BGT_WGL_voetpad".tijdstipregistratie ||
       '-' || 'BGT_WGL_voetpad' AS id,
       "BGT_WGL_voetpad".bgt_functie AS type,
       ST_makeValid("BGT_WGL_voetpad".geometrie) AS geometrie,
       "BGT_WGL_voetpad".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_voetpad"
UNION
SELECT "BGT_WGL_voetpad_op_trap".identificatie_lokaalid || '-' ||
       "BGT_WGL_voetpad_op_trap".tijdstipregistratie || '-' || 'BGT_WGL_voetpad_op_trap' AS id,
       "BGT_WGL_voetpad_op_trap".bgt_functie AS type,
       ST_makeValid("BGT_WGL_voetpad_op_trap".geometrie) AS geometrie,
       "BGT_WGL_voetpad_op_trap".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_voetpad_op_trap"
UNION
SELECT "BGT_WGL_woonerf".identificatie_lokaalid || '-' || "BGT_WGL_woonerf".tijdstipregistratie ||
       '-' || 'BGT_WGL_woonerf' AS id,
       "BGT_WGL_woonerf".bgt_functie AS type,
       ST_makeValid("BGT_WGL_woonerf".geometrie) AS geometrie,
       "BGT_WGL_woonerf".relatievehoogteligging,
       'bgt' AS bron,
       16 AS minzoom,
       NULL::INT AS maxzoom
FROM bgt."BGT_WGL_woonerf"
UNION

/* KBK10 */

SELECT "WGL_startbaan_landingsbaan".ogc_fid::TEXT || '-' ||
       'WGL_startbaan_landingsbaan' AS identificatie_lokaal_id,
       'startbaan_landingsbaan' AS type,
       ST_makeValid("WGL_startbaan_landingsbaan".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_startbaan_landingsbaan"
UNION
SELECT "TRN_spoorbaanlichaam".ogc_fid::TEXT || '-' ||
       'TRN_spoorbaanlichaam' AS identificatie_lokaal_id,
       'spoorbaanlichaam' AS type,
       ST_makeValid("TRN_spoorbaanlichaam".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."TRN_spoorbaanlichaam"
UNION
SELECT "WGL_autosnelweg".ogc_fid::TEXT || '-' || 'WGL_autosnelweg' AS identificatie_lokaal_id,
       'autosnelweg' AS type,
       ST_makeValid("WGL_autosnelweg".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_autosnelweg"
UNION
SELECT "WGL_autosnelweg".ogc_fid::TEXT || '-' || 'WGL_autosnelweg' AS identificatie_lokaal_id,
       'autosnelweg' AS type,
       ST_makeValid("WGL_autosnelweg".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_autosnelweg"
UNION
SELECT "WGL_rolbaan_platform".ogc_fid::TEXT || '-' ||
       'WGL_rolbaan_platform' AS identificatie_lokaal_id,
       'rolbaan_platform' AS type,
       ST_makeValid("WGL_rolbaan_platform".geom) AS geometrie,
       0 AS relatievehoogteligging,
       'kbk10' AS bron,
       13 AS minzoom,
       15 AS maxzoom
FROM kbk10."WGL_rolbaan_platform"
"""

SELECT_LABELS_SQL: Final = """
SELECT "BAG_LBL_Ligplaatsnummeraanduidingreeks"."BAG_identificatie" ||
       'BAG_LBL_Ligplaatsnummeraanduidingreeks' AS id,
       'ligplaats' AS type,
       ST_GeometryN(ST_makeValid("BAG_LBL_Ligplaatsnummeraanduidingreeks".geometrie),
                    1) AS geometrie,
       "BAG_LBL_Ligplaatsnummeraanduidingreeks".hoek,
       "BAG_LBL_Ligplaatsnummeraanduidingreeks".tekst,
       'bag/bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BAG_LBL_Ligplaatsnummeraanduidingreeks"
UNION
SELECT "BAG_LBL_Standplaatsnummeraanduidingreeks"."BAG_identificatie" ||
       'BAG_LBL_Standplaatsnummeraanduidingreeks' AS id,
       'standplaats' AS type,
       ST_GeometryN(ST_makeValid("BAG_LBL_Standplaatsnummeraanduidingreeks".geometrie),
                    1) AS geometrie,
       "BAG_LBL_Standplaatsnummeraanduidingreeks".hoek,
       "BAG_LBL_Standplaatsnummeraanduidingreeks".tekst,
       'bag/bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BAG_LBL_Standplaatsnummeraanduidingreeks"
UNION
SELECT "BGT_LBL_administratief_gebied".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                                        OVER (PARTITION BY "BGT_LBL_administratief_gebied".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_LBL_administratief_gebied' AS id,
       "BGT_LBL_administratief_gebied".openbareruimtetype AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_administratief_gebied".geometrie), 1) AS geometrie,
       "BGT_LBL_administratief_gebied".hoek,
       "BGT_LBL_administratief_gebied".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_administratief_gebied"
UNION
SELECT "BGT_LBL_kunstwerk".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                            OVER (PARTITION BY "BGT_LBL_kunstwerk".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_LBL_kunstwerk' AS id,
       "BGT_LBL_kunstwerk".openbareruimtetype AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_kunstwerk".geometrie), 1) AS geometrie,
       "BGT_LBL_kunstwerk".hoek,
       "BGT_LBL_kunstwerk".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_kunstwerk"
UNION
SELECT "BGT_LBL_landschappelijk_gebied".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                                         OVER (PARTITION BY "BGT_LBL_landschappelijk_gebied".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_LBL_landschappelijk_gebied' AS id,
       "BGT_LBL_landschappelijk_gebied".openbareruimtetype AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_landschappelijk_gebied".geometrie), 1) AS geometrie,
       "BGT_LBL_landschappelijk_gebied".hoek,
       "BGT_LBL_landschappelijk_gebied".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_landschappelijk_gebied"
UNION
SELECT "BGT_LBL_nummeraanduidingreeks".identificatie_lokaalid || '-' ||
       "BGT_LBL_nummeraanduidingreeks".tekst || '-' || "BGT_LBL_nummeraanduidingreeks".ogc_fid ||
       '-' || 'BGT_LBL_nummeraanduidingreeks' AS id,
       'nummeraanduiding' AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_nummeraanduidingreeks".geometrie), 1) AS geometrie,
       "BGT_LBL_nummeraanduidingreeks".hoek,
       "BGT_LBL_nummeraanduidingreeks".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_nummeraanduidingreeks"
UNION
SELECT "BGT_LBL_terrein".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                          OVER (PARTITION BY "BGT_LBL_terrein".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_LBL_terrein' AS id,
       "BGT_LBL_terrein".openbareruimtetype AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_terrein".geometrie), 1) AS geometrie,
       "BGT_LBL_terrein".hoek,
       "BGT_LBL_terrein".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_terrein"
UNION
SELECT "BGT_LBL_water".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                        OVER (PARTITION BY "BGT_LBL_water".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_LBL_water' AS id,
       "BGT_LBL_water".openbareruimtetype AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_water".geometrie), 1) AS geometrie,
       "BGT_LBL_water".hoek,
       "BGT_LBL_water".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_water"
UNION
SELECT "BGT_LBL_weg".identificatie_lokaalid || '-' || ROW_NUMBER()
                                                      OVER (PARTITION BY "BGT_LBL_weg".identificatie_lokaalid ORDER BY tijdstipregistratie DESC ) ||
       '-' || 'BGT_LBL_weg' AS id,
       "BGT_LBL_weg".openbareruimtetype AS type,
       ST_GeometryN(ST_makeValid("BGT_LBL_weg".geometrie), 1) AS geometrie,
       "BGT_LBL_weg".hoek,
       "BGT_LBL_weg".tekst,
       'bgt' AS bron,
       16 AS minzoom,
       22 AS maxzoom
FROM bgt."BGT_LBL_weg"
"""
