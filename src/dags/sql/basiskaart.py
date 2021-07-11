from typing import Final

# SQL: contains select en create queries
#
# Note1:
# From the GOB objectstore the uploaded BGT and KBK data is processed in the basiskaart database (by Jenkins).
# The vector based tile generator (https://github.com/Amsterdam/vector_tiles_t_rex) uses the data
# from the basiskaart database, which is a collection of materialized views based upon
# the BGT (basiskaart grootschalige topologie) en KBK (kleinschalig basiskaart level 10 and 50).
#
# Note2:
# For the DSO-API and logical replication the data from the basiskaart database is processed into
# tables in the master database (a.k.a. referentie database). These tables can also be used to create the
# necessary materialized views (see note 1) for the vector based tile generator.
# i.e. CREATE MATERIALIZED VIEW <name_view> AS SELECT * FROM <table_name>
#


# --------------------------------------------------------------------------
# CREATE tables (on master DB)
# --------------------------------------------------------------------------
CREATE_TABLES: Final = """
	{% if 'labels' == params.base_table %}

	DROP TABLE IF EXISTS {{ params.dag_id }}_{{ params.base_table }}_TEMP CASCADE;
    CREATE TABLE IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }}_TEMP (
      id character varying PRIMARY KEY,
      type character varying,
      geometrie geometry,
      hoek float,
	  tekst character varying,
      bron character varying,
      minzoom integer,
      maxzoom integer
    );
    CREATE TABLE IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }} (
      id character varying PRIMARY KEY,
      type character varying,
      geometrie geometry,
      hoek float,
	  tekst character varying,
      bron character varying,
      minzoom integer,
      maxzoom integer
    );

	{% elif 'wegdeelvlak' == params.base_table %}

	DROP TABLE IF EXISTS {{ params.dag_id }}_{{ params.base_table }}_TEMP CASCADE;
    CREATE TABLE IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }}_TEMP (
      id character varying PRIMARY KEY,
      type character varying,
	  subtype character varying,
	  subsubtype character varying,
      geometrie geometry,
      relatievehoogteligging integer,
      bron character varying,
      minzoom integer,
      maxzoom integer
    );
    CREATE TABLE IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }} (
      id character varying PRIMARY KEY,
      type character varying,
	  subtype character varying,
	  subsubtype character varying,
      geometrie geometry,
      relatievehoogteligging integer,
      bron character varying,
      minzoom integer,
      maxzoom integer
    );

	{% else %}

	DROP TABLE IF EXISTS {{ params.dag_id }}_{{ params.base_table }}_TEMP CASCADE;
    CREATE TABLE IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }}_TEMP (
      id character varying PRIMARY KEY,
      type character varying,
      geometrie geometry,
      relatievehoogteligging integer,
      bron character varying,
      minzoom integer,
      maxzoom integer
    );
    CREATE TABLE IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }} (
      id character varying PRIMARY KEY,
      type character varying,
      geometrie geometry,
      relatievehoogteligging integer,
      bron character varying,
      minzoom integer,
      maxzoom integer
    );

	{% endif %}

	/* creating index on geometrie */
	CREATE INDEX IF NOT EXISTS {{ params.dag_id }}_{{ params.base_table }}_geom_idx ON {{ params.dag_id }}_{{ params.base_table }} USING gist (geometrie);
"""
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
SELECT
	"BGTPLUS_GISE_bordes".identificatie_lokaalid || 'BGTPLUS_GISE_bordes' as id,
    "BGTPLUS_GISE_bordes".plus_type as type,
	ST_makeValid(    "BGTPLUS_GISE_bordes".geometrie) as geometrie,
    "BGTPLUS_GISE_bordes".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_GISE_bordes"
UNION
 SELECT
	"BGTPLUS_GISE_luifel".identificatie_lokaalid || 'BGTPLUS_GISE_luifel' as id,
    "BGTPLUS_GISE_luifel".plus_type as type,
	ST_makeValid(    "BGTPLUS_GISE_luifel".geometrie) as geometrie,
    "BGTPLUS_GISE_luifel".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_GISE_luifel"
UNION
 SELECT
	"BGTPLUS_GISE_onbekend".identificatie_lokaalid || 'BGTPLUS_GISE_onbekend' as id,
    "BGTPLUS_GISE_onbekend".plus_type as type,
	ST_makeValid(    "BGTPLUS_GISE_onbekend".geometrie) as geometrie,
    "BGTPLUS_GISE_onbekend".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_GISE_onbekend"
UNION
 SELECT
	"BGTPLUS_GISE_toegangstrap".identificatie_lokaalid || 'BGTPLUS_GISE_toegangstrap' as id,
    "BGTPLUS_GISE_toegangstrap".plus_type as type,
	ST_makeValid(    "BGTPLUS_GISE_toegangstrap".geometrie) as geometrie,
    "BGTPLUS_GISE_toegangstrap".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_GISE_toegangstrap"
UNION
 SELECT
	"BGTPLUS_OBW_bunker".identificatie_lokaalid || 'BGTPLUS_OBW_bunker' as id,
    "BGTPLUS_OBW_bunker".plus_type as type,
	ST_makeValid(    "BGTPLUS_OBW_bunker".geometrie) as geometrie,
    "BGTPLUS_OBW_bunker".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_OBW_bunker"
UNION
 SELECT
	"BGTPLUS_OBW_onbekend".identificatie_lokaalid || 'BGTPLUS_OBW_onbekend' as id,
    "BGTPLUS_OBW_onbekend".plus_type as type,
	ST_makeValid(    "BGTPLUS_OBW_onbekend".geometrie) as geometrie,
    "BGTPLUS_OBW_onbekend".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_OBW_onbekend"
UNION
 SELECT
	"BGTPLUS_OBW_schuur".identificatie_lokaalid || 'BGTPLUS_OBW_schuur' as id,
    "BGTPLUS_OBW_schuur".plus_type as type,
	ST_makeValid(    "BGTPLUS_OBW_schuur".geometrie) as geometrie,
    "BGTPLUS_OBW_schuur".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_OBW_schuur"
UNION
 SELECT
	"BGTPLUS_OBW_voedersilo".identificatie_lokaalid || 'BGTPLUS_OBW_voedersilo' as id,
    "BGTPLUS_OBW_voedersilo".plus_type as type,
	ST_makeValid(    "BGTPLUS_OBW_voedersilo".geometrie) as geometrie,
    "BGTPLUS_OBW_voedersilo".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGTPLUS_OBW_voedersilo"
UNION
 SELECT
	"BGT_OBW_bassin".identificatie_lokaalid || 'BGT_OBW_bassin' as id,
    "BGT_OBW_bassin".bgt_type as type,
	ST_makeValid(    "BGT_OBW_bassin".geometrie) as geometrie,
    "BGT_OBW_bassin".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_bassin"
UNION
 SELECT
	"BGT_OBW_bezinkbak".identificatie_lokaalid || 'BGT_OBW_bezinkbak' as id,
    "BGT_OBW_bezinkbak".bgt_type as type,
	ST_makeValid(    "BGT_OBW_bezinkbak".geometrie) as geometrie,
    "BGT_OBW_bezinkbak".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_bezinkbak"
UNION
 SELECT
	"BGT_OBW_lage_trafo".identificatie_lokaalid || 'BGT_OBW_lage_trafo' as id,
    "BGT_OBW_lage_trafo".bgt_type as type,
	ST_makeValid(    "BGT_OBW_lage_trafo".geometrie) as geometrie,
    "BGT_OBW_lage_trafo".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_lage_trafo"
UNION
 SELECT
	"BGT_OBW_open_loods".identificatie_lokaalid || 'BGT_OBW_open_loods' as id,
    "BGT_OBW_open_loods".bgt_type as type,
	ST_makeValid(    "BGT_OBW_open_loods".geometrie) as geometrie,
    "BGT_OBW_open_loods".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_open_loods"
UNION
 SELECT
	"BGT_OBW_opslagtank".identificatie_lokaalid || 'BGT_OBW_opslagtank' as id,
    "BGT_OBW_opslagtank".bgt_type as type,
	ST_makeValid(    "BGT_OBW_opslagtank".geometrie) as geometrie,
    "BGT_OBW_opslagtank".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_opslagtank"
UNION
 SELECT
	"BGT_OBW_overkapping".identificatie_lokaalid || 'BGT_OBW_overkapping' as id,
    "BGT_OBW_overkapping".bgt_type as type,
	ST_makeValid(    "BGT_OBW_overkapping".geometrie) as geometrie,
    "BGT_OBW_overkapping".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_overkapping"
UNION
 SELECT
	"BGT_OBW_transitie".identificatie_lokaalid || 'BGT_OBW_transitie' as id,
    "BGT_OBW_transitie".bgt_type as type,
	ST_makeValid(    "BGT_OBW_transitie".geometrie) as geometrie,
    "BGT_OBW_transitie".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_transitie"
UNION
 SELECT
	"BGT_OBW_windturbine".identificatie_lokaalid || 'BGT_OBW_windturbine' as id,
    "BGT_OBW_windturbine".bgt_type as type,
	ST_makeValid(    "BGT_OBW_windturbine".geometrie) as geometrie,
    "BGT_OBW_windturbine".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_OBW_windturbine"
UNION
 SELECT
	"BGT_PND_pand".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_PND_pand".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'|| 'BGT_PND_pand' as id,
    "BGT_PND_pand".bgt_status as type,
	ST_makeValid(    "BGT_PND_pand".geometrie) as geometrie,
    "BGT_PND_pand".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_PND_pand"
UNION
 SELECT
	"CFT_Onderbouw".guid || 'CFT_Onderbouw' AS identificatie_lokaalid,
    'onderbouw' as type,
	ST_makeValid(    "CFT_Onderbouw".geometrie) as geometrie,
    "CFT_Onderbouw".relatievehoogteligging,
 	'cft' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."CFT_Onderbouw"
UNION
 SELECT
	"CFT_Overbouw".guid || 'CFT_Overbouw' AS identificatie_lokaalid,
    'overbouw' as type,
	ST_makeValid(    "CFT_Overbouw".geometrie) as geometrie,
    "CFT_Overbouw".relatievehoogteligging,
 	'cft' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."CFT_Overbouw"

/* BAG (uit BGT dataset) */
UNION
  SELECT
	"BAG_Standplaats"."BAG_identificatie"  || 'BAG_Standplaats' AS identificatie_lokaalid,
    'standplaats' as type,
	ST_makeValid(    "BAG_Standplaats".geometrie) as geometrie,
    0 as relatievehoogteligging,
 	'bag' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BAG_Standplaats"
UNION
 SELECT
	"BAG_Ligplaats"."BAG_identificatie" || 'BAG_Ligplaats' AS identificatie_lokaalid,
    'ligplaats' as type,
	ST_makeValid(    "BAG_Ligplaats".geometrie) as geometrie,
    0 as relatievehoogteligging,
 	'bag' as bron,
 	16  as minzoom,
 	22 as maxzoom
   FROM bgt."BAG_Ligplaats"
UNION
  SELECT
	"GBW_overdekt".ogc_fid::text || 'GBW_overdekt_kbk10' as identificatie_lokaal_id,
    'overdekt' as type,
	ST_makeValid(    "GBW_overdekt".geom) as geometrie,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15 as maxzoom
  FROM kbk10."GBW_overdekt"
 UNION
  SELECT
	"GBW_gebouw".ogc_fid::text || 'GBW_gebouw_kbk10' as identificatie_lokaal_id,
    'gebouw' as type,
	ST_makeValid(    "GBW_gebouw".geom) as geometrie,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15 as maxzoom
  FROM kbk10."GBW_gebouw"
 UNION
  SELECT
	"GBW_hoogbouw".ogc_fid::text || 'GBW_hoogbouw_kbk10' as identificatie_lokaal_id,
    'hoogbouw' as type,
	ST_makeValid(    "GBW_hoogbouw".geom) as geometrie,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15 as maxzoom
  FROM kbk10."GBW_hoogbouw"
 UNION
  SELECT
	"GBW_kas_warenhuis".ogc_fid::text || 'GBW_kas_warenhuis_kbk10' as identificatie_lokaal_id,
    'kas_warenhuis' as type,
	ST_makeValid(    "GBW_kas_warenhuis".geom) as geometrie,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13 as minzoom,
 	15 as maxzoom
  FROM kbk10."GBW_kas_warenhuis"
 UNION
 SELECT
	"GBW_bebouwing".ogc_fid::text || 'GBW_bebouwing_kbk50' as identificatie_lokaal_id,
    'bebouwing' as type,
	ST_makeValid(    "GBW_bebouwing".geom) as geometrie,
    0  as relatievehoogteligging,
 	'kbk50' as bron,
 	8 as minzoom,
 	12 as maxzoom
  FROM kbk50."GBW_bebouwing"
   UNION
 SELECT
	"GBW_kassen".ogc_fid::text || 'GBW_kassen_kbk50' as identificatie_lokaal_id,
    'kassen' as type,
	ST_makeValid(    "GBW_kassen".geom) as geometrie,
    0  as relatievehoogteligging,
 	'kbk50' as bron,
 	8 as minzoom,
 	12 as maxzoom
  FROM kbk50."GBW_kassen"
"""

SELECT_INRICHTINGSELEMENTLIJN_SQL: Final = """
/*
NOTE: since 23 dec not present any more in the source. Possibly part of BGT instead of BGTPLUS.
SELECT
	"BGTPLUS_OSDG_damwand".identificatie_lokaalid || 'BGTPLUS_OSDG_damwand' as id,
    "BGTPLUS_OSDG_damwand".plus_type as type,
	ST_makeValid(    "BGTPLUS_OSDG_damwand".geometrie) as geometrie,
    "BGTPLUS_OSDG_damwand".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_OSDG_damwand"
UNION
 SELECT
	"BGTPLUS_OSDG_geluidsscherm".identificatie_lokaalid || 'BGTPLUS_OSDG_geluidsscherm' as id,
    "BGTPLUS_OSDG_geluidsscherm".plus_type as type,
	ST_makeValid(    "BGTPLUS_OSDG_geluidsscherm".geometrie) as geometrie,
    "BGTPLUS_OSDG_geluidsscherm".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_OSDG_geluidsscherm"
UNION
*/
 SELECT
	"BGTPLUS_OSDG_hek".identificatie_lokaalid || 'BGTPLUS_OSDG_hek' as id,
    "BGTPLUS_OSDG_hek".plus_type as type,
	ST_makeValid(    "BGTPLUS_OSDG_hek".geometrie) as geometrie,
    "BGTPLUS_OSDG_hek".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_OSDG_hek"
UNION
 SELECT
	"BGTPLUS_OSDG_kademuur_L".identificatie_lokaalid || 'BGTPLUS_OSDG_kademuur_L' as id,
    "BGTPLUS_OSDG_kademuur_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_OSDG_kademuur_L".geometrie) as geometrie,
    "BGTPLUS_OSDG_kademuur_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_OSDG_kademuur_L"
UNION
 SELECT
	"BGTPLUS_OSDG_muur_L".identificatie_lokaalid || 'BGTPLUS_OSDG_muur_L' as id,
    "BGTPLUS_OSDG_muur_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_OSDG_muur_L".geometrie) as geometrie,
    "BGTPLUS_OSDG_muur_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_OSDG_muur_L"
UNION
 SELECT
	"BGTPLUS_OSDG_walbescherming".identificatie_lokaalid || 'BGTPLUS_OSDG_walbescherming' as id,
    "BGTPLUS_OSDG_walbescherming".plus_type as type,
	ST_makeValid(    "BGTPLUS_OSDG_walbescherming".geometrie) as geometrie,
    "BGTPLUS_OSDG_walbescherming".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_OSDG_walbescherming"
UNION
 SELECT
	"BGTPLUS_SDG_draadraster".identificatie_lokaalid || 'BGTPLUS_SDG_draadraster' as id,
    "BGTPLUS_SDG_draadraster".plus_type as type,
	ST_makeValid(    "BGTPLUS_SDG_draadraster".geometrie) as geometrie,
    "BGTPLUS_SDG_draadraster".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_SDG_draadraster"
UNION
 SELECT
	"BGTPLUS_SDG_faunaraster".identificatie_lokaalid || 'BGTPLUS_SDG_faunaraster' as id,
    "BGTPLUS_SDG_faunaraster".plus_type as type,
	ST_makeValid(    "BGTPLUS_SDG_faunaraster".geometrie) as geometrie,
    "BGTPLUS_SDG_faunaraster".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_SDG_faunaraster"
UNION
 SELECT
	"BGTPLUS_VGT_haag_L".identificatie_lokaalid || 'BGTPLUS_VGT_haag_L' as id,
    "BGTPLUS_VGT_haag_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_VGT_haag_L".geometrie) as geometrie,
    "BGTPLUS_VGT_haag_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_VGT_haag_L"
UNION
 SELECT
	"BGTPLUS_WDI_geleidewerk".identificatie_lokaalid || 'BGTPLUS_WDI_geleidewerk' as id,
    "BGTPLUS_WDI_geleidewerk".plus_type as type,
	ST_makeValid(    "BGTPLUS_WDI_geleidewerk".geometrie) as geometrie,
    "BGTPLUS_WDI_geleidewerk".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WDI_geleidewerk"
UNION
 SELECT
	"BGTPLUS_WDI_remmingswerk".identificatie_lokaalid || 'BGTPLUS_WDI_remmingswerk' as id,
    "BGTPLUS_WDI_remmingswerk".plus_type as type,
	ST_makeValid(    "BGTPLUS_WDI_remmingswerk".geometrie) as geometrie,
    "BGTPLUS_WDI_remmingswerk".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WDI_remmingswerk"
UNION
 SELECT
	"BGTPLUS_WGI_balustrade".identificatie_lokaalid || 'BGTPLUS_WGI_balustrade' as id,
    "BGTPLUS_WGI_balustrade".plus_type as type,
	ST_makeValid(    "BGTPLUS_WGI_balustrade".geometrie) as geometrie,
    "BGTPLUS_WGI_balustrade".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_balustrade"
UNION
 SELECT
	"BGTPLUS_WGI_geleideconstructie_L".identificatie_lokaalid || 'BGTPLUS_WGI_geleideconstructie_L' as id,
    "BGTPLUS_WGI_geleideconstructie_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_WGI_geleideconstructie_L".geometrie) as geometrie,
    "BGTPLUS_WGI_geleideconstructie_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_geleideconstructie_L"
UNION
 SELECT
	"BGTPLUS_WGI_rooster_L".identificatie_lokaalid || 'BGTPLUS_WGI_rooster_L' as id,
    "BGTPLUS_WGI_rooster_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_WGI_rooster_L".geometrie) as geometrie,
    "BGTPLUS_WGI_rooster_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_rooster_L"
UNION
 SELECT
	"BGTPLUS_WGI_wildrooster_L".identificatie_lokaalid || 'BGTPLUS_WGI_wildrooster_L' as id,
    "BGTPLUS_WGI_wildrooster_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_WGI_wildrooster_L".geometrie) as geometrie,
    "BGTPLUS_WGI_wildrooster_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_wildrooster_L"
UNION
 SELECT
	"BGT_KDL_stuw_L".identificatie_lokaalid || 'BGT_KDL_stuw_L' as id,
    "BGT_KDL_stuw_L".bgt_type as type,
	ST_makeValid(    "BGT_KDL_stuw_L".geometrie) as geometrie,
    "BGT_KDL_stuw_L".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_KDL_stuw_L"
UNION
 SELECT
	"BGT_SDG_damwand".identificatie_lokaalid || 'BGT_SDG_damwand' as id,
    "BGT_SDG_damwand".bgt_type as type,
	ST_makeValid(    "BGT_SDG_damwand".geometrie) as geometrie,
    "BGT_SDG_damwand".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_SDG_damwand"
UNION
 SELECT
	"BGT_SDG_geluidsscherm".identificatie_lokaalid || 'BGT_SDG_geluidsscherm' as id,
    "BGT_SDG_geluidsscherm".bgt_type as type,
	ST_makeValid(    "BGT_SDG_geluidsscherm".geometrie) as geometrie,
    "BGT_SDG_geluidsscherm".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_SDG_geluidsscherm"
UNION
 SELECT
	"BGT_SDG_hek".identificatie_lokaalid || 'BGT_SDG_hek' as id,
    "BGT_SDG_hek".bgt_type as type,
	ST_makeValid(    "BGT_SDG_hek".geometrie) as geometrie,
    "BGT_SDG_hek".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_SDG_hek"
UNION
 SELECT
	"BGT_SDG_kademuur_L".identificatie_lokaalid || 'BGT_SDG_kademuur_L' as id,
    "BGT_SDG_kademuur_L".bgt_type as type,
	ST_makeValid(    "BGT_SDG_kademuur_L".geometrie) as geometrie,
    "BGT_SDG_kademuur_L".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_SDG_kademuur_L"
UNION
 SELECT
	"BGT_SDG_muur_L".identificatie_lokaalid || 'BGT_SDG_muur_L' as id,
    "BGT_SDG_muur_L".bgt_type as type,
	ST_makeValid(    "BGT_SDG_muur_L".geometrie) as geometrie,
    "BGT_SDG_muur_L".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_SDG_muur_L"
UNION
 SELECT
	"BGT_SDG_walbescherming".identificatie_lokaalid || 'BGT_SDG_walbescherming' as id,
    "BGT_SDG_walbescherming".bgt_type as type,
	ST_makeValid(    "BGT_SDG_walbescherming".geometrie) as geometrie,
    "BGT_SDG_walbescherming".relatievehoogteligging,
 	'bgt' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGT_SDG_walbescherming"
UNION
 SELECT
	"BGTPLUS_KDL_onbekend_L".identificatie_lokaalid || 'BGTPLUS_KDL_onbekend_L' as id,
    "BGTPLUS_KDL_onbekend_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_KDL_onbekend_L".geometrie) as geometrie,
    "BGTPLUS_KDL_onbekend_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_KDL_onbekend_L"
UNION
 SELECT
	"BGTPLUS_SDG_onbekend_L".identificatie_lokaalid || 'BGTPLUS_SDG_onbekend_L' as id,
    "BGTPLUS_SDG_onbekend_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_SDG_onbekend_L".geometrie) as geometrie,
    "BGTPLUS_SDG_onbekend_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_SDG_onbekend_L"
UNION
 SELECT
	"BGTPLUS_VGT_onbekend_L".identificatie_lokaalid || 'BGTPLUS_VGT_onbekend_L' as id,
    "BGTPLUS_VGT_onbekend_L".plus_type as type,
	ST_makeValid(    "BGTPLUS_VGT_onbekend_L".geometrie) as geometrie,
    "BGTPLUS_VGT_onbekend_L".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_VGT_onbekend_L"
UNION
 SELECT
	"BGTPLUS_WGI_lijnafwatering".identificatie_lokaalid || 'BGTPLUS_WGI_lijnafwatering' as id,
    "BGTPLUS_WGI_lijnafwatering".plus_type as type,
	ST_makeValid(    "BGTPLUS_WGI_lijnafwatering".geometrie) as geometrie,
    "BGTPLUS_WGI_lijnafwatering".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_lijnafwatering"
UNION
 SELECT
	"BGTPLUS_WGI_molgoot".identificatie_lokaalid || 'BGTPLUS_WGI_molgoot' as id,
    "BGTPLUS_WGI_molgoot".plus_type as type,
	ST_makeValid(    "BGTPLUS_WGI_molgoot".geometrie) as geometrie,
    "BGTPLUS_WGI_molgoot".relatievehoogteligging,
 	'bgtplus' as bron,
 	16  as minzoom,
 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_molgoot"

/* ---- KBK10 ---- */
 UNION
  SELECT
	"WDL_smal_water_3_tot_6m".ogc_fid::text || 'WDL_smal_water_3_tot_6m' as identificatie_lokaal_id,
    'smal_water_3_tot_6m' as type,
    "WDL_smal_water_3_tot_6m".geom,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15  as maxzoom
  FROM kbk10."WDL_smal_water_3_tot_6m"
 UNION
  SELECT
	"WDL_smal_water_tot_3m".ogc_fid::text || 'WDL_smal_water_tot_3m' as identificatie_lokaal_id,
    'smal_water_tot_3m' as type,
    "WDL_smal_water_tot_3m".geom,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15  as maxzoom
  FROM kbk10."WDL_smal_water_tot_3m"
 UNION
  SELECT
	"KRT_tunnelcontour".ogc_fid::text || 'KRT_tunnelcontour' as identificatie_lokaal_id,
    'tunnelcontour' as type,
    "KRT_tunnelcontour".geom,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15  as maxzoom
  FROM kbk10."KRT_tunnelcontour"
 UNION
  SELECT
	"IRT_aanlegsteiger_smal".ogc_fid::text || 'IRT_aanlegsteiger_smal' as identificatie_lokaal_id,
    'aanlegsteiger_smal' as type,
    "IRT_aanlegsteiger_smal".geom,
    0  as relatievehoogteligging,
 	'kbk10' as bron,
 	13  as minzoom,
 	15  as maxzoom
  FROM kbk10."IRT_aanlegsteiger_smal"
"""


SELECT_INRICHTINGSELEMENTPUNT_SQL: Final = """
SELECT
		"BGTPLUS_BAK_afvalbak".identificatie_lokaalid || 'BGTPLUS_BAK_afvalbak' as id,
    "BGTPLUS_BAK_afvalbak".plus_type as type,
		ST_makeValid(    "BGTPLUS_BAK_afvalbak".geometrie) as geometrie,
    "BGTPLUS_BAK_afvalbak".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BAK_afvalbak"

UNION
 SELECT
		"BGTPLUS_BAK_afval_apart_plaats".identificatie_lokaalid || 'BGTPLUS_BAK_afval_apart_plaats' as id,
    "BGTPLUS_BAK_afval_apart_plaats".plus_type as type,
		ST_makeValid(    "BGTPLUS_BAK_afval_apart_plaats".geometrie) as geometrie,
    "BGTPLUS_BAK_afval_apart_plaats".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BAK_afval_apart_plaats"

UNION
 SELECT
		"BGTPLUS_ISE_onbekend".identificatie_lokaalid || 'BGTPLUS_ISE_onbekend' as id,
    "BGTPLUS_ISE_onbekend".plus_type as type,
		ST_makeValid(    "BGTPLUS_ISE_onbekend".geometrie) as geometrie,
    "BGTPLUS_ISE_onbekend".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_ISE_onbekend"

UNION
 SELECT
		"BGTPLUS_ISE_pomp".identificatie_lokaalid || 'BGTPLUS_ISE_pomp' as id,
    "BGTPLUS_ISE_pomp".plus_type as type,
		ST_makeValid(    "BGTPLUS_ISE_pomp".geometrie) as geometrie,
    "BGTPLUS_ISE_pomp".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_ISE_pomp"

UNION
 SELECT
		"BGTPLUS_KST_cai-kast".identificatie_lokaalid || 'BGTPLUS_KST_cai-kast' as id,
    "BGTPLUS_KST_cai-kast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_cai-kast".geometrie) as geometrie,
    "BGTPLUS_KST_cai-kast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_cai-kast"

UNION
 SELECT
		"BGTPLUS_KST_elektrakast".identificatie_lokaalid || 'BGTPLUS_KST_elektrakast' as id,
    "BGTPLUS_KST_elektrakast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_elektrakast".geometrie) as geometrie,
    "BGTPLUS_KST_elektrakast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_elektrakast"

UNION
 SELECT
		"BGTPLUS_KST_onbekend".identificatie_lokaalid || 'BGTPLUS_KST_onbekend' as id,
    "BGTPLUS_KST_onbekend".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_onbekend".geometrie) as geometrie,
    "BGTPLUS_KST_onbekend".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_onbekend"

UNION
 SELECT
		"BGTPLUS_PAL_lichtmast".identificatie_lokaalid || 'BGTPLUS_PAL_lichtmast' as id,
    "BGTPLUS_PAL_lichtmast".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_lichtmast".geometrie) as geometrie,
    "BGTPLUS_PAL_lichtmast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_lichtmast"

UNION
 SELECT
		"BGTPLUS_PUT_brandkraan_-put".identificatie_lokaalid || 'BGTPLUS_PUT_brandkraan_-put' as id,
    "BGTPLUS_PUT_brandkraan_-put".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_brandkraan_-put".geometrie) as geometrie,
    "BGTPLUS_PUT_brandkraan_-put".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_brandkraan_-put"

UNION
 SELECT
		"BGTPLUS_PUT_inspectie-_rioolput".identificatie_lokaalid || 'BGTPLUS_PUT_inspectie-_rioolput' as id,
    "BGTPLUS_PUT_inspectie-_rioolput".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_inspectie-_rioolput".geometrie) as geometrie,
    "BGTPLUS_PUT_inspectie-_rioolput".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_inspectie-_rioolput"

UNION
 SELECT
		"BGTPLUS_PUT_kolk".identificatie_lokaalid || 'BGTPLUS_PUT_kolk' as id,
    "BGTPLUS_PUT_kolk".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_kolk".geometrie) as geometrie,
    "BGTPLUS_PUT_kolk".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_kolk"

UNION
 SELECT
		"BGTPLUS_SMR_abri".identificatie_lokaalid || 'BGTPLUS_SMR_abri' as id,
    "BGTPLUS_SMR_abri".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_abri".geometrie) as geometrie,
    "BGTPLUS_SMR_abri".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_abri"

UNION
 SELECT
		"BGTPLUS_SMR_betaalautomaat".identificatie_lokaalid || 'BGTPLUS_SMR_betaalautomaat' as id,
    "BGTPLUS_SMR_betaalautomaat".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_betaalautomaat".geometrie) as geometrie,
    "BGTPLUS_SMR_betaalautomaat".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_betaalautomaat"

UNION
 SELECT
		"BGTPLUS_SMR_fietsenrek".identificatie_lokaalid || 'BGTPLUS_SMR_fietsenrek' as id,
    "BGTPLUS_SMR_fietsenrek".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_fietsenrek".geometrie) as geometrie,
    "BGTPLUS_SMR_fietsenrek".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_fietsenrek"

UNION
 SELECT
		"BGTPLUS_SMR_herdenkingsmonument".identificatie_lokaalid || 'BGTPLUS_SMR_herdenkingsmonument' as id,
    "BGTPLUS_SMR_herdenkingsmonument".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_herdenkingsmonument".geometrie) as geometrie,
    "BGTPLUS_SMR_herdenkingsmonument".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_herdenkingsmonument"

UNION
 SELECT
		"BGTPLUS_SMR_kunstobject".identificatie_lokaalid || 'BGTPLUS_SMR_kunstobject' as id,
    "BGTPLUS_SMR_kunstobject".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_kunstobject".geometrie) as geometrie,
    "BGTPLUS_SMR_kunstobject".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_kunstobject"

UNION
 SELECT
		"BGTPLUS_SMR_openbaar_toilet".identificatie_lokaalid || 'BGTPLUS_SMR_openbaar_toilet' as id,
    "BGTPLUS_SMR_openbaar_toilet".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_openbaar_toilet".geometrie) as geometrie,
    "BGTPLUS_SMR_openbaar_toilet".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_openbaar_toilet"

UNION
 SELECT
		"BGTPLUS_SMR_reclamezuil".identificatie_lokaalid || 'BGTPLUS_SMR_reclamezuil' as id,
    "BGTPLUS_SMR_reclamezuil".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_reclamezuil".geometrie) as geometrie,
    "BGTPLUS_SMR_reclamezuil".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_reclamezuil"

UNION
 SELECT
		"BGTPLUS_SMR_telefooncel".identificatie_lokaalid || 'BGTPLUS_SMR_telefooncel' as id,
    "BGTPLUS_SMR_telefooncel".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_telefooncel".geometrie) as geometrie,
    "BGTPLUS_SMR_telefooncel".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_telefooncel"

UNION
 SELECT
		"BGTPLUS_VGT_boom".identificatie_lokaalid || 'BGTPLUS_VGT_boom' as id,
    "BGTPLUS_VGT_boom".plus_type as type,
		ST_makeValid(    "BGTPLUS_VGT_boom".geometrie) as geometrie,
    "BGTPLUS_VGT_boom".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_VGT_boom"

UNION
 SELECT
		"BGT_KDL_hoogspanningsmast_P".identificatie_lokaalid || 'BGT_KDL_hoogspanningsmast_P' as id,
    "BGT_KDL_hoogspanningsmast_P".bgt_type as type,
		ST_makeValid(    "BGT_KDL_hoogspanningsmast_P".geometrie) as geometrie,
    "BGT_KDL_hoogspanningsmast_P".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGT_KDL_hoogspanningsmast_P"

UNION
 SELECT
		"BGTPLUS_BRD_informatiebord".identificatie_lokaalid || 'BGTPLUS_BRD_informatiebord' as id,
    "BGTPLUS_BRD_informatiebord".plus_type as type,
		ST_makeValid(    "BGTPLUS_BRD_informatiebord".geometrie) as geometrie,
    "BGTPLUS_BRD_informatiebord".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BRD_informatiebord"

UNION
 SELECT
		"BGTPLUS_BRD_reclamebord".identificatie_lokaalid || 'BGTPLUS_BRD_reclamebord' as id,
    "BGTPLUS_BRD_reclamebord".plus_type as type,
		ST_makeValid(    "BGTPLUS_BRD_reclamebord".geometrie) as geometrie,
    "BGTPLUS_BRD_reclamebord".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BRD_reclamebord"

UNION
 SELECT
		"BGTPLUS_BRD_straatnaambord".identificatie_lokaalid || 'BGTPLUS_BRD_straatnaambord' as id,
    "BGTPLUS_BRD_straatnaambord".plus_type as type,
		ST_makeValid(    "BGTPLUS_BRD_straatnaambord".geometrie) as geometrie,
    "BGTPLUS_BRD_straatnaambord".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BRD_straatnaambord"

UNION
 SELECT
		"BGTPLUS_BRD_verkeersbord".identificatie_lokaalid || 'BGTPLUS_BRD_verkeersbord' as id,
    "BGTPLUS_BRD_verkeersbord".plus_type as type,
		ST_makeValid(    "BGTPLUS_BRD_verkeersbord".geometrie) as geometrie,
    "BGTPLUS_BRD_verkeersbord".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BRD_verkeersbord"

UNION
 SELECT
		"BGTPLUS_BRD_verklikker_transportleiding".identificatie_lokaalid || 'BGTPLUS_BRD_verklikker_transportleiding' as id,
    "BGTPLUS_BRD_verklikker_transportleiding".plus_type as type,
		ST_makeValid(    "BGTPLUS_BRD_verklikker_transportleiding".geometrie) as geometrie,
    "BGTPLUS_BRD_verklikker_transportleiding".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BRD_verklikker_transportleiding"

UNION
 SELECT
		"BGTPLUS_BRD_wegwijzer".identificatie_lokaalid || 'BGTPLUS_BRD_wegwijzer' as id,
    "BGTPLUS_BRD_wegwijzer".plus_type as type,
		ST_makeValid(    "BGTPLUS_BRD_wegwijzer".geometrie) as geometrie,
    "BGTPLUS_BRD_wegwijzer".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_BRD_wegwijzer"

UNION
 SELECT
		"BGTPLUS_KST_gaskast".identificatie_lokaalid || 'BGTPLUS_KST_gaskast' as id,
    "BGTPLUS_KST_gaskast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_gaskast".geometrie) as geometrie,
    "BGTPLUS_KST_gaskast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_gaskast"

UNION
 SELECT
		"BGTPLUS_KST_gms_kast".identificatie_lokaalid || 'BGTPLUS_KST_gms_kast' as id,
    "BGTPLUS_KST_gms_kast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_gms_kast".geometrie) as geometrie,
    "BGTPLUS_KST_gms_kast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_gms_kast"

UNION
 SELECT
		"BGTPLUS_KST_openbare_verlichtingkast".identificatie_lokaalid || 'BGTPLUS_KST_openbare_verlichtingkast' as id,
    "BGTPLUS_KST_openbare_verlichtingkast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_openbare_verlichtingkast".geometrie) as geometrie,
    "BGTPLUS_KST_openbare_verlichtingkast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_openbare_verlichtingkast"

UNION
 SELECT
		"BGTPLUS_KST_rioolkast".identificatie_lokaalid || 'BGTPLUS_KST_rioolkast' as id,
    "BGTPLUS_KST_rioolkast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_rioolkast".geometrie) as geometrie,
    "BGTPLUS_KST_rioolkast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_rioolkast"

UNION
 SELECT
		"BGTPLUS_KST_telecom_kast".identificatie_lokaalid || 'BGTPLUS_KST_telecom_kast' as id,
    "BGTPLUS_KST_telecom_kast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_telecom_kast".geometrie) as geometrie,
    "BGTPLUS_KST_telecom_kast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_telecom_kast"

UNION
 SELECT
		"BGTPLUS_KST_telkast".identificatie_lokaalid || 'BGTPLUS_KST_telkast' as id,
    "BGTPLUS_KST_telkast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_telkast".geometrie) as geometrie,
    "BGTPLUS_KST_telkast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_telkast"

UNION
 SELECT
		"BGTPLUS_KST_verkeersregelinstallatiekast".identificatie_lokaalid || 'BGTPLUS_KST_verkeersregelinstallatiekast' as id,
    "BGTPLUS_KST_verkeersregelinstallatiekast".plus_type as type,
		ST_makeValid(    "BGTPLUS_KST_verkeersregelinstallatiekast".geometrie) as geometrie,
    "BGTPLUS_KST_verkeersregelinstallatiekast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_KST_verkeersregelinstallatiekast"

UNION
 SELECT
		"BGTPLUS_MST_onbekend".identificatie_lokaalid || 'BGTPLUS_MST_onbekend' as id,
    "BGTPLUS_MST_onbekend".plus_type as type,
		ST_makeValid(    "BGTPLUS_MST_onbekend".geometrie) as geometrie,
    "BGTPLUS_MST_onbekend".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_MST_onbekend"

UNION
 SELECT
		"BGTPLUS_MST_zendmast".identificatie_lokaalid || 'BGTPLUS_MST_zendmast' as id,
    "BGTPLUS_MST_zendmast".plus_type as type,
		ST_makeValid(    "BGTPLUS_MST_zendmast".geometrie) as geometrie,
    "BGTPLUS_MST_zendmast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_MST_zendmast"

UNION
 SELECT
		"BGTPLUS_PAL_afsluitpaal".identificatie_lokaalid || 'BGTPLUS_PAL_afsluitpaal' as id,
    "BGTPLUS_PAL_afsluitpaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_afsluitpaal".geometrie) as geometrie,
    "BGTPLUS_PAL_afsluitpaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_afsluitpaal"

UNION
 SELECT
		"BGTPLUS_PAL_drukknoppaal".identificatie_lokaalid || 'BGTPLUS_PAL_drukknoppaal' as id,
    "BGTPLUS_PAL_drukknoppaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_drukknoppaal".geometrie) as geometrie,
    "BGTPLUS_PAL_drukknoppaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_drukknoppaal"

UNION
 SELECT
		"BGTPLUS_PAL_haltepaal".identificatie_lokaalid || 'BGTPLUS_PAL_haltepaal' as id,
    "BGTPLUS_PAL_haltepaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_haltepaal".geometrie) as geometrie,
    "BGTPLUS_PAL_haltepaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_haltepaal"

UNION
 SELECT
		"BGTPLUS_PAL_hectometerpaal".identificatie_lokaalid || 'BGTPLUS_PAL_hectometerpaal' as id,
    "BGTPLUS_PAL_hectometerpaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_hectometerpaal".geometrie) as geometrie,
    "BGTPLUS_PAL_hectometerpaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_hectometerpaal"

UNION
 SELECT
		"BGTPLUS_PAL_poller".identificatie_lokaalid || 'BGTPLUS_PAL_poller' as id,
    "BGTPLUS_PAL_poller".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_poller".geometrie) as geometrie,
    "BGTPLUS_PAL_poller".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_poller"

UNION
 SELECT
		"BGTPLUS_PAL_telpaal".identificatie_lokaalid || 'BGTPLUS_PAL_telpaal' as id,
    "BGTPLUS_PAL_telpaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_telpaal".geometrie) as geometrie,
    "BGTPLUS_PAL_telpaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_telpaal"

UNION
 SELECT
		"BGTPLUS_PAL_verkeersbordpaal".identificatie_lokaalid || 'BGTPLUS_PAL_verkeersbordpaal' as id,
    "BGTPLUS_PAL_verkeersbordpaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_verkeersbordpaal".geometrie) as geometrie,
    "BGTPLUS_PAL_verkeersbordpaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_verkeersbordpaal"

UNION
 SELECT
		"BGTPLUS_PAL_verkeersregelinstallatiepaal".identificatie_lokaalid || 'BGTPLUS_PAL_verkeersregelinstallatiepaal' as id,
    "BGTPLUS_PAL_verkeersregelinstallatiepaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_verkeersregelinstallatiepaal".geometrie) as geometrie,
    "BGTPLUS_PAL_verkeersregelinstallatiepaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_verkeersregelinstallatiepaal"

UNION
 SELECT
		"BGTPLUS_PAL_vlaggenmast".identificatie_lokaalid || 'BGTPLUS_PAL_vlaggenmast' as id,
    "BGTPLUS_PAL_vlaggenmast".plus_type as type,
		ST_makeValid(    "BGTPLUS_PAL_vlaggenmast".geometrie) as geometrie,
    "BGTPLUS_PAL_vlaggenmast".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PAL_vlaggenmast"

UNION
 SELECT
		"BGTPLUS_PUT_benzine-_olieput".identificatie_lokaalid || 'BGTPLUS_PUT_benzine-_olieput' as id,
    "BGTPLUS_PUT_benzine-_olieput".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_benzine-_olieput".geometrie) as geometrie,
    "BGTPLUS_PUT_benzine-_olieput".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_benzine-_olieput"

UNION
 SELECT
		"BGTPLUS_PUT_drainageput".identificatie_lokaalid || 'BGTPLUS_PUT_drainageput' as id,
    "BGTPLUS_PUT_drainageput".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_drainageput".geometrie) as geometrie,
    "BGTPLUS_PUT_drainageput".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_drainageput"

UNION
 SELECT
		"BGTPLUS_PUT_gasput".identificatie_lokaalid || 'BGTPLUS_PUT_gasput' as id,
    "BGTPLUS_PUT_gasput".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_gasput".geometrie) as geometrie,
    "BGTPLUS_PUT_gasput".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_gasput"

UNION
 SELECT
		"BGTPLUS_PUT_onbekend".identificatie_lokaalid || 'BGTPLUS_PUT_onbekend' as id,
    "BGTPLUS_PUT_onbekend".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_onbekend".geometrie) as geometrie,
    "BGTPLUS_PUT_onbekend".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_onbekend"

UNION
 SELECT
		"BGTPLUS_PUT_waterleidingput".identificatie_lokaalid || 'BGTPLUS_PUT_waterleidingput' as id,
    "BGTPLUS_PUT_waterleidingput".plus_type as type,
		ST_makeValid(    "BGTPLUS_PUT_waterleidingput".geometrie) as geometrie,
    "BGTPLUS_PUT_waterleidingput".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_PUT_waterleidingput"

UNION
 SELECT
		"BGTPLUS_SMR_bank".identificatie_lokaalid || 'BGTPLUS_SMR_bank' as id,
    "BGTPLUS_SMR_bank".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_bank".geometrie) as geometrie,
    "BGTPLUS_SMR_bank".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_bank"

UNION
 SELECT
		"BGTPLUS_SMR_brievenbus".identificatie_lokaalid || 'BGTPLUS_SMR_brievenbus' as id,
    "BGTPLUS_SMR_brievenbus".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_brievenbus".geometrie) as geometrie,
    "BGTPLUS_SMR_brievenbus".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_brievenbus"

UNION
 SELECT
		"BGTPLUS_SMR_fietsenkluis".identificatie_lokaalid || 'BGTPLUS_SMR_fietsenkluis' as id,
    "BGTPLUS_SMR_fietsenkluis".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_fietsenkluis".geometrie) as geometrie,
    "BGTPLUS_SMR_fietsenkluis".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_fietsenkluis"

UNION
 SELECT
		"BGTPLUS_SMR_lichtpunt".identificatie_lokaalid || 'BGTPLUS_SMR_lichtpunt' as id,
    "BGTPLUS_SMR_lichtpunt".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_lichtpunt".geometrie) as geometrie,
    "BGTPLUS_SMR_lichtpunt".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_lichtpunt"

UNION
 SELECT
		"BGTPLUS_SMR_slagboom".identificatie_lokaalid || 'BGTPLUS_SMR_slagboom' as id,
    "BGTPLUS_SMR_slagboom".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_slagboom".geometrie) as geometrie,
    "BGTPLUS_SMR_slagboom".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_slagboom"

UNION
 SELECT
		"BGTPLUS_SMR_speelvoorziening".identificatie_lokaalid || 'BGTPLUS_SMR_speelvoorziening' as id,
    "BGTPLUS_SMR_speelvoorziening".plus_type as type,
		ST_makeValid(    "BGTPLUS_SMR_speelvoorziening".geometrie) as geometrie,
    "BGTPLUS_SMR_speelvoorziening".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SMR_speelvoorziening"

UNION
 SELECT
		"BGTPLUS_SSR_camera".identificatie_lokaalid || 'BGTPLUS_SSR_camera' as id,
    "BGTPLUS_SSR_camera".plus_type as type,
		ST_makeValid(    "BGTPLUS_SSR_camera".geometrie) as geometrie,
    "BGTPLUS_SSR_camera".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SSR_camera"

UNION
 SELECT
		"BGTPLUS_SSR_flitser".identificatie_lokaalid || 'BGTPLUS_SSR_flitser' as id,
    "BGTPLUS_SSR_flitser".plus_type as type,
		ST_makeValid(    "BGTPLUS_SSR_flitser".geometrie) as geometrie,
    "BGTPLUS_SSR_flitser".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SSR_flitser"

UNION
 SELECT
		"BGTPLUS_SSR_waterstandmeter".identificatie_lokaalid || 'BGTPLUS_SSR_waterstandmeter' as id,
    "BGTPLUS_SSR_waterstandmeter".plus_type as type,
		ST_makeValid(    "BGTPLUS_SSR_waterstandmeter".geometrie) as geometrie,
    "BGTPLUS_SSR_waterstandmeter".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_SSR_waterstandmeter"

UNION
 SELECT
		"BGTPLUS_WDI_meerpaal".identificatie_lokaalid || 'BGTPLUS_WDI_meerpaal' as id,
    "BGTPLUS_WDI_meerpaal".plus_type as type,
		ST_makeValid(    "BGTPLUS_WDI_meerpaal".geometrie) as geometrie,
    "BGTPLUS_WDI_meerpaal".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL  as maxzoom
   FROM bgt."BGTPLUS_WDI_meerpaal"

"""


SELECT_INRICHTINGSELEMENTVLAK_SQL: Final = """
SELECT
		"BGTPLUS_KDL_keermuur".identificatie_lokaalid || 'BGTPLUS_KDL_keermuur' as id,
    "BGTPLUS_KDL_keermuur".plus_type as type,
		ST_makeValid(    "BGTPLUS_KDL_keermuur". geometrie) as geometrie,
     "BGTPLUS_KDL_keermuur".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGTPLUS_KDL_keermuur"
UNION
 SELECT
		"BGTPLUS_OSDG_muur_V".identificatie_lokaalid || 'BGTPLUS_OSDG_muur_V' as id,
     "BGTPLUS_OSDG_muur_V".plus_type as type,
		ST_makeValid(    "BGTPLUS_OSDG_muur_V". geometrie) as geometrie,
     "BGTPLUS_OSDG_muur_V".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGTPLUS_OSDG_muur_V"
UNION
 SELECT
		"BGTPLUS_VGT_haag_V".identificatie_lokaalid || 'BGTPLUS_VGT_haag_V' as id,
    "BGTPLUS_VGT_haag_V".plus_type as type,
		ST_makeValid(    "BGTPLUS_VGT_haag_V". geometrie) as geometrie,
     "BGTPLUS_VGT_haag_V".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGTPLUS_VGT_haag_V"
UNION
 SELECT
		"BGTPLUS_WGI_boomspiegel_V".identificatie_lokaalid || 'BGTPLUS_WGI_boomspiegel_V' as id,
    "BGTPLUS_WGI_boomspiegel_V".plus_type as type,
		ST_makeValid(    "BGTPLUS_WGI_boomspiegel_V". geometrie) as geometrie,
     "BGTPLUS_WGI_boomspiegel_V".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGTPLUS_WGI_boomspiegel_V"
UNION
 SELECT
		"BGTPLUS_WGI_rooster_V".identificatie_lokaalid || 'BGTPLUS_WGI_rooster_V' as id,
    "BGTPLUS_WGI_rooster_V".plus_type as type,
		ST_makeValid(    "BGTPLUS_WGI_rooster_V". geometrie) as geometrie,
     "BGTPLUS_WGI_rooster_V".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_WGI_rooster_V"
UNION
 SELECT
		"BGTPLUS_WGI_wildrooster_V".identificatie_lokaalid || 'BGTPLUS_WGI_wildrooster_V' as id,
    "BGTPLUS_WGI_wildrooster_V".plus_type as type,
		ST_makeValid(    "BGTPLUS_WGI_wildrooster_V". geometrie) as geometrie,
     "BGTPLUS_WGI_wildrooster_V".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGTPLUS_WGI_wildrooster_V"
UNION
 SELECT
		"BGT_KDL_gemaal".identificatie_lokaalid || 'BGT_KDL_gemaal' as id,
    "BGT_KDL_gemaal".bgt_type as type,
		ST_makeValid(    "BGT_KDL_gemaal". geometrie) as geometrie,
     "BGT_KDL_gemaal".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_KDL_gemaal"
UNION
 SELECT
		"BGT_KDL_hoogspanningsmast_V".identificatie_lokaalid || 'BGT_KDL_hoogspanningsmast_V' as id,
    "BGT_KDL_hoogspanningsmast_V".bgt_type as type,
		ST_makeValid(    "BGT_KDL_hoogspanningsmast_V". geometrie) as geometrie,
     "BGT_KDL_hoogspanningsmast_V".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_KDL_hoogspanningsmast_V"
UNION
 SELECT
		"BGT_KDL_sluis".identificatie_lokaalid || 'BGT_KDL_sluis' as id,
    "BGT_KDL_sluis".bgt_type as type,
		ST_makeValid(    "BGT_KDL_sluis". geometrie) as geometrie,
     "BGT_KDL_sluis".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_KDL_sluis"
UNION
 SELECT
		"BGT_KDL_steiger".identificatie_lokaalid || 'BGT_KDL_steiger' as id,
    "BGT_KDL_steiger".bgt_type as type,
		ST_makeValid(    "BGT_KDL_steiger". geometrie) as geometrie,
     "BGT_KDL_steiger".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_KDL_steiger"
UNION
 SELECT
		"BGT_KDL_stuw_V".identificatie_lokaalid || 'BGT_KDL_stuw_V' as id,
    "BGT_KDL_stuw_V".bgt_type as type,
		ST_makeValid(    "BGT_KDL_stuw_V". geometrie) as geometrie,
     "BGT_KDL_stuw_V".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_KDL_stuw_V"
UNION
 SELECT
		"BGT_SDG_kademuur_V".identificatie_lokaalid || 'BGT_SDG_kademuur_V' as id,
    "BGT_SDG_kademuur_V".bgt_type as type,
		ST_makeValid(    "BGT_SDG_kademuur_V". geometrie) as geometrie,
     "BGT_SDG_kademuur_V".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_SDG_kademuur_V"
UNION
 SELECT
		"BGT_SDG_muur_V".identificatie_lokaalid || 'BGT_SDG_muur_V' as id,
    "BGT_SDG_muur_V".bgt_type as type,
		ST_makeValid(    "BGT_SDG_muur_V". geometrie) as geometrie,
     "BGT_SDG_muur_V".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_SDG_muur_V"

    /* KBK10 */
  UNION
  SELECT
		"WDL_waterbassin".ogc_fid::text || 'WDL_waterbassin' as identificatie_lokaal_id,
    'waterbassin' as type,
		ST_makeValid(    "WDL_waterbassin".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
  FROM kbk10."WDL_waterbassin"
  UNION
   SELECT
		"TRN_aanlegsteiger".ogc_fid::text || 'TRN_aanlegsteiger_kbk10' as identificatie_lokaal_id,
    'aanlegsteiger' as type,
		ST_makeValid(    "TRN_aanlegsteiger".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
   FROM kbk10."TRN_aanlegsteiger"
"""

SELECT_SPOORLIJN_SQL: Final = """
  SELECT
 		"BGT_SPR_sneltram".identificatie_lokaalid || 'BGT_SPR_sneltram' as id,
        "BGT_SPR_sneltram".bgt_functie as type,
		ST_makeValid(        "BGT_SPR_sneltram".geometrie) as geometrie,
        "BGT_SPR_sneltram".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_SPR_sneltram"
    WHERE  1=1
UNION
    SELECT
 		"BGT_SPR_tram".identificatie_lokaalid || 'BGT_SPR_tram' as id,
        "BGT_SPR_tram".bgt_functie as type,
		ST_makeValid(        "BGT_SPR_tram".geometrie) as geometrie,
        "BGT_SPR_tram".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_SPR_tram"
UNION
    SELECT
 		"BGT_SPR_trein".identificatie_lokaalid || 'BGT_SPR_trein' as id,
        "BGT_SPR_trein".bgt_functie as type,
		ST_makeValid(        "BGT_SPR_trein".geometrie) as geometrie,
        "BGT_SPR_trein".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_SPR_trein"

UNION

/* KBK10 */
    SELECT
 		"SBL_metro_overdekt".ogc_fid::text || 'SBL_metro_overdekt' as identificatie_lokaal_id,
        'SBL_metro_overdekt' as type,
		ST_makeValid(        "SBL_metro_overdekt".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_metro_overdekt"
UNION
    SELECT
 		"SBL_trein_overdekt_1sp".ogc_fid::text || 'SBL_trein_overdekt_1sp' as identificatie_lokaal_id,
        'trein_overdekt_1sp' as type,
		ST_makeValid(        "SBL_trein_overdekt_1sp".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_trein_overdekt_1sp"
UNION
    SELECT
 		"SBL_trein_overdekt_nsp".ogc_fid::text || 'SBL_trein_overdekt_nsp' as identificatie_lokaal_id,
        'SBL_trein_overdekt_nsp' as type,
		ST_makeValid(        "SBL_trein_overdekt_nsp".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_trein_overdekt_nsp"
UNION
    SELECT
 		"SBL_metro_nietoverdekt_1sp".ogc_fid::text || 'SBL_metro_nietoverdekt_1sp' as identificatie_lokaal_id,
        'SBL_metro_nietoverdekt_1sp' as type,
		ST_makeValid(        "SBL_metro_nietoverdekt_1sp".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_metro_nietoverdekt_1sp"
UNION
    SELECT
 		"SBL_metro_nietoverdekt_nsp".ogc_fid::text || 'SBL_metro_nietoverdekt_nsp' as identificatie_lokaal_id,
        'SBL_metro_nietoverdekt_nsp' as type,
		ST_makeValid(        "SBL_metro_nietoverdekt_nsp".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_metro_nietoverdekt_nsp"
UNION
    SELECT
 		"SBL_trein_ongeelektrificeerd".ogc_fid::text || 'SBL_trein_ongeelektrificeerd_kbk10' as identificatie_lokaal_id,
        'SBL_trein_ongeelektrificeerd' as type,
		ST_makeValid(        "SBL_trein_ongeelektrificeerd".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_trein_ongeelektrificeerd"
UNION
    SELECT
 		"SBL_trein_nietoverdekt_1sp".ogc_fid::text || 'SBL_trein_nietoverdekt_1sp' as identificatie_lokaal_id,
        'SBL_trein_nietoverdekt_1sp' as type,
		ST_makeValid(        "SBL_trein_nietoverdekt_1sp".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_trein_nietoverdekt_1sp"
UNION
    SELECT
 		"SBL_trein_nietoverdekt_nsp".ogc_fid::text || 'SBL_trein_nietoverdekt_nsp' as identificatie_lokaal_id,
        'SBL_trein_nietoverdekt_nsp' as type,
		ST_makeValid(        "SBL_trein_nietoverdekt_nsp".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."SBL_trein_nietoverdekt_nsp"

/* KBK50 */
UNION
    SELECT
 		"SBL_metro_sneltram_in_tunnel".ogc_fid::text || 'SBL_metro_sneltram_in_tunnel' as identificatie_lokaal_id,
        'metro_sneltram_in_tunnel' as type,
		ST_makeValid(        "SBL_metro_sneltram_in_tunnel".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
    FROM kbk50."SBL_metro_sneltram_in_tunnel"
UNION
    SELECT
 		"SBL_trein_in_tunnel".ogc_fid::text || 'SBL_trein_in_tunnel' as identificatie_lokaal_id,
        'trein_in_tunnel' as type,
		ST_makeValid(        "SBL_trein_in_tunnel".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
    FROM kbk50."SBL_trein_in_tunnel"
UNION
    SELECT
 		"SBL_metro_sneltram".ogc_fid::text || 'SBL_metro_sneltram' as identificatie_lokaal_id,
        'metro_sneltram' as type,
		ST_makeValid(        "SBL_metro_sneltram".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
    FROM kbk50."SBL_metro_sneltram"
UNION
    SELECT
 		"SBL_trein_ongeelektrificeerd".ogc_fid::text || 'SBL_trein_ongeelektrificeerd_kbk50' as identificatie_lokaal_id,
        'trein_ongeelektrificeerd' as type,
		ST_makeValid(        "SBL_trein_ongeelektrificeerd".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
    FROM kbk50."SBL_trein_ongeelektrificeerd"
UNION
    SELECT
 		"SBL_trein".ogc_fid::text || 'SBL_trein' as identificatie_lokaal_id,
        'trein' as type,
		ST_makeValid(        "SBL_trein".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
    FROM kbk50."SBL_trein"
"""

SELECT_TERREINDEELVLAK_SQL: Final = """
SELECT
 		"BGT_BTRN_boomteelt".identificatie_lokaalid || 'BGT_BTRN_boomteelt' as id,
    "BGT_BTRN_boomteelt".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_boomteelt".geometrie) as geometrie,
    "BGT_BTRN_boomteelt".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_boomteelt"
UNION
 SELECT
 		"BGT_BTRN_bouwland".identificatie_lokaalid || 'BGT_BTRN_bouwland' as id,
    "BGT_BTRN_bouwland".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_bouwland".geometrie) as geometrie,
    "BGT_BTRN_bouwland".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_bouwland"
UNION
 SELECT
 		"BGT_BTRN_fruitteelt".identificatie_lokaalid || 'BGT_BTRN_fruitteelt' as id,
    "BGT_BTRN_fruitteelt".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_fruitteelt".geometrie) as geometrie,
    "BGT_BTRN_fruitteelt".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_fruitteelt"
UNION
 SELECT
 		"BGT_BTRN_gemengd_bos".identificatie_lokaalid || 'BGT_BTRN_gemengd_bos' as id,
    "BGT_BTRN_gemengd_bos".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_gemengd_bos".geometrie) as geometrie,
    "BGT_BTRN_gemengd_bos".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_gemengd_bos"
UNION
 SELECT
 		"BGT_BTRN_grasland_agrarisch".identificatie_lokaalid ||'-'||  "BGT_BTRN_grasland_agrarisch".tijdstipregistratie ||'-'||  'BGT_BTRN_grasland_agrarisch' as id,
    "BGT_BTRN_grasland_agrarisch".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_grasland_agrarisch".geometrie) as geometrie,
    "BGT_BTRN_grasland_agrarisch".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_grasland_agrarisch"
UNION
 SELECT
 		"BGT_BTRN_grasland_overig".identificatie_lokaalid ||'-'||  "BGT_BTRN_grasland_overig".tijdstipregistratie ||'-'|| 'BGT_BTRN_grasland_overig' as id,
    "BGT_BTRN_grasland_overig".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_grasland_overig".geometrie) as geometrie,
    "BGT_BTRN_grasland_overig".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_grasland_overig"
UNION
 SELECT
 		"BGT_BTRN_groenvoorziening".identificatie_lokaalid || 'BGT_BTRN_groenvoorziening' as id,
    "BGT_BTRN_groenvoorziening".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_groenvoorziening".geometrie) as geometrie,
    "BGT_BTRN_groenvoorziening".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_groenvoorziening"
UNION
 SELECT
 		"BGT_BTRN_houtwal".identificatie_lokaalid || 'BGT_BTRN_houtwal' as id,
    "BGT_BTRN_houtwal".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_houtwal".geometrie) as geometrie,
    "BGT_BTRN_houtwal".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_houtwal"
UNION
 SELECT
 		"BGT_BTRN_loofbos".identificatie_lokaalid || 'BGT_BTRN_loofbos' as id,
    "BGT_BTRN_loofbos".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_loofbos".geometrie) as geometrie,
    "BGT_BTRN_loofbos".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_loofbos"
UNION
 SELECT
 		"BGT_BTRN_moeras".identificatie_lokaalid || 'BGT_BTRN_moeras' as id,
    "BGT_BTRN_moeras".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_moeras".geometrie) as geometrie,
    "BGT_BTRN_moeras".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_moeras"
UNION
 SELECT
 		"BGT_BTRN_naaldbos".identificatie_lokaalid || 'BGT_BTRN_naaldbos' as id,
    "BGT_BTRN_naaldbos".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_naaldbos".geometrie) as geometrie,
    "BGT_BTRN_naaldbos".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_naaldbos"
UNION
 SELECT
 		"BGT_BTRN_rietland".identificatie_lokaalid || 'BGT_BTRN_rietland' as id,
    "BGT_BTRN_rietland".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_rietland".geometrie) as geometrie,
    "BGT_BTRN_rietland".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_rietland"
UNION
 SELECT
 		"BGT_BTRN_struiken".identificatie_lokaalid || 'BGT_BTRN_struiken' as id,
    "BGT_BTRN_struiken".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_struiken".geometrie) as geometrie,
    "BGT_BTRN_struiken".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_struiken"
UNION
 SELECT
 		"BGT_KDL_perron".identificatie_lokaalid || 'BGT_KDL_perron' as id,
    "BGT_KDL_perron".bgt_type as type,
		ST_makeValid(    "BGT_KDL_perron".geometrie) as geometrie,
    "BGT_KDL_perron".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_KDL_perron"
UNION
 SELECT
 		"BGT_KDL_strekdam".identificatie_lokaalid || 'BGT_KDL_strekdam' as id,
    "BGT_KDL_strekdam".bgt_type as type,
		ST_makeValid(    "BGT_KDL_strekdam".geometrie) as geometrie,
    "BGT_KDL_strekdam".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_KDL_strekdam"
UNION
 SELECT
 		"BGT_OTRN_erf".identificatie_lokaalid ||'-'||  	"BGT_OTRN_erf".tijdstipregistratie ||'-'|| 'BGT_OTRN_erf' as id,
    "BGT_OTRN_erf".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_OTRN_erf".geometrie) as geometrie,
    "BGT_OTRN_erf".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OTRN_erf"
UNION
 SELECT
 		"BGT_OTRN_gesloten_verharding".identificatie_lokaalid || 'BGT_OTRN_gesloten_verharding' as id,
    "BGT_OTRN_gesloten_verharding".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_OTRN_gesloten_verharding".geometrie) as geometrie,
    "BGT_OTRN_gesloten_verharding".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OTRN_gesloten_verharding"
UNION
 SELECT
 		"BGT_OTRN_half_verhard".identificatie_lokaalid || 'BGT_OTRN_half_verhard' as id,
    "BGT_OTRN_half_verhard".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_OTRN_half_verhard".geometrie) as geometrie,
    "BGT_OTRN_half_verhard".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OTRN_half_verhard"
UNION
 SELECT
 		 "BGT_OTRN_onverhard".identificatie_lokaalid || 'BGT_OTRN_onverhard' as id,
    "BGT_OTRN_onverhard".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_OTRN_onverhard".geometrie) as geometrie,
    "BGT_OTRN_onverhard".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OTRN_onverhard"
UNION
 SELECT
 		"BGT_OTRN_open_verharding".identificatie_lokaalid || 'BGT_OTRN_open_verharding' as id,
    "BGT_OTRN_open_verharding".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_OTRN_open_verharding".geometrie) as geometrie,
    "BGT_OTRN_open_verharding".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OTRN_open_verharding"
UNION
 SELECT
 		"BGT_OTRN_zand".identificatie_lokaalid || 'BGT_OTRN_zand' as id,
    "BGT_OTRN_zand".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_OTRN_zand".geometrie) as geometrie,
    "BGT_OTRN_zand".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OTRN_zand"
UNION
 SELECT
 		"BGT_OWDL_oever_slootkant".identificatie_lokaalid ||'-'||  	"BGT_OWDL_oever_slootkant".tijdstipregistratie ||'-'|| 'BGT_OWDL_oever_slootkant' as id,
    "BGT_OWDL_oever_slootkant".bgt_type as type,
		ST_makeValid(    "BGT_OWDL_oever_slootkant".geometrie) as geometrie,
    "BGT_OWDL_oever_slootkant".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_OWDL_oever_slootkant"
UNION
 SELECT
 		"BGT_WGL_spoorbaan".identificatie_lokaalid || 'BGT_WGL_spoorbaan' as id,
    "BGT_WGL_spoorbaan".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_WGL_spoorbaan".geometrie) as geometrie,
    "BGT_WGL_spoorbaan".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_WGL_spoorbaan"
UNION
 SELECT
 		"BGT_BTRN_heide".identificatie_lokaalid || 'BGT_BTRN_heide' as id,
    "BGT_BTRN_heide".bgt_fysiekvoorkomen as type,
		ST_makeValid(    "BGT_BTRN_heide".geometrie) as geometrie,
    "BGT_BTRN_heide".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22 as maxzoom
   FROM bgt."BGT_BTRN_heide"

    /* KBK10 */

  UNION
  SELECT
		"TRN_basaltblokken_steenglooiing".ogc_fid::text || 'TRN_basaltblokken_steenglooiing_kbk10' as identificatie_lokaal_id,
    'basaltblokken_steenglooiing' as type,
		ST_makeValid(    "TRN_basaltblokken_steenglooiing".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_basaltblokken_steenglooiing"
  UNION
  SELECT
		"TRN_grasland".ogc_fid::text || 'TRN_grasland_kbk10' as identificatie_lokaal_id,
    'grasland overig' as type,
		ST_makeValid(    "TRN_grasland".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_grasland"
  UNION
  SELECT
		"TRN_akkerland".ogc_fid::text || 'TRN_akkerland_kbk10' as identificatie_lokaal_id,
    'bouwland' as type,
		ST_makeValid(    "TRN_akkerland".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_akkerland"
  UNION
  SELECT
	  "TRN_overig".ogc_fid::text || 'TRN_overig_kbk10' as identificatie_lokaal_id,
	  'overig' as type,
	  ST_makeValid(    "TRN_overig".geom) as geometrie,
	  0  as relatievehoogteligging,
	  'kbk10' as bron,
	  13  as minzoom,
	  15 as maxzoom
  FROM kbk10."TRN_overig"
  UNION
  SELECT
		"TRN_bedrijfsterrein".ogc_fid::text || 'TRN_bedrijfsterrein_kbk10' as identificatie_lokaal_id,
    'bedrijfsterrein' as type,
		ST_makeValid(    "TRN_bedrijfsterrein".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_bedrijfsterrein"
  UNION
  SELECT
		"TRN_openbaar_groen".ogc_fid::text || 'TRN_openbaar_groen_kbk10' as identificatie_lokaal_id,
    'groenvoorziening' as type,
		ST_makeValid(    "TRN_openbaar_groen".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_openbaar_groen"
  UNION
  SELECT
		"TRN_zand".ogc_fid::text || 'TRN_zand_kbk10' as identificatie_lokaal_id,
    'zand' as type,
		ST_makeValid(    "TRN_zand".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_zand"
  UNION
  SELECT
		"TRN_bos-loofbos".ogc_fid::text || 'TRN_bos-loofbos_kbk10' as identificatie_lokaal_id,
    'loofbos' as type,
		ST_makeValid(    "TRN_bos-loofbos".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_bos-loofbos"
  UNION
  SELECT
		"TRN_bos-naaldbos".ogc_fid::text || 'TRN_bos-naaldbos_kbk10' as identificatie_lokaal_id,
    'naaldbos' as type,
		ST_makeValid(    "TRN_bos-naaldbos".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_bos-naaldbos"
  UNION
  SELECT
		"TRN_bos-gemengd_bos".ogc_fid::text || 'TRN_bos-gemengd_bos_kbk10' as identificatie_lokaal_id,
    'gemengd bos' as type,
		ST_makeValid(    "TRN_bos-gemengd_bos".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_bos-gemengd_bos"
  UNION
  SELECT
		"TRN_bos-griend".ogc_fid::text || 'TRN_bos-griend_kbk10' as identificatie_lokaal_id,
    'griend' as type,
		ST_makeValid(    "TRN_bos-griend".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_bos-griend"
  UNION
  SELECT
		"TRN_boomgaard".ogc_fid::text || 'TRN_boomgaard_kbk10' as identificatie_lokaal_id,
    'boomgaard' as type,
		ST_makeValid(    "TRN_boomgaard".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_boomgaard"
  UNION
  SELECT
		"TRN_boomkwekerij".ogc_fid::text || 'TRN_boomkwekerij_kbk10' as identificatie_lokaal_id,
    'boomteelt' as type,
		ST_makeValid(    "TRN_boomkwekerij".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_boomkwekerij"
  UNION
  SELECT
		"TRN_dodenakker".ogc_fid::text || 'TRN_dodenakker_kbk10' as identificatie_lokaal_id,
    'dodenakker' as type,
		ST_makeValid(    "TRN_dodenakker".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_dodenakker"
  UNION
  SELECT
		"TRN_dodenakker_met_bos".ogc_fid::text || 'TRN_dodenakker_met_bos_kbk10' as identificatie_lokaal_id,
    'dodenakker_met_bos' as type,
		ST_makeValid(    "TRN_dodenakker_met_bos".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_dodenakker_met_bos"
  UNION
  SELECT
		"TRN_fruitkwekerij".ogc_fid::text || 'TRN_fruitkwekerij_kbk10' as identificatie_lokaal_id,
    'fruitteelt' as type,
		ST_makeValid(    "TRN_fruitkwekerij".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_fruitkwekerij"
  UNION
  SELECT
		"TRN_binnentuin".ogc_fid::text || 'TRN_binnentuin_kbk10' as identificatie_lokaal_id,
    'binnentuin' as type,
		ST_makeValid(    "TRN_binnentuin".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	15  as minzoom,
 	 	15 as maxzoom
  FROM kbk10."TRN_binnentuin"
UNION
 SELECT
		"TRN_agrarisch".ogc_fid::text || 'TRN_agrarisch_kbk50' as identificatie_lokaal_id,
    'bouwland' as type,
		ST_makeValid(    "TRN_agrarisch".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as minzoom
  FROM kbk50."TRN_agrarisch"
 UNION
 SELECT
		"TRN_overig".ogc_fid::text || 'TRN_overig_kbk50' as identificatie_lokaal_id,
    'overig' as type,
		ST_makeValid(    "TRN_overig".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as minzoom
  FROM kbk50."TRN_overig"
 UNION
 SELECT
		"TRN_bedrijfsterrein_dienstverlening".ogc_fid::text || 'TRN_bedrijfsterrein_dienstverlening_kbk50' as identificatie_lokaal_id,
    'bedrijfsterrein' as type,
		ST_makeValid(    "TRN_bedrijfsterrein_dienstverlening".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as minzoom
  FROM kbk50."TRN_bedrijfsterrein_dienstverlening"
 UNION
 SELECT
		"TRN_bos_groen_sport".ogc_fid::text || 'TRN_bos_groen_sport_kbk50' as identificatie_lokaal_id,
    'bos_groen_sport' as type,
		ST_makeValid(    "TRN_bos_groen_sport".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as minzoom
  FROM kbk50."TRN_bos_groen_sport"
 UNION
 SELECT
		"TRN_zand".ogc_fid::text || 'TRN_zand_kbk50' as identificatie_lokaal_id,
    'zand' as type,
		ST_makeValid(    "TRN_zand".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as minzoom
  FROM kbk50."TRN_zand"
"""


SELECT_WATERDEELLIJN_SQL: Final = """
SELECT
 		"BGTPLUS_KDL_duiker_L".identificatie_lokaalid || 'BGTPLUS_KDL_duiker_L' as id,
    "BGTPLUS_KDL_duiker_L".plus_type as type,
		ST_makeValid(    "BGTPLUS_KDL_duiker_L".geometrie) as geometrie,
    "BGTPLUS_KDL_duiker_L".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
   FROM bgt."BGTPLUS_KDL_duiker_L"
  WHERE  1=1

/* --- KBK50 ---- */

 UNION
 SELECT
 		"WDL_brede_waterloop".ogc_fid::text || 'WDL_brede_waterloop' as identificatie_lokaal_id,
    'brede_waterloop' as type,
		ST_makeValid(    "WDL_brede_waterloop".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
   FROM kbk50."WDL_brede_waterloop"

 UNION
 SELECT
 		"WDL_smalle_waterloop".ogc_fid::text || 'WDL_smalle_waterloop' as identificatie_lokaal_id,
    'smalle_waterloop' as type,
		ST_makeValid(    "WDL_smalle_waterloop".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	NULL::int  as minzoom,
 	 	12  as maxzoom
   FROM kbk50."WDL_smalle_waterloop"
"""

SELECT_WATERDEELVLAK_SQL: Final = """
SELECT
 		"BGTPLUS_KDL_duiker_V".identificatie_lokaalid || 'BGTPLUS_KDL_duiker_V' as id,
    "BGTPLUS_KDL_duiker_V".plus_type as type,
		ST_makeValid(    "BGTPLUS_KDL_duiker_V". geometrie) as geometrie,
    "BGTPLUS_KDL_duiker_V".relatievehoogteligging,
 	 	'bgtplus' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGTPLUS_KDL_duiker_V"
UNION
 SELECT
 		"BGT_WDL_greppel_droge_sloot".identificatie_lokaalid || 'BGT_WDL_greppel_droge_sloot' as id,
    "BGT_WDL_greppel_droge_sloot".bgt_type as type,
		ST_makeValid(    "BGT_WDL_greppel_droge_sloot". geometrie) as geometrie,
    "BGT_WDL_greppel_droge_sloot".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_WDL_greppel_droge_sloot"
UNION
 SELECT
 		"BGT_WDL_waterloop".identificatie_lokaalid ||'-'|| "BGT_WDL_waterloop".tijdstipregistratie ||'-'|| 'BGT_WDL_waterloop' as id,
    "BGT_WDL_waterloop".bgt_type as type,
		ST_makeValid(    "BGT_WDL_waterloop". geometrie) as geometrie,
    "BGT_WDL_waterloop".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_WDL_waterloop"
UNION
 SELECT
 		"BGT_WDL_watervlakte".identificatie_lokaalid || 'BGT_WDL_watervlakte' as id,
    "BGT_WDL_watervlakte".bgt_type as type,
		ST_makeValid(    "BGT_WDL_watervlakte". geometrie) as geometrie,
    "BGT_WDL_watervlakte".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	22  as maxzoom
   FROM bgt."BGT_WDL_watervlakte"

  /* KBK10 */

 UNION
 SELECT
 		"WDL_breed_water".ogc_fid::text || 'WDL_breed_water' as identificatie_lokaal_id,
    'breed_water' as type,
		ST_makeValid(    "WDL_breed_water".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
   FROM kbk10."WDL_breed_water"
  UNION
   SELECT
		"WDL_haven".ogc_fid::text || 'WDL_haven_kbk10' as identificatie_lokaal_id,
    'haven' as type,
		ST_makeValid(    "WDL_haven".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
   FROM kbk10."WDL_haven"

/* --- KBK50 ---- */

 UNION
 SELECT
 		"WDL_wateroppervlak".ogc_fid::text || 'WDL_wateroppervlak' as identificatie_lokaal_id,
    'breed_water' as type,
		ST_makeValid(    "WDL_wateroppervlak".geom) as geometrie,
    0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8  as minzoom,
 	 	12  as maxzoom
   FROM kbk50."WDL_wateroppervlak"
"""


SELECT_WEGDEELLIJN_SQL: Final = """
/* --- KBK10 ---- */
    select
        "WGL_smalle_weg".ogc_fid::text ||'-'|| 'WGL_smalle_weg' as id,
        'smalle_weg' as type,
		ST_makeValid(        "WGL_smalle_weg".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    from
        kbk10."WGL_smalle_weg"

    UNION ALL
      select
        "WGL_autoveer".ogc_fid::text ||'-'|| 'WGL_autoveer' as id,
        'autoveer' as type,
		ST_makeValid(        "WGL_autoveer".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    from
        kbk10."WGL_autoveer"

    UNION ALL
      select
        "WGL_hartlijn".ogc_fid::text ||'-'|| 'WGL_hartlijn' as id,
        'hartlijn' as type,
		ST_makeValid(        "WGL_hartlijn".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    from
        kbk10."WGL_hartlijn"

UNION
    SELECT
        "WGL_voetveer".ogc_fid::text ||'-'|| 'WGL_voetveer' as id,
        'voetveer' as type,
		ST_makeValid(        "WGL_voetveer".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13 as minzoom,
 	 	15  as maxzoom
    FROM kbk10."WGL_voetveer"

 /* --- KBK50 ---- */
UNION
    SELECT
        "WGL_straat_in_tunnel".ogc_fid::text ||'-'|| 'WGL_straat_in_tunnel' as id,
        'straat_in_tunnel' as type,
		ST_makeValid(        "WGL_straat_in_tunnel".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as maxzoom
    FROM kbk50."WGL_straat_in_tunnel"
UNION
    SELECT
        "WGL_hoofdweg_in_tunnel".ogc_fid::text ||'-'|| 'WGL_hoofdweg_in_tunnel' as id,
        'hoofdweg_in_tunnel' as type,
		ST_makeValid(        "WGL_hoofdweg_in_tunnel".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12 as maxzoom
    FROM kbk50."WGL_hoofdweg_in_tunnel"
UNION
    SELECT
        "WGL_regionale_weg".ogc_fid::text ||'-'|| 'WGL_regionale_weg' as id,
        'regionale_weg' as type,
		ST_makeValid(        "WGL_regionale_weg".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_regionale_weg"
UNION
    SELECT
        "WGL_regionale_weg_in_tunnel".ogc_fid::text ||'-'|| 'WGL_regionale_weg_in_tunnel' as id,
        'regionale_weg_in_tunnel' as type,
		ST_makeValid(        "WGL_regionale_weg_in_tunnel".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_regionale_weg_in_tunnel"
UNION
    SELECT
        "WGL_autosnelweg_in_tunnel".ogc_fid::text ||'-'|| 'WGL_autosnelweg_in_tunnel' as id,
        'autosnelweg_in_tunnel' as type,
		ST_makeValid(        "WGL_autosnelweg_in_tunnel".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_autosnelweg_in_tunnel"
UNION
    SELECT
        "WGL_straat".ogc_fid::text ||'-'|| 'WGL_straat' as id,
        'straat' as type,
		ST_makeValid(        "WGL_straat".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_straat"
UNION
    SELECT
        "WGL_hoofdweg".ogc_fid::text ||'-'|| 'WGL_hoofdweg' as id,
        'hoofdweg' as type,
		ST_makeValid(        "WGL_hoofdweg".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_hoofdweg"
UNION
    SELECT
        "WGL_autosnelweg".ogc_fid::text ||'-'|| 'WGL_autosnelweg' as id,
        'autosnelweg' as type,
		ST_makeValid(        "WGL_autosnelweg".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_autosnelweg"
UNION
    SELECT
        "WGL_veerverbinding".ogc_fid::text ||'-'|| 'WGL_veerverbinding' as id,
        'veerverbinding' as type,
		ST_makeValid(        "WGL_veerverbinding".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk50' as bron,
 	 	8 as minzoom,
 	 	12  as maxzoom
    FROM kbk50."WGL_veerverbinding"
"""

SELECT_WEGDEELVLAK_SQL: Final = """
 SELECT
 		"BGT_OWGL_berm".identificatie_lokaalid ||'-'|| "BGT_OWGL_berm".tijdstipregistratie ||'-'|| 'BGT_OWGL_berm' as id,
        "BGT_OWGL_berm".bgt_functie as type,
        "BGT_OWGL_berm".bgt_fysiekvoorkomen as subtype,
        "BGT_OWGL_berm".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_OWGL_berm". geometrie) as geometrie,
        "BGT_OWGL_berm".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_OWGL_berm"
UNION
    SELECT
 		"BGT_OWGL_verkeerseiland".identificatie_lokaalid || 'BGT_OWGL_verkeerseiland' as id,
        "BGT_OWGL_verkeerseiland".bgt_functie as type,
        "BGT_OWGL_verkeerseiland".bgt_fysiekvoorkomen as subtype,
        "BGT_OWGL_verkeerseiland".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_OWGL_verkeerseiland". geometrie) as geometrie,
        "BGT_OWGL_verkeerseiland".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_OWGL_verkeerseiland"
UNION
    SELECT
 		"BGT_WGL_baan_voor_vliegverkeer".identificatie_lokaalid || 'BGT_WGL_baan_voor_vliegverkeer' as id,
        "BGT_WGL_baan_voor_vliegverkeer".bgt_functie as type,
        "BGT_WGL_baan_voor_vliegverkeer".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_baan_voor_vliegverkeer".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_baan_voor_vliegverkeer". geometrie) as geometrie,
        "BGT_WGL_baan_voor_vliegverkeer".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_baan_voor_vliegverkeer"
UNION
    SELECT
 		"BGT_WGL_fietspad".identificatie_lokaalid ||'-'|| "BGT_WGL_fietspad".tijdstipregistratie ||'-'||  'BGT_WGL_fietspad' as id,
        "BGT_WGL_fietspad".bgt_functie as type,
        "BGT_WGL_fietspad".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_fietspad".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_fietspad". geometrie) as geometrie,
        "BGT_WGL_fietspad".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_fietspad"
UNION
    SELECT
 		"BGT_WGL_inrit".identificatie_lokaalid || 'BGT_WGL_inrit' as id,
        "BGT_WGL_inrit".bgt_functie as type,
        "BGT_WGL_inrit".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_inrit".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_inrit". geometrie) as geometrie,
        "BGT_WGL_inrit".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_inrit"
UNION
    SELECT
 		"BGT_WGL_ov-baan".identificatie_lokaalid || 'BGT_WGL_ov-baan' as id,
        "BGT_WGL_ov-baan".bgt_functie as type,
        "BGT_WGL_ov-baan".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_ov-baan".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_ov-baan". geometrie) as geometrie,
        "BGT_WGL_ov-baan".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_ov-baan"
UNION
    SELECT
 		"BGT_WGL_overweg".identificatie_lokaalid || 'BGT_WGL_overweg' as id,
        "BGT_WGL_overweg".bgt_functie as type,
        "BGT_WGL_overweg".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_overweg".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_overweg". geometrie) as geometrie,
        "BGT_WGL_overweg".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_overweg"
UNION
    SELECT
 		"BGT_WGL_parkeervlak".identificatie_lokaalid || 'BGT_WGL_parkeervlak' as id,
        "BGT_WGL_parkeervlak".bgt_functie as type,
        "BGT_WGL_parkeervlak".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_parkeervlak".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_parkeervlak". geometrie) as geometrie,
        "BGT_WGL_parkeervlak".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_parkeervlak"
UNION
    SELECT
 		"BGT_WGL_rijbaan_autosnelweg".identificatie_lokaalid ||'-'|| "BGT_WGL_rijbaan_autosnelweg".tijdstipregistratie ||'-'||  'BGT_WGL_rijbaan_autosnelweg' as id,
        "BGT_WGL_rijbaan_autosnelweg".bgt_functie as type,
        "BGT_WGL_rijbaan_autosnelweg".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_rijbaan_autosnelweg".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_rijbaan_autosnelweg". geometrie) as geometrie,
        "BGT_WGL_rijbaan_autosnelweg".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_rijbaan_autosnelweg"
UNION
    SELECT
 		"BGT_WGL_rijbaan_autoweg".identificatie_lokaalid ||'-'|| "BGT_WGL_rijbaan_autoweg".tijdstipregistratie ||'-'||  'BGT_WGL_rijbaan_autoweg' as id,
        "BGT_WGL_rijbaan_autoweg".bgt_functie as type,
        "BGT_WGL_rijbaan_autoweg".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_rijbaan_autoweg".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_rijbaan_autoweg". geometrie) as geometrie,
        "BGT_WGL_rijbaan_autoweg".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_rijbaan_autoweg"
UNION
    SELECT
 		"BGT_WGL_rijbaan_lokale_weg".identificatie_lokaalid ||'-'|| "BGT_WGL_rijbaan_lokale_weg".tijdstipregistratie ||'-'||   'BGT_WGL_rijbaan_lokale_weg' as id,
        "BGT_WGL_rijbaan_lokale_weg".bgt_functie as type,
        "BGT_WGL_rijbaan_lokale_weg".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_rijbaan_lokale_weg".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_rijbaan_lokale_weg". geometrie) as geometrie,
        "BGT_WGL_rijbaan_lokale_weg".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_rijbaan_lokale_weg"
UNION
    SELECT
 		"BGT_WGL_rijbaan_regionale_weg".identificatie_lokaalid ||'-'|| "BGT_WGL_rijbaan_regionale_weg".tijdstipregistratie ||'-'|| 'BGT_WGL_rijbaan_regionale_weg' as id,
        "BGT_WGL_rijbaan_regionale_weg".bgt_functie as type,
        "BGT_WGL_rijbaan_regionale_weg".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_rijbaan_regionale_weg".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_rijbaan_regionale_weg". geometrie) as geometrie,
        "BGT_WGL_rijbaan_regionale_weg".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_rijbaan_regionale_weg"
UNION
    SELECT
 		"BGT_WGL_ruiterpad".identificatie_lokaalid ||'-'|| "BGT_WGL_ruiterpad".tijdstipregistratie ||'-'||  'BGT_WGL_ruiterpad' as id,
        "BGT_WGL_ruiterpad".bgt_functie as type,
        "BGT_WGL_ruiterpad".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_ruiterpad".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_ruiterpad". geometrie) as geometrie,
        "BGT_WGL_ruiterpad".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_ruiterpad"
UNION
    SELECT
 		"BGT_WGL_voetgangersgebied".identificatie_lokaalid ||'-'|| "BGT_WGL_voetgangersgebied".tijdstipregistratie ||'-'||  'BGT_WGL_voetgangersgebied' as id,
        "BGT_WGL_voetgangersgebied".bgt_functie as type,
        "BGT_WGL_voetgangersgebied".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_voetgangersgebied".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_voetgangersgebied". geometrie) as geometrie,
        "BGT_WGL_voetgangersgebied".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_voetgangersgebied"
UNION
    SELECT
 		"BGT_WGL_voetpad".identificatie_lokaalid ||'-'|| "BGT_WGL_voetpad".tijdstipregistratie ||'-'||  'BGT_WGL_voetpad' as id,
        "BGT_WGL_voetpad".bgt_functie as type,
        "BGT_WGL_voetpad".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_voetpad".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_voetpad". geometrie) as geometrie,
        "BGT_WGL_voetpad".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_voetpad"
UNION
    SELECT
        "BGT_WGL_voetpad_op_trap".identificatie_lokaalid ||'-'|| "BGT_WGL_voetpad_op_trap".tijdstipregistratie ||'-'||  'BGT_WGL_voetpad_op_trap' as id,
        "BGT_WGL_voetpad_op_trap".bgt_functie as type,
        "BGT_WGL_voetpad_op_trap".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_voetpad_op_trap".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_voetpad_op_trap". geometrie) as geometrie,
        "BGT_WGL_voetpad_op_trap".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_voetpad_op_trap"
UNION
    SELECT
        "BGT_WGL_woonerf".identificatie_lokaalid ||'-'|| "BGT_WGL_woonerf".tijdstipregistratie ||'-'||  'BGT_WGL_woonerf' as id,
        "BGT_WGL_woonerf".bgt_functie as type,
        "BGT_WGL_woonerf".bgt_fysiekvoorkomen as subtype,
        "BGT_WGL_woonerf".plus_fysiekvoorkomen as subsubtype,
		ST_makeValid(        "BGT_WGL_woonerf". geometrie) as geometrie,
        "BGT_WGL_woonerf".relatievehoogteligging,
 	 	'bgt' as bron,
 	 	16  as minzoom,
 	 	NULL::int  as maxzoom
    FROM bgt."BGT_WGL_woonerf"
UNION

    /* KBK10 */

    SELECT
        "WGL_startbaan_landingsbaan".ogc_fid::text ||'-'|| 'WGL_startbaan_landingsbaan' as identificatie_lokaal_id,
        'startbaan_landingsbaan' as type,
        NULL as subtype,
        NULL as subsubtype,
		ST_makeValid(        "WGL_startbaan_landingsbaan".geom) as geometrie,
        0  as relatievehoogteligging,
 	 	'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."WGL_startbaan_landingsbaan"
UNION
    SELECT
        "TRN_spoorbaanlichaam".ogc_fid::text ||'-'|| 'TRN_spoorbaanlichaam' as identificatie_lokaal_id,
        'spoorbaanlichaam' as type,
        NULL as subtype,
        NULL as subsubtype,
		ST_makeValid(        "TRN_spoorbaanlichaam".geom) as geometrie,
        0  as relatievehoogteligging,
 	    'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."TRN_spoorbaanlichaam"
UNION
    SELECT
        "WGL_autosnelweg".ogc_fid::text ||'-'|| 'WGL_autosnelweg' as identificatie_lokaal_id,
        'autosnelweg' as type,
        NULL as subtype,
        NULL as subsubtype,
		ST_makeValid(        "WGL_autosnelweg".geom) as geometrie,
        0  as relatievehoogteligging,
 	    'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."WGL_autosnelweg"
UNION
    SELECT
        "WGL_autosnelweg".ogc_fid::text ||'-'|| 'WGL_autosnelweg' as identificatie_lokaal_id,
        'autosnelweg' as type,
        NULL as subtype,
        NULL as subsubtype,
		ST_makeValid(        "WGL_autosnelweg".geom) as geometrie,
        0  as relatievehoogteligging,
 	    'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."WGL_autosnelweg"
UNION
    SELECT
        "WGL_rolbaan_platform".ogc_fid::text ||'-'|| 'WGL_rolbaan_platform' as identificatie_lokaal_id,
        'rolbaan_platform' as type,
        NULL as subtype,
        NULL as subsubtype,
		ST_makeValid(        "WGL_rolbaan_platform".geom) as geometrie,
        0  as relatievehoogteligging,
 	    'kbk10' as bron,
 	 	13  as minzoom,
 	 	15  as maxzoom
    FROM kbk10."WGL_rolbaan_platform"
"""

SELECT_LABELS_SQL: Final = """
SELECT
    "BAG_LBL_Ligplaatsnummeraanduidingreeks"."BAG_identificatie" || 'BAG_LBL_Ligplaatsnummeraanduidingreeks' as id,
    'ligplaats' as type,
	ST_GeometryN(ST_makeValid("BAG_LBL_Ligplaatsnummeraanduidingreeks".geometrie),1) as geometrie,
    "BAG_LBL_Ligplaatsnummeraanduidingreeks".hoek,
    "BAG_LBL_Ligplaatsnummeraanduidingreeks".tekst,
 	'bag/bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BAG_LBL_Ligplaatsnummeraanduidingreeks"
UNION
SELECT
    "BAG_LBL_Standplaatsnummeraanduidingreeks"."BAG_identificatie" || 'BAG_LBL_Standplaatsnummeraanduidingreeks' as id,
    'standplaats' as type,
	ST_GeometryN(ST_makeValid("BAG_LBL_Standplaatsnummeraanduidingreeks".geometrie),1) as geometrie,
    "BAG_LBL_Standplaatsnummeraanduidingreeks".hoek,
    "BAG_LBL_Standplaatsnummeraanduidingreeks".tekst,
 	'bag/bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BAG_LBL_Standplaatsnummeraanduidingreeks"
UNION
SELECT
    "BGT_LBL_administratief_gebied".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_LBL_administratief_gebied".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'|| 'BGT_LBL_administratief_gebied' as id,
    "BGT_LBL_administratief_gebied".openbareruimtetype as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_administratief_gebied".geometrie),1) as geometrie,
    "BGT_LBL_administratief_gebied".hoek,
    "BGT_LBL_administratief_gebied".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_administratief_gebied"
UNION
SELECT
    "BGT_LBL_kunstwerk".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_LBL_kunstwerk".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'||  'BGT_LBL_kunstwerk' as id,
    "BGT_LBL_kunstwerk".openbareruimtetype as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_kunstwerk".geometrie),1) as geometrie,
    "BGT_LBL_kunstwerk".hoek,
    "BGT_LBL_kunstwerk".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_kunstwerk"
UNION
SELECT
    "BGT_LBL_landschappelijk_gebied".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_LBL_landschappelijk_gebied".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'|| 'BGT_LBL_landschappelijk_gebied' as id,
    "BGT_LBL_landschappelijk_gebied".openbareruimtetype as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_landschappelijk_gebied".geometrie),1) as geometrie,
    "BGT_LBL_landschappelijk_gebied".hoek,
    "BGT_LBL_landschappelijk_gebied".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_landschappelijk_gebied"
UNION
SELECT
    "BGT_LBL_nummeraanduidingreeks".identificatie_lokaalid ||'-'|| "BGT_LBL_nummeraanduidingreeks".tekst ||'-'|| "BGT_LBL_nummeraanduidingreeks".ogc_fid ||'-'|| 'BGT_LBL_nummeraanduidingreeks' as id,
    'nummeraanduiding' as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_nummeraanduidingreeks".geometrie),1) as geometrie,
    "BGT_LBL_nummeraanduidingreeks".hoek,
    "BGT_LBL_nummeraanduidingreeks".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_nummeraanduidingreeks"
UNION
SELECT
    "BGT_LBL_terrein".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_LBL_terrein".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'|| 'BGT_LBL_terrein' as id,
    "BGT_LBL_terrein".openbareruimtetype as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_terrein".geometrie),1) as geometrie,
    "BGT_LBL_terrein".hoek,
    "BGT_LBL_terrein".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_terrein"
UNION
SELECT
    "BGT_LBL_water".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_LBL_water".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'|| 'BGT_LBL_water' as id,
    "BGT_LBL_water".openbareruimtetype as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_water".geometrie),1) as geometrie,
    "BGT_LBL_water".hoek,
    "BGT_LBL_water".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_water"
UNION
SELECT
    "BGT_LBL_weg".identificatie_lokaalid ||'-'|| row_number() over (partition by "BGT_LBL_weg".identificatie_lokaalid order by tijdstipregistratie desc ) ||'-'|| 'BGT_LBL_weg' as id,
    "BGT_LBL_weg".openbareruimtetype as type,
	ST_GeometryN(ST_makeValid("BGT_LBL_weg".geometrie),1) as geometrie,
    "BGT_LBL_weg".hoek,
    "BGT_LBL_weg".tekst,
 	'bgt' as bron,
 	16 as minzoom,
 	22 as maxzoom
   FROM bgt."BGT_LBL_weg"
"""
