#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py "parkeerzones/20190606_Vergunninggebieden v3.zip"
unzip $TMPDIR/20190606_Vergunninggebieden\ v3.zip -d ${TMPDIR}

ogr2ogr -f "PGDump" -t_srs EPSG:28992 -s_srs EPSG:4326 -nln parkeerzones_new ${TMPDIR}/parkeerzones.sql ${TMPDIR}/20190606_Vergunninggebieden/act_RDW_3EXP_VERGUNNINGGEBIED.shp
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -s_srs EPSG:4326 -nln parkeerzones_uitz_new ${TMPDIR}/parkeerzones_uitz.sql ${TMPDIR}/20190606_Vergunninggebieden/act_RDW_3EXP_UITZ_VERGUNNINGGEBIED.shp

iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/parkeerzones.sql > ${TMPDIR}/parkeerzones.utf8.sql
iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/parkeerzones_uitz.sql > ${TMPDIR}/parkeerzones_uitz.utf8.sql

echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/parkeerzones.utf8.sql
ALTER TABLE parkeerzones_new RENAME COLUMN b_dat_gebi TO begin_datum_gebied;
ALTER TABLE parkeerzones_new RENAME COLUMN gebied_cod TO gebied_code;
ALTER TABLE parkeerzones_new RENAME COLUMN gebied_oms TO gebied_omschrijving;
ALTER TABLE parkeerzones_new RENAME COLUMN e_dat_gebi TO eind_datum_gebied;
ALTER TABLE parkeerzones_new RENAME COLUMN domein_cod TO domein_code;
ALTER TABLE parkeerzones_new RENAME COLUMN gebruiks_d TO gebruiks_doel;
ALTER TABLE parkeerzones_new RENAME COLUMN gebied_naa TO gebied_naam;
ALTER TABLE parkeerzones_new ADD COLUMN color VARCHAR(7);
DROP TABLE IF EXISTS parkeerzones_map_color;
\i ${DATA_DIR}/parkeerzones_map_color.sql
UPDATE parkeerzones_new p SET color = pmc.color
FROM parkeerzones_map_color pmc WHERE p.ogc_fid = pmc.ogc_fid AND p.gebied_code = pmc.gebied_code;
DROP TABLE parkeerzones_map_color;
\i ${TMPDIR}/parkeerzones_uitz.utf8.sql
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN b_dat_gebi TO begin_datum_gebied;
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN gebied_cod TO gebied_code;
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN omschrijvi TO omschrijving;
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN e_dat_gebi TO eind_datum_gebied;
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN domein_cod TO domein_code;
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN gebruiks_d TO gebruiks_doel;
ALTER TABLE parkeerzones_uitz_new RENAME COLUMN gebied_naa TO gebied_naam;
-- Delete data  that is not used.
DELETE FROM parkeerzones_uitz_new WHERE show = 'FALSE';
DELETE FROM parkeerzones_new WHERE show = 'FALSE';
SQL

echo "Check imported data"
${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS parkeerzones RENAME TO parkeerzones_old;
ALTER TABLE parkeerzones_new RENAME TO parkeerzones;
DROP TABLE IF EXISTS parkeerzones_old;
ALTER INDEX parkeerzones_new_pk RENAME TO parkeerzones_pk;
ALTER INDEX parkeerzones_new_wkb_geometry_geom_idx RENAME TO parkeerzones_wkb_geometry_geom_idx;

ALTER TABLE IF EXISTS parkeerzones_uitz RENAME TO parkeerzones_uitz_old;
ALTER TABLE parkeerzones_uitz_new RENAME TO parkeerzones_uitz;
DROP TABLE IF EXISTS parkeerzones_uitz_old;
ALTER INDEX parkeerzones_uitz_new_pk RENAME TO parkeerzones_uitz_pk;
ALTER INDEX parkeerzones_uitz_new_wkb_geometry_geom_idx RENAME TO parkeerzones_uitz_wkb_geometry_geom_idx;

COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh

