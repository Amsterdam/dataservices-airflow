#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

# curl -X GET https://maps.amsterdam.nl/open_geodata/geojson.php?KAARTLAAG=VIS_BFA\&THEMA=vis > ${TMPDIR}/vezips.json

wget https://maps.amsterdam.nl/open_geodata/geojson.php?KAARTLAAG=VIS_BFA\&THEMA=vis -O ${TMPDIR}/vezips.json
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -s_srs EPSG:4326  -nln vezips_new ${TMPDIR}/vezips.sql ${TMPDIR}/vezips.json

echo "Create tables & import data for vezips"
# Rename bfa to vezip to keep original fieldnames
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
DROP TABLE IF EXISTS vezips_new;
COMMIT;
\i ${TMPDIR}/vezips.sql
BEGIN;
ALTER TABLE vezips_new RENAME COLUMN bfa_nummer TO vezip_nummer;
ALTER TABLE vezips_new RENAME COLUMN bfa_type TO vezip_type;
COMMIT;
SQL

${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS vezips RENAME TO vezips_old;
ALTER TABLE vezips_new RENAME TO vezips;
DROP TABLE IF EXISTS vezips_old CASCADE;
ALTER INDEX vezips_new_pk RENAME TO vezips_pk;
ALTER INDEX vezips_new_wkb_geometry_geom_idx RENAME TO vezips_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
