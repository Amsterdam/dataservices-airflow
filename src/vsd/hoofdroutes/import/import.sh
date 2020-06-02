#!/usr/bin/env bash


source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Import data from OpenStreetMap with overpass"
# Import hoofdroutes from OpenStreetMap with overpass
python ${SCRIPT_DIR}/import_hoofdroutes.py > ${TMPDIR}/hoofdroutes.json
ogr2ogr -f "PGDump" -nlt MULTILINESTRING -t_srs EPSG:28992 -s_srs EPSG:4326 -nln hoofdroutes_new ${TMPDIR}/hoofdroutes.sql ${TMPDIR}/hoofdroutes.json

echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/hoofdroutes.sql
SQL

echo "Check imported data"
${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS hoofdroutes RENAME TO hoofdroutes_old;
ALTER TABLE hoofdroutes_new RENAME TO hoofdroutes;
DROP TABLE IF EXISTS hoofdroutes_old;
ALTER INDEX hoofdroutes_new_pk RENAME TO hoofdroutes_pk;
ALTER INDEX hoofdroutes_new_wkb_geometry_geom_idx RENAME TO hoofdroutes_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
