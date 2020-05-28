#!/usr/bin/env bash

# Convert Tram Metro Data to sql files and import in trm_tram_new and tram_metro_new tables
# If all is OK the tables are renamed to trm_tram and trm_metro, and the indexes are
# also renamed to make future imports possible

# export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# export PYTHONPATH="$SCRIPT_DIR/../..:$SCRIPT_DIR/../../.."
# export SHARED_DIR=${SCRIPT_DIR}/../../shared

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data"
unzip $DATA_DIR/Tram\ KGEs.zip -d ${TMPDIR}
unzip $DATA_DIR/Metro\ KGEs.zip -d ${TMPDIR}

# Convert Shape files to PostGIS SQL dump file
ogr2ogr -f "PGDump" -nlt GEOMETRY -t_srs EPSG:28992 -s_srs EPSG:28992 -nln trm_tram_new ${TMPDIR}/trm_tram.sql ${TMPDIR}/KGE_hartlijnen_Amsterdam_2.054.shp
ogr2ogr -f "PGDump" -nlt GEOMETRY -t_srs EPSG:28992 -s_srs EPSG:28992 -nln trm_metro_new ${TMPDIR}/trm_metro.sql ${TMPDIR}/Metro\ KGEs.shp

# Convert SQL file to utf-8 SQL file
iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/trm_tram.sql > ${TMPDIR}/trm_tram.utf8.sql
iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/trm_metro.sql > ${TMPDIR}/trm_metro.utf8.sql

# Remove drop table statements. They give error with ON_ERROR_STOP
perl -i -ne "print unless /DROP TABLE/" ${TMPDIR}/trm_tram.utf8.sql
perl -i -ne "print unless /DROP TABLE/" ${TMPDIR}/trm_metro.utf8.sql

# Replace HTML entities
sed -i -- 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g;' ${TMPDIR}/trm_tram.utf8.sql

echo "Create tables & import data "
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
DROP TABLE IF EXISTS trm_tram_new, trm_metro_new;
COMMIT;
\i ${TMPDIR}/trm_metro.utf8.sql
\i ${TMPDIR}/trm_tram.utf8.sql
SQL

${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS trm_metro RENAME TO trm_metro_old;
ALTER TABLE trm_metro_new RENAME TO trm_metro;
DROP TABLE IF EXISTS trm_metro_old;
ALTER INDEX trm_metro_new_pk RENAME TO trm_metro_pk;
ALTER INDEX trm_metro_new_wkb_geometry_geom_idx RENAME TO trm_metro_wkb_geometry_geom_idx;
ALTER TABLE IF EXISTS trm_tram RENAME TO trm_tram_old;
ALTER TABLE trm_tram_new RENAME TO trm_tram;
DROP TABLE IF EXISTS trm_tram_old;
ALTER INDEX trm_tram_new_pk RENAME TO trm_tram_pk;
ALTER INDEX trm_tram_new_wkb_geometry_geom_idx RENAME TO trm_tram_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
